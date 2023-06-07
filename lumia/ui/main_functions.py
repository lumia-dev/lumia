#!/usr/bin/env python3

import os
from argparse import ArgumentParser, Namespace
from loguru import logger
from typing import List
from pathlib import Path
from pandas import Timestamp
from types import SimpleNamespace
from omegaconf import DictConfig, OmegaConf
import warnings
from lumia import Observations
from lumia.models.footprints.io import xr
from lumia.models.footprints.transport import Transport


# Ensure that warnings are captured by the logger (https://loguru.readthedocs.io/en/stable/resources/recipes.html#capturing-standard-stdout-stderr-and-warnings)
showwarning_ = warnings.showwarning
def showwarning(message, *args, **kwargs):
    logger.warning(message)
    showwarning_(message, *args, **kwargs)
warnings.showwarning = showwarning


def parse_args(args: List) -> Namespace:
    p = ArgumentParser()
    p.add_argument('--rcf', type=Path, help='Path to the configuration file (rc-file)')
    p.add_argument('--verbosity', '-v', default='INFO')
    p.add_argument('action', choices=['forward', 'optim', 'adjtest', 'gradtest', 'emis', 'adjtestmod', 'validate'], help='What to do')
    p.add_argument('--fakeobs', action='store_true', help='Run the model at the time of existing footprints instead of on the basis of an observation file')
    p.add_argument('--start', help='Start of the simulation (overwrites the "time.start" rc-key')
    p.add_argument('--end', help='End of the simulation (overwrites the "time.end" rc-key')
    p.add_argument('--tag', default='', help='suffix appended to the output path (default is based on the start and end time of the simulation')
    p.add_argument('--setkey', action='append', help='Overwrites the value of one rc-key. This can be used multiple times. For instance, "lumia forward --rcf config.rcf --setkey obs.file:file.tar.gz --setkey --setkey emissions.co2.interval:1H" will set the "obs.file" key to "file.tar.gz", and "emissions.*.interval" to "1H" (regardless of whatever settings were given before)')
    p.add_argument('--host')

    return p.parse_args(args)


def forward(dconf : DictConfig):
    """
    Do a forward model run
    """
    obs = load_observations(dconf)
    emis = load_emissions(dconf)
    model = Transport(dconf)
    model.setup_observations(obs)
    model.run_forward(emis, obs)
    return SimpleNamespace(obs = obs, emis = emis, model = model)


def optimize(dconf: DictConfig):
    opt = init_optim(dconf)
    opt.solve()
    return opt


def adjtest(dconf: DictConfig):
    from lumia.optimizer.optimizer import adjoint_test
    opt = init_optim(dconf)
    return adjoint_test(opt)


def gradtest(dconf: DictConfig):
    from lumia.optimizer.optimizer import gradient_test
    opt = init_optim(dconf)
    return gradient_test(opt)


def adjtestmod(dconf: DictConfig):
    from lumia.models.footprints.transport import Transport, adjoint_test
    obs = load_observations(dconf)
    emis = load_emissions(dconf)
    model = Transport(dconf)
    model.setup_observations(obs)
    return adjoint_test(model, emis)
    
    
def validate(dconf: DictConfig):
    obs = Observations.from_tar(dconf.observations.validation)
    emis = load_emissions(dconf)
    transport = Transport(dconf)
    transport.setup_observations(obs)
    transport.run_forward(emis, obs, step='validation')
    return transport


def init_optim(dconf: DictConfig):
    """
    Do an inversion
    """
    from lumia.models.footprints.mapping.multitracer import Mapping
    from lumia.prior.prior import PriorConstraints
    from lumia.optimizer.optimizer import Var4D

    obs = load_observations(dconf)
    emis = load_emissions(dconf)
    transport = Transport(dconf)
    transport.setup_observations(obs)
    sensi_map = transport.calc_sensi_map(emis)
    mapping = Mapping.init(dconf, emis, sensi_map=sensi_map)
    prior = PriorConstraints.setup(mapping)
    return Var4D(
        prior=prior, 
        model=transport, 
        mapping=mapping, 
        forcings=obs, 
        minimizer_settings=dconf.congrad, 
        settings=dconf.run
    )


def load_observations(dconf: DictConfig) -> Observations:
    obs = Observations.from_tar(dconf.observations.file)
    obs.select_times(dconf.run.start, dconf.run.end)
    return obs


def load_emissions(dconf: DictConfig) -> xr.Data:
    return xr.Data.from_dconf(dconf, dconf.run.start, dconf.run.end)


def parse_config(args: Namespace) -> DictConfig:
    dconf = OmegaConf.load(args.rcf)
    
    # # Deprecated keys:
    # # The dictionary below contains the list of keys that should no longer be used, and what their replacement key is.
    # # - If a deprecated key is found in the settings, then a warning is raised, and the correct key is set to the value of the deprecated one (respecting resolvers).
    # # - the deprecated key is set to point to the new one, with a deprecation warning (${oc.deprecated} resolver), so accessing the old key in the code will raise a deprecation warning
    # # - If both the deprecated key and its replacement are found, an exception is raised.
    # deprecated_keys = {
    #     'time.start': 'run.start',
    #     'time.end': 'run.end',
    #     'path.footprints': 'model.footprints',
    #     'model.transport.exec': 'model.exec',
    #     'model.transport.serial': 'model.options.serial'
    # }
    # for k, v in deprecated_keys.items():
    #     if k in rcf :
    #         logger.warning(f'The "{k}" key is deprecated. Use "{v}" instead')
    #         if v in rcf :
    #             msg = f'Only one of the keys "{k}" and "{v}" should be defined. Please fix your settings ...'
    #             logger.exception(RuntimeError(msg))
    #         rcf.set(v, rcf.get(k.rsplit('.', -1)[0])._content[k.split('.')[-1]])

    #     # Regardless of
    #     rcf.set(k, f'${{oc.deprecated:{v}}}')
    
    
    # handle the --host key
    dconf.machine = dconf[args.host]
    
    # handle the --setkey option(s) 
    if args.setkey:
        for kv in args.setkeys:
            dconf.merge_with_dotlist(kv.replace(':', '=', 1))

    # handle the --start and --end options
    if args.start is None:
        start = Timestamp(dconf.run.start)
    else:
        start = Timestamp(args.start)
        dconf.run.start = start.strftime('%Y-%m-%d')

    if args.end is None:
        end = Timestamp(dconf.run.end)
    else:
        end = Timestamp(args.end)
        dconf.run.end = end.strftime('%Y-%m-%d')

    # handle the --fakeobs option
    if args.fakeobs:
        raise NotImplementedError
        # rcf.setkey('observations.make_from_footprints', True)

    # handle the --tag option
    tag = args.tag
    if not tag :
        tag = f'{start:%Y%m%d}-{end:%Y%m%d}'
    dconf.run.paths.temp = os.path.join(dconf.run.paths.temp, tag)
    dconf.run.paths.output = os.path.join(dconf.run.paths.output, tag)
    
    return dconf
