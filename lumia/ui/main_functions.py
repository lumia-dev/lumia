#!/usr/bin/env python3

import sys
import os
from argparse import ArgumentParser, Namespace
from loguru import logger
import subprocess
from typing import List
from pathlib import Path
from lumia.config import RcFile
from pandas import Timestamp
import lumia
from lumia.obsdb.InversionDb import obsdb
from lumia.formatters import xr
from lumia.interfaces.multitracer import Interface
from types import SimpleNamespace
from configparser import ConfigParser
import warnings


# Ensure that warnings are captured by the logger (https://loguru.readthedocs.io/en/stable/resources/recipes.html#capturing-standard-stdout-stderr-and-warnings)
showwarning_ = warnings.showwarning
def showwarning(message, *args, **kwargs):
    logger.warning(message)
    showwarning_(message, *args, **kwargs)
warnings.showwarning = showwarning


def apptainer_wrapper(args: List):
    """
    Determine if the main script should be run in an apptainer container. If so, resubmit the command within the requested container, otherwise just continue.

    """
    # Load defaults:
    parser = ArgumentParser(add_help=False)
    parser.add_argument('--app', action='store_true', default=False)
    parser.add_argument('--footprints', default=None)
    parser.add_argument('--output', '-o', default=None)
    parser.add_argument('--scratch', default=None)
    parser.add_argument('--bind', '-b', action='append')
    parser.add_argument('--container', default=None)
    parser.add_argument('--dev', action='store_true', default=False, help='Use a dev container and mount the lumia source code under /lumia')
    parser.add_argument('--app-config', dest='settings', default=Path('lumia.ini'), help='Location of the file containing the container default settings. If no value is provided, the code will look for "lumia.ini" in the current directory, then for "lumia.ini" in the ~/.config folder.', type=Path)

    args, remainder = parser.parse_known_args(args)

    if args.app:
        # Read default settings from various config files. Each file overwrites the previous one
        # (so one can be viewed as a machine settings, the next one a project settings, etc.)
        # Not all files are expected to be present, so no error is raised if one or more files are missing.

        conf = ConfigParser()
        conf.read(Path.home() / '.config/lumia.ini')  # Read from ~/.config
        conf.read(lumia.prefix / 'etc/lumia.ini')  # Read from the path where the lumia python library is installed
        conf.read(args.settings)  # Read from the path provided as argument (default: current path)

        # handle the "--dev" flag:
        if args.dev:
            conf['container']['active'] = conf['container']['dev']
            conf['bind']['/lumia'] = str(lumia.prefix)
        else:
            conf['container']['active'] = conf['container']['default']

        # Next, overwrite with script arguments
        if args.footprints is not None:
            conf['bind']['/footprints'] = args.footprints
        if args.output is not None:
            conf['bind']['/output'] = args.output
        if args.bind is not None:
            conf['bind']['/scratch'] = args.scratch
        if args.container is not None:
            conf['container']['active'] = args.container

        if args.bind is not None:
            for b in args.bind:
                dest, src = b.split(':')
                conf['bind'][dest] = src

        # Construct the command line:
        cmd = 'apptainer run --cleanenv'
        for dest, src in conf['bind'].items():
            cmd += f' --bind {src}:{dest}'
        cmd += ' ' + conf['container']['active']

        # Ensure that the three mandatory paths exist:
        Path(conf['bind']['/output']).mkdir(exist_ok=True)
        Path(conf['bind']['/scratch']).mkdir(exist_ok=True)
        Path(conf['bind']['/data']).mkdir(exist_ok=True)

        if '--help' in remainder or '-h' in remainder:
            parser.print_help()
            sys.exit()

        cmd += ' ' + ' '.join([_ for _ in remainder])
        logger.info(cmd)
        subprocess.run(cmd.split())
        sys.exit()

    else:
        return remainder


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

    return p.parse_args(args)


def parse_config(args: Namespace) -> RcFile:
    rcf = RcFile(args.rcf)
    
    # Deprecated keys:
    # The dictionary below contains the list of keys that should no longer be used, and what their replacement key is.
    # - If a deprecated key is found in the settings, then a warning is raised, and the correct key is set to the value of the deprecated one (respecting resolvers).
    # - the deprecated key is set to point to the new one, with a deprecation warning (${oc.deprecated} resolver), so accessing the old key in the code will raise a deprecation warning
    # - If both the deprecated key and its replacement are found, an exception is raised.
    deprecated_keys = {
        'time.start': 'run.start',
        'time.end': 'run.end',
        'path.footprints': 'model.footprints',
        'model.transport.exec': 'model.exec',
        'model.transport.serial': 'model.options.serial'
    }
    for k, v in deprecated_keys.items():
        if k in rcf :
            logger.warning(f'The "{k}" key is deprecated. Use "{v}" instead')
            if v in rcf :
                msg = f'Only one of the keys "{k}" and "{v}" should be defined. Please fix your settings ...'
                logger.exception(RuntimeError(msg))
            rcf.set(v, rcf.rcfGet(k.rsplit('.', -1)[0])._content[k.split('.')[-1]])

        # Regardless of
        rcf.set(k, f'${{oc.deprecated:{v}}}')
    
    # handle the --setkey option(s) 
    if args.setkey:
        for kv in args.setkey:
            k, v = kv.split(':')
            rcf.setkey(k, v)

    # handle the --start and --end options
    if args.start is None:
        start = Timestamp(rcf.rcfGet('run.start'))
    else:
        start = Timestamp(args.start)
        rcf.setkey('run.start', start.strftime('%Y-%m-%d'))

    if args.end is None:
        end = Timestamp(rcf.rcfGet('run.end'))
    else:
        end = Timestamp(args.end)
        rcf.setkey('run.end', end.strftime('%Y-%m-%d'))

    # handle the --fakeobs option
    if args.fakeobs:
        rcf.setkey('observations.make_from_footprints', True)

    # handle the --tag option
    tag = args.tag
    if not tag :
        tag = f'{start:%Y%m%d}-{end:%Y%m%d}'
    rcf['run']['paths']['temp'] = os.path.join(rcf.rcfGet('run.paths.output'), tag)
    rcf['run']['paths']['output'] = os.path.join(rcf.rcfGet('run.paths.output'), tag)
    
    return rcf


def load_observations(rcf: RcFile) -> obsdb:
    """
    Load the observation database, either based on rc-keys (default behaviour), or just run for all existing footprints in the "path.footprints" folder.
    Relevant rc-keys:
        obs.fields.rename   : pair of old_col_name:new_col_name to rename one column of the obs database
        obs.file            : name of the observation file
        path.footprints     : location of the footprints (only needed if fakeobs set to True)
        time.start
        time.end
        tracers
        obs.uncertainty.setup : TODO: that one and the next shouldn't be here ...
        obs.uncertainty
    """
    if rcf.rcfGet('observations.make_from_footprints', default=False):
        from lumia.obsdb.runflex import obsdb
        db = obsdb(rcf['observations.footprints'], rcf['run.start'], rcf['run.end'], tracers=list(rcf['run.tracers']))
    else:
        from lumia.obsdb.InversionDb import obsdb
        db = obsdb.from_rc(rcf)
    return db


def setup_uncertainties(model: lumia.transport, emis: xr.Data = None) -> lumia.transport:
    """
    Compute observation uncertainties, depending on the computation method given by the "obs.uncertainty" key.
    If "obs.uncertainty" is set to "dyn", then the uncertainty is based on the prior fit of the model to the data,
    therefore this (can) involve a forward model run.
    """
    for conf in model.rcf.rcfGet('optimize.observations').values():
        res = None
        if conf['uncertainty']['type'] == 'dyn':
            logger.info(f"mainfunctions_setup_uncertainties(): setting up uncertainties for conf={conf} and type=dyn")
            if res is None :
                # Avoid recomputing multiple times if there are several tracers
                res = model.calcDepartures(emis, 'apri')
            model.db.setup_uncertainties_dynamic(
                'mix_apri',
                conf['uncertainty'].get('freq', '7D'),
                conf['uncertainty'].get('obs_err_field', 'err_obs')
            )
        else:
            model.db.setup_uncertainties()

    return model


def init_model(rcf: RcFile) -> (obsdb, xr.Data, lumia.transport):
    """
    Load the observations, emissions and instantiate the transport model
    """
    obs = load_observations(rcf)
    emis = prepare_emis(rcf)
    model = lumia.transport(rcf, obs=obs, formatter=xr)
    return obs, emis, model


def init_optim(model: lumia.transport, emis: xr.Data) -> lumia.optimizer.Optimizer:
    """
    Instantiate and initialize the optimizer (this requires an adjoint run)
    """
    sensi = model.calcSensitivityMap(emis)
    control = Interface(model.rcf, model_data=emis, sensi_map=sensi)
    return lumia.optimizer.Optimizer(model.rcf, model, control)


def forward(rcf: RcFile) -> SimpleNamespace:
    """
    do a forward model run
    """
    obs, emis, model = init_model(rcf)
    model.calcDepartures(emis, step = rcf['run'].get('tag', 'forward'))
    return SimpleNamespace(obs=obs, emis=emis, model=model)


def optimize(rcf: RcFile) -> SimpleNamespace:
    """
    do an optimization
    """
    obs, emis, model = init_model(rcf)
    model = setup_uncertainties(model, emis)
    opt = init_optim(model, emis)
    logger.info(f"Entering main_functions_optimize() and calling opt.Var4D() opt={opt}")
    opt.Var4D()

    if rcf.rcfGet('observations.validation', default=False):
        obs_valid = obsdb.from_rc(rcf, filekey='validation', setup_uncertainties=False)
        model.run_forward(opt.interface.model_data, obs_valid, step='validation')
    return SimpleNamespace(obs=obs, model=model, emis=emis, optim=opt)


def validate(rcf: RcFile) -> SimpleNamespace:
    obs = obsdb.from_rc(rcf, filekey='validation', setup_uncertainties=False)
    model = lumia.transport(rcf, obs=obs, formatter=xr)
    emis = xr.Data.from_file(Path(rcf.rcfGet('path.output')) / 'emissions_apos.nc')
    model.run_forward(emis, obs, step='validation')
    return SimpleNamespace(obs=obs, model=model, emis=emis)


def gradtest(rcf: RcFile) -> SimpleNamespace:
    """
    do a gradient test
    """
    obs, emis, model = init_model(rcf)
    obs.setup_uncertainties(model, emis)
    opt = init_optim(model, emis)
    opt.GradientTest()
    return SimpleNamespace(obs=obs, emis=emis, model=model, optim=opt)


def adjtest(rcf: RcFile) -> SimpleNamespace:
    """
    do an adjoint test
    """
    obs, emis, model = init_model(rcf)
    opt = init_optim(model, emis)
    opt.AdjointTest()
    return SimpleNamespace(obs=obs, emis=emis, model=model, optim=opt)


def adjtestmod(rcf: RcFile):
    """
    do an adjoint test on the transport model (i.e. do not test the adjoint of the "Interface").
    """
    obs, emis, model = init_model(rcf)
    model.adjoint_test(emis)
    return SimpleNamespace(obs=obs, emis=emis, model=model)


def prepare_emis(rcf: RcFile) -> xr.Data:
    """
    Construct an emission file for the simulation based on pre-processed, annual, category-specific emission files.
    """
    emis = xr.Data.from_rc(rcf, rcf.rcfGet('run.start'), rcf.rcfGet('run.end'))
    emis.print_summary()
    return emis
