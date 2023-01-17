#!/usr/bin/env python

# This module gathers upper level functions typically called by the bin/lumia script, enabling their use in
# other scripts. The functions are grouped in three objects, defined at the bottom of the module:
# - prepare : contains the functions to prepare input data (config file, emissions/fluxes and observations)
# - run: contains the function to perform standard runs (forward, optim and validation)
# - test: contains additional run configurations (gradient and adjoint test, adjoint test of the transport model).


import os
from argparse import Namespace
from rctools import RcFile
from pandas import Timestamp
from loguru import logger
from lumia.obsdb import obsdb
from lumia.formatters import xr
from lumia.interfaces.multitracer import Interface
from types import SimpleNamespace
from pathlib import Path
import lumia


def parse_config(args: Namespace) -> RcFile:
    """
    Read in LUMIA settings. Settings are adjusted in three steps:
    1. default settings are semi hard-coded (some paths are appended with the "tag" value, if it's provided).
    2. settings are read from the rc-file (which overwrites default settings if needed).
    3. settings from the called script arguments can overwrite default and rc- settings:
        - the "--start" and "--end" arguments overwrite the "time.start" and "time.end" rc-keys
        - the "--tag" argument overwrites the "tag" rc-key
        - any argument passed with --setkey key:value will set the value of the rc-key "key" to "value"

    The function returns a RcFile object, which contains the set of key:value pairs resulting from this 3-step process.
    """

    # Default paths, common to all runs within this container, normally
    defaults = {
        # Global paths
        'path.data': '/input',
        'path.temp': os.path.join('/scratch', args.tag),
        'path.footprints': '/footprints',
        'correlation.inputdir': '${path.data}/corr',

        # Run-dependent paths
        'tag': args.tag,
        'path.output': os.path.join('/output', args.tag),
        'var4d.communication.file': '${path.temp}/congrad.nc',
        'emissions.*.archive': 'rclone:lumia:fluxes/nc/',
        'emissions.*.path': '${path.data}/fluxes/nc',
        'model.transport.exec': lumia.prefix / 'transport/multitracer.py',
        'transport.output': 'T',
        'transport.output.steps': ['forward'],
    }

    rcf = RcFile(args.rcf)

    if args.setkey :
        for kv in args.setkey :
            k, v = kv.split(':')
            rcf.setkey(k, v)

    for tr in rcf.get('tracers', tolist='force'):
        defaults[f'emissions.{tr}.archive'] = f'rclone:lumia:fluxes/nc/${{emissions.{tr}.region}}/${{emissions.{tr}.interval}}/'

    # Read simulation time
    if args.start is None :
        start = Timestamp(rcf.get('time.start'))
    else :
        start = Timestamp(args.start)
        rcf.setkey('time.start', start.strftime('%Y-%m-%d'))
    if args.end is None :
        end = Timestamp(rcf.get('time.end'))
    else :
        end = Timestamp(args.end)
        rcf.setkey('time.end', end.strftime('%Y-%m-%d'))

    if args.fakeobs :
        rcf.setkey('observations.make_from_footprints', True)

    # Create subfolder based on the inversion time:
    defaults['path.output'] = os.path.join(defaults['path.output'], f'{start:%Y%m%d}-{end:%Y%m%d}')
    defaults['path.temp'] = os.path.join(defaults['path.temp'], f'{start:%Y%m%d}-{end:%Y%m%d}')

    rcf.set_defaults(**defaults)

    logger.info(f"Temporary files will be stored in {rcf.get('path.temp')}")

    # Adjust the paths in memory:

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
    if rcf.get('observations.make_from_footprints', default=False) :
        from lumia.obsdb.runflex import obsdb
        db = obsdb(rcf.get('path.footprints'), rcf.get('time.start'), rcf.get('time.end'), tracers=rcf.get('tracers', tolist='force'))
    else :
        from lumia.obsdb.InversionDb import obsdb
        db = obsdb.from_rc(rcf)
    return db


def setup_uncertainties(model: lumia.transport, emis: xr.Data = None) -> lumia.transport:
    """
    Compute observation uncertainties, depending on the computation method given by the "obs.uncertainty" key.
    If "obs.uncertainty" is set to "dyn", then the uncertainty is based on the prior fit of the model to the data,
    therefore this (can) involve a forward model run.
    """
    if model.rcf.get('obs.uncertainty') == 'dyn':
        model.calcDepartures(emis, 'apri')
        model.db.setup_uncertainties_dynamic(
            'mix_apri',
            model.rcf.get('obs.uncertainty.dyn.freq', default='7D'),
            model.rcf.get('obs.uncertainty.obs_field', default='err_obs')
        )
    else :
        model.db.setup_uncertainties()

    return model


def prepare_emis(rcf: RcFile) -> xr.Data:
    """
    Construct an emission file for the simulation based on pre-processed, annual, category-specific emission files.
    """
    emis = xr.Data.from_rc(rcf, rcf.get('time.start'), rcf.get('time.end'))
    emis.print_summary()
    return emis


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
    model.calcDepartures(emis, 'forward')
    return SimpleNamespace(obs=obs, emis=emis, model=model)


def optimize(rcf: RcFile) -> SimpleNamespace:
    """
    do an optimization
    """
    obs, emis, model = init_model(rcf)
    model = setup_uncertainties(model, emis)
    opt = init_optim(model, emis)
    opt.Var4D()
    if rcf.get('obs.validation_file', default=False):
        obs_valid = obsdb.from_rc(rcf, filekey='obs.validation_file', setupUncertainties=False)
        model.run_forward(opt.interface.model_data, obs_valid, step='validation')
    return SimpleNamespace(obs=obs, model=model, emis=emis, optim=opt)


def validate(rcf: RcFile) -> SimpleNamespace:
    obs = obsdb.from_rc(rcf, filekey='obs.validation_file', setupUncertainties=False)
    model = lumia.transport(rcf, obs=obs, formatter=xr)
    emis = xr.Data.from_file(Path(rcf.get('path.output')) / 'emissions_apos.nc')
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
