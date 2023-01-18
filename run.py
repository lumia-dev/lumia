#!/usr/bin/env python3

import os
import sys
from pandas import Timestamp
from argparse import ArgumentParser
import lumia
from rctools import RcFile as rc
from loguru import logger
from lumia.obsdb.InversionDb import obsdb
from lumia.formatters import xr

p = ArgumentParser()
p.add_argument('--forward', default=False, action='store_true')
p.add_argument('--model-adjtest', default=False, action='store_true', dest='adjtestmod')
p.add_argument('--adjtest', default=False, action='store_true', dest='adjtest')
p.add_argument('--gradtest', default=False, action='store_true', dest='gradtest')
p.add_argument('--noobs', default=False, action='store_true', help="Run without an observations database, on the basis of the footprints available.")
p.add_argument('--optimize', default=False, action='store_true')
p.add_argument('--prepare_emis', default=False, action='store_true', dest='emis', help="Use this command to prepare an emission file without actually running the transport model or an inversion.")
p.add_argument('--start', default=None, help="Start of the simulation. Overwrites the value in the rc-file")
p.add_argument('--end', default=None, help="End of the simulation. Overwrites the value in the rc-file")
p.add_argument('--setkey', action='append', help="use to override some rc-keys")
p.add_argument('--tag', default='')
p.add_argument('--rcf')   # what used to be the resource file (now yaml file) - only yaml format is supported
p.add_argument('--ymf')   # yaml configuration file where the user plans his or her Lumia run: parameters, input files etc.
p.add_argument('--verbosity', '-v', default='INFO')
args = p.parse_args(sys.argv[1:])

# Set the verbosity in the logger (loguru quirks ...)
logger.remove()
logger.add(sys.stderr, level=args.verbosity)

if(args.rcf is None):
    if(args.ymf is None):
        print("Lumia: Fatal error: no user configuration (yaml) file provided.")
        sys.exit(1)
    else:
        rcf = rc(args.ymf)
else:            
    rcf = rc(args.rcf)

if args.setkey :
    for kv in args.setkey :
        k, v = kv.split(':')
        rcf.setkey(k, v)

# Default paths, common to all runs within this container, normally
defaults = {
    # Global paths
    'path.data': '/data',
    'run.paths.temp': os.path.join('/temp', args.tag),
    'run.paths.footprints': '/footprints',
    'correlation.inputdir': '/data/corr',

    # Run-dependent paths
    'tag': args.tag,
    'run.paths.output': os.path.join('/output', args.tag),
    'var4d.communication.file': '${run.paths.temp}/congrad.nc',
    'emissions.*.archive': 'rclone:lumia:fluxes/nc/',
    'emissions.*.path': '/data/fluxes/nc',
    'model.transport.exec': '/lumia/transport/multitracer.py',
    'transport.output': 'T',
    'transport.output.steps': ['forward'],
}

# for tr in rcf.get('run.tracers', tolist='force'):
for tr in list(rcf['run']['tracers']):      # or  list(rcf.get('run.tracers'))
    defaults[f'emissions.{tr}.archive'] = f'rclone:lumia:fluxes/nc/${{emissions.{tr}.region}}/${{emissions.{tr}.interval}}/'

# Read simulation time
if args.start is None :
    start = Timestamp(rcf.get('time.start'))
else :
    start = Timestamp(args.start)
    rcf.setkey('time.start', start.strftime('%Y,%m,%d'))
if args.end is None :
    end = Timestamp(rcf.get('time.end'))
else :
    end = Timestamp(args.end)
    rcf.setkey('time.end', end.strftime('%Y,%m,%d'))


# Create subfolder based on the inversion time:
defaults['run.paths.output'] = os.path.join(defaults['run.paths.output'], f'{start:%Y%m%d}-{end:%Y%m%d}')
defaults['run.paths.temp'] = os.path.join(defaults['run.paths.temp'], f'{start:%Y%m%d}-{end:%Y%m%d}')

rcf.set_defaults(**defaults)

logger.info(f"Temporary files will be stored in {rcf.get('run.paths.temp')}")
logger.info(f"Temporary files will be stored in {rcf['run']['paths']['temp']}")


# lumia.paths.setup(rcf)

# TODO: move " Load the pre-processed emissions" to after Load observations block again - just for testing so we can 
# work on reading co2 emissions via DA before being bugged down by reading observations in the debugger....
# Load the pre-processed emissions:
emis = xr.Data.from_rc(rcf, start, end)
emis.print_summary()

# Load observations
if args.noobs :
    from lumia.obsdb.runflex import obsdb
    db = obsdb(rcf.get('paths.footprints'), start, end)
elif args.forward or args.optimize or args.adjtest or args.gradtest or args.adjtestmod:
    db = obsdb.from_rc(rcf)
else :
    # if we just want to write the emissions ...
    db = None

# Create model instance
model = lumia.transport(rcf, obs=db, formatter=xr)

# Do a model run (or something else ...).
if args.forward :
    model.calcDepartures(emis, 'forward')


# Setup uncertainties if needed:
if args.optimize or args.gradtest :
    if rcf.get('obs.uncertainty') == 'dyn':
        model.calcDepartures(emis, 'apri')
        db.setup_uncertainties_dynamic(
            'mix_apri',
            rcf.get('obs.uncertainty.dyn.freq', default='7D'),
            rcf.get('obs.uncertainty.obs_field', default='err_obs')
        )
    else :
        db.setup_uncertainties()

if args.optimize or args.adjtest or args.gradtest :
    from lumia.interfaces.multitracer import Interface

    sensi = model.calcSensitivityMap(emis)
    control = Interface(rcf, model_data=emis, sensi_map=sensi)
    opt = lumia.optimizer.Optimizer(rcf, model, control)

    if args.optimize :
        opt.Var4D()
        if rcf.get('obs.validation_file', default=False):
            obs_valid = obsdb.from_rc(rcf, filekey='obs.validation_file', setupUncertainties=False)
            model.run_forward(control.model_data, obs_valid, step='validation')

    elif args.adjtest :
        opt.AdjointTest()
    elif args.gradtest :
        opt.GradientTest()

elif args.emis :
    model.writeStruct(emis, rcf['run']['paths']['output'], 'modelData')

elif args.adjtestmod :
    # Test only the adjoint of the CTM (skip the lumia stuff ...).
    model.adjoint_test(emis)
print("Done.")