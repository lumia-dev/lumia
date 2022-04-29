#!/usr/bin/env python3

import os
import sys
from datetime import datetime
from argparse import ArgumentParser
import lumia
from loguru import logger
from lumia.obsdb.InversionDb import obsdb
from lumia.formatters import lagrange

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
p.add_argument('--tag', default='')
p.add_argument('--rcf')
p.add_argument('--verbosity', '-v', default='INFO')
args = p.parse_args(sys.argv[1:])

#ogger.setLevel(args.verbosity)

rcf = lumia.rc(args.rcf)

# Default paths, common to all runs within this container, normally
defaults = {
    # Global paths
    'path.data': '/data',
    'path.temp': os.path.join('/temp', args.tag),
    'path.footprints': '/footprints',
    'correlation.inputdir': '/data/corr',

    # Run-dependent paths
    'tag': args.tag,
    'path.output': os.path.join('/output', args.tag),
    'var4d.communication.file': '${path.output}/comm_file.nc4',
    'emissions.archive': 'rclone:lumia:fluxes/nc/${region}/${emissions.convert_from.interval}',
    'emissions.prefix': '/data/fluxes/nc/${region}/${emissions.convert_from.interval}',
    'model.transport.exec': '/lumia/transport/default.py',
    'transport.output': 'T',
    'transport.output.steps': 'apri, apos, forward',
    'emissions.convert_from.interval': '${emissions.interval}',
}

# Read simulation time
if args.start is None :
    start = datetime(*rcf.get('time.start'))
else :
    start = datetime.strptime(args.start, '%Y%m%d')
    rcf.setkey('time.start', start.strftime('%Y,%m,%d'))
if args.end is None :
    end = datetime(*rcf.get('time.end'))
else :
    end = datetime.strptime(args.end, '%Y%m%d')
    rcf.setkey('time.end', end.strftime('%Y,%m,%d'))


# Create subfolder based on the inversion time:
defaults['path.output'] = os.path.join(defaults['path.output'], f'{start:%Y%m%d}-{end:%Y%m%d}')
defaults['path.temp'] = os.path.join(defaults['path.temp'], f'{start:%Y%m%d}-{end:%Y%m%d}')

for k, v in defaults.items():
    if k not in rcf.keys :
        rcf.setkey(k, v)

logger.info(f"Temporary files will be stored in {rcf.get('path.temp')}")

# Load observations
if args.noobs :
    from lumia.obsdb.runflex import obsdb
    db = obsdb(rcf.get('path.footprints'), start, end)
elif args.forward or args.optimize :
    db = obsdb(rcf)
    if args.optimize :
        db.SetupUncertainties()
else :
    db = None


# Load the pre-processed emissions:
emis = lagrange.Emissions(rcf, start, end)
emis.print_summary(unit=rcf.get(f'emissions.{rcf.get("tracer")}.unit'))

# Create model instance
model = lumia.transport(rcf, obs=db, formatter=lagrange)


# Do a model run (or something else ...).
if args.forward :
    model.calcDepartures(emis.data, 'forward')

elif args.optimize or args.adjtest or args.gradtest :
    from lumia.interfaces.footprint_flexRes import Interface

    sensi = model.calcSensitivityMap()
    control = Interface(rcf, ancilliary={'sensi_map': sensi}, emis=emis.data)
    opt = lumia.optimizer.Optimizer(rcf, model, control)
    if args.optimize :
        opt.Var4D()
    elif args.adjtest :
        opt.AdjointTest()
    elif args.gradtest :
        opt.GradientTest()

elif args.emis :
    model.writeStruct(emis.data, rcf.get('path.output'), 'modelData')

elif args.adjtestmod :
    # Test only the adjoint of the CTM (skip the lumia stuff ...).
    model.adjoint_test(emis.data)