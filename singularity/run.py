#!/usr/bin/env python3

import os
import sys
from datetime import datetime
from argparse import ArgumentParser
import lumia
from lumia.Tools.logging_tools import logger
#from lumia.obsdb import obsdb
from lumia.obsdb.InversionDb import obsdb
from lumia.formatters import lagrange

p = ArgumentParser()
p.add_argument('--forward', default=False, action='store_true')
p.add_argument('--optimize', default=False, action='store_true')
p.add_argument('--tag', default='')
p.add_argument('--rcf')
p.add_argument('--verbosity', '-v', default='INFO')
args = p.parse_args(sys.argv[1:])

logger.setLevel(args.verbosity)

rcf = lumia.rc(args.rcf)

# Default paths, common to all runs within this container, normally
defaults = {
    # Global paths
    'path.data': '/data',
    'path.temp': '/tmp',
    'path.footprints': '/footprints',
    'correlation.inputdir': '/data/corr',

    # Run-dependent paths
    'tag': args.tag,
    'path.output': os.path.join('/output', args.tag),
    'var4d.communication.file': '${path.output}/comm_file.nc4',
    'path.archive': 'rclone:results:${project}/${tag}',
    'emissions.archive': 'rclone:lumia:fluxes/nc/${region}/${emissions.interval}',
    'emissions.prefix': '/data/fluxes/nc/${region}/${emissions.interval}/flux_co2.',
    'model.transport.exec': '/lumia/transport/default.py',
}
for k, v in defaults.items():
    if k not in rcf.keys :
        rcf.setkey(k, v)

# Read simulation time
start = datetime(*rcf.get('time.start'))
end = datetime(*rcf.get('time.end'))

# Load observations
db = obsdb(rcf)
db.SetupUncertainties()

# Load the pre-processed emissions:
categories = dict.fromkeys(rcf.get('emissions.categories'))
for cat in categories :
    categories[cat] = rcf.get(f'emissions.{cat}.origin')

emis = lagrange.ReadArchive(rcf.get('emissions.prefix'), start, end, categories=categories, archive=rcf.get('emissions.archive'))

model = lumia.transport(rcf, obs=db, formatter=lagrange)

if args.forward :
    model.runForward(emis, 'forward')

elif args.optimize :
    from lumia.interfaces.footprint_flexRes import Interface

    sensi = model.calcSensitivityMap()
    control = Interface(rcf, ancilliary={'sensi_map':sensi}, emis=emis)
    opt = lumia.optimizer.Optimizer(rcf, model, control)
    opt.Var4D()
