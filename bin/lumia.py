#!/usr/bin/env python

import sys
from argparse import ArgumentParser
import lumia
from lumia.formatters import lagrange
from lumia.Tools.logging_tools import logger
from lumia.interfaces.footprint_flexRes import Interface
from lumia.obsdb.InversionDb import obsdb

p = ArgumentParser()
p.add_argument('action', type=str, choices=['var4d'])
p.add_argument('--rc', type=str, help='Configuration file ("rc-file")')
p.add_argument('--verbosity', '-v', default='INFO')
args = p.parse_args(sys.argv[1:])

logger.setLevel(args.verbosity)

rcf = lumia.rc(args.rc)

if args.action == 'var4d':
    obs = obsdb(rcf, setupUncertainties=rcf.get('obs.uncertainty.setup', default=True))
    emis = lagrange.Emissions(rcf, obs.start, obs.end)
    model = lumia.transport(rcf, obs=obs, formatter=lagrange)
    sensi = model.calcSensitivityMap()
    control = Interface(rcf, ancilliary={'sensi_map':sensi}, emis=emis.data)
    opt = lumia.optimizer.Optimizer(rcf, model, control)
    opt.Var4D()