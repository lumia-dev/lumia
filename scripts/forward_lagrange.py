#!/usr/bin/env python

import sys
from argparse import ArgumentParser, REMAINDER
from datetime import datetime
from lumia.obsdb.footprintdb import obsdb
from lumia.formatters import lagrange

def readArgs(args):
    p = ArgumentParser()
    p.add_argument('--rc', required=True)
    p.add_argument('--debug', action='store_true', default=False)
    p.add_argument('args', nargs=REMAINDER)
    args = p.parse_args(args)
    return args

args = readArgs(sys.argv[1:])

import lumia

from lumia.Tools.logging_tools import logger
logger.setLevel('INFO')
if args.debug :
    logger.setLevel('DEBUG')

rcf = lumia.rc(args.rc)

start = datetime(*rcf.get('time.start'))
end = datetime(*rcf.get('time.end'))

db = obsdb(filename=rcf.get('observations.input_file'), start=start, end=end)
db.setupFootprints(path=rcf.get('footprints.path'), cache=rcf.get('footprints.cache'))

emis = lagrange.ReadStruct(rcf.get('emissions.input_file'))

model = lumia.transport(rcf, obs=db, formatter=lagrange)
model.runForward(emis, step='forward')
model.save()
