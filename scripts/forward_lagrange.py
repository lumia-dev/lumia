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

if 'tag' not in rcf.keys:
    rcf.setkey('tag', f'{start.strftime("%Y%m%d%H%M")}-{end.strftime("%Y%m%d%H%M")}')

if rcf.get('transport.output'):
    #rcf.keys.keys['transport.output.steps'] = '${tag}'
    rcf.setkey('transport.output.steps', rcf.get('config'))

db = obsdb(filename=rcf.get('observations.input_file'), start=start, end=end)
db.setupFootprints(path=rcf.get('footprints.path'), cache=rcf.get('footprints.cache'))

categories = dict.fromkeys(rcf.get('emissions.categories'))
for cat in categories :
    categories[cat] = rcf.get(f'emissions.{cat}.origin')
emis = lagrange.ReadArchive(rcf.get('emissions.prefix'), start, end, categories=categories)

model = lumia.transport(rcf, obs=db, formatter=lagrange)
model.runForward(emis, step=rcf.get('config'))
#model.save()
