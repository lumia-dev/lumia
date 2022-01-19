#!/usr/bin/env python

import sys
import configparser
from argparse import ArgumentParser
import os

p = ArgumentParser()
p.add_argument('--container')
p.add_argument('--dev', default=None)
p.add_argument('--exec', default='~/.local/bin')
p.add_argument('--extras', action='append')
p.add_argument('--output', default=None)
p.add_argument('--data', default=None)
p.add_argument('--footprints', default=None)
p.add_argument('--scratch', default=None)
args = p.parse_args(sys.argv[1:])

c = configparser.ConfigParser()
c['lumia'] = {
    'DefaultContainer': os.path.abspath(args.container),
    'DevFolder': os.path.abspath(args.dev)
}

if args.footprints is not None :
    c['footprints'] = os.path.abspath(args.footprints)

if args.output is not None :
    c['output'] = os.path.abspath(args.output)

if args.scratch is not None :
    c['scratch'] = os.path.abspath(args.scratch)

if args.data is not None :
    c['data'] = os.path.abspath(args.data)

if args.extras is not None:
    c['extras'] = {}
    for extra in args.extras :
        external, internal = extra.split(':')
        c['extras'][external] = internal

with open(os.path.join(os.environ['HOME'], '.config/lumia.ini'), 'w') as configfile:
    c.write(configfile)