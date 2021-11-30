#!/usr/bin/env python3

import sys
from argparse import ArgumentParser

p = ArgumentParser()
p.add_argument('action', choices=['fwd', 'inv'])
args = p.parse_args(sys.argv[1:])

if args.action == 'fwd':
    pass

elif args.action == 'inv':
    pass

else :
    # This shouldn't happen, but you never know ...
    raise RuntimeError(f"Action {args.action} not understood ...")