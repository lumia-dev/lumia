#!/usr/bin/env python
import os
import logging
from footprints import FootprintTransport, LumiaFootprintFile


if __name__ == '__main__':
    import sys
    from argparse import ArgumentParser, REMAINDER

    logger = logging.getLogger(os.path.basename(__file__))

    p = ArgumentParser()
    p.add_argument('--forward', '-f', action='store_true', default=False, help="Do a forward run")
    p.add_argument('--serial', '-s', action='store_true', default=False, help="Run on a single CPU")
    p.add_argument('--verbosity', '-v', default='INFO')
    p.add_argument('--rc')
    p.add_argument('--db', required=True)
    p.add_argument('--emis', required=True)
    p.add_argument('--checkfile', '-c')
    p.add_argument('--check-footprints', action='store_true', default=True, help="Locate the footprint files and check them", dest='checkFootprints')
    p.add_argument('args', nargs=REMAINDER)
    args = p.parse_args(sys.argv[1:])

    logger.setLevel(args.verbosity)

    # Create the transport model
    model = FootprintTransport(args.rc, args.db, args.emis, LumiaFootprintFile, mp=not args.serial, checkfile=args.checkfile)

    if args.checkFootprints:
        model.checkFootprints(model.rcf.get('path.footprints'))

    if args.forward :
        model.runForward()