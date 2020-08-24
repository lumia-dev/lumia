#!/usr/bin/env python

from datetime import datetime
from argparse import ArgumentParser
from lumia.Tools.rctools import rc
import lumia
from lumia.obsdb.footprintdb import obsdb
from lumia.formatters import lagrange
from lumia.Tools.logging_tools import logger

def Forward(rcf, verbosity='INFO'):

    # Set verbosity level
    logger.setLevel(verbosity)

    # Read config
    rcf = rc(rcf)
    start = datetime(*rcf.get('time.start'))
    end = datetime(*rcf.get('time.end'))

    # Read observations
    obsfile = rcf.get('observations.filename')
    db = obsdb(filename=obsfile)
    db.setupFootprints(path=rcf.get('footprints.path'), cache=rcf.get('footprints.cache'))

    # Read fluxes
    categories = dict.fromkeys(rcf.get('emissions.categories'))
    for cat in categories :
        categories[cat] = rcf.get(f'emissions.{cat}.origin')
    emis = lagrange.ReadArchive(rcf.get('emissions.prefix'), start, end, categories=categories)

    # transport model run
    model = lumia.transport(rcf, obs=db, formatter=lagrange)
    model.runForward(struct=emis, step=rcf.get('tag'))

    return model

if __name__ == '__main__' :

    # Read arguments
    p = ArgumentParser()
    p.add_argument('rc', help="Main configuration file (i.e. rc-file)")
    p.add_argument('--verbosity', help="verbosity of the run (i.e. level of logging). Choose between DEBUG, INFO (default) and WARNING", default='INFO')
    args = p.parse_args()

    fwd = Forward(args.rc, args.verbosity)
