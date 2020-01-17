#!/usr/bin/env python

import lumia
from argparse import ArgumentParser
from lumia.obsdb.footprintdb import obsdb
from datetime import datetime
from lumia.formatters import lagrange
from lumia.interfaces import Interface
from lumia.Tools.logging_tools import logger
from lumia.obsdb.backgroundDb import backgroundDb
import os

def optimize(rcfile, obs=None, emis=None, setuponly=False, verbosity='INFO'):

    # Set verbosity level
    logger.setLevel(verbosity)

    # Create basic objects
    rcf = lumia.rc(rcfile)
    obsfile = rcf.get('observations.input_file') if obs is None else args.obs

    # Read additional basic settings from rc-file:
    start = datetime(*rcf.get('time.start'))
    end = datetime(*rcf.get('time.end'))

    # Load the observations database
    db = obsdb(filename=obsfile, start=start, end=end)
    db.setupFootprints(path=rcf.get('footprints.path'), cache=rcf.get('footprints.cache'))
    db = backgroundDb(db)
    db.read_backgrounds(path=rcf.get('backgrounds.path'))

    #TODO: setup the uncertainties
    db.SetMinErr(rcf.get('obs.err_min'))

    # Load the pre-processed emissions:
    categories = dict.fromkeys(rcf.get('emissions.categories'))
    for cat in categories :
        categories[cat] = rcf.get(f'emissions.{cat}.origin')
    emis = lagrange.ReadArchive(rcf.get('emissions.prefix'), start, end, categories=categories)

    # Initialize the obs operator (transport model)
    model = lumia.transport(rcf, obs=db, formatter=lagrange)

    # Initialize the data container (control)
    ctrl = lumia.Control(rcf)

    # Create the "Interface" (to convert between control vector and model driver structure)
    interface = Interface(ctrl.name, model.name, rcf, ancilliary=emis)

    # ... Should this to to the optimizer?
    ctrl.setupPrior(interface.StructToVec(emis))
    unc = lumia.Uncertainties(rcf, ctrl.vectors)
    ctrl.setupUncertainties(unc())

    # Initialize the optimization and run it
    opt = lumia.optimizer.Optimizer(rcf, ctrl, model, interface)
    if not setuponly :
        opt.Var4D()

if __name__ == '__main__' :

    # Read arguments
    p = ArgumentParser()
    p.add_argument('rc', help="Main configuration file (i.e. rc-file) of the inversion run")
    p.add_argument('--verbosity', help="verbosity of the run (i.e. level of logging). Choose between DEBUG, INFO (default) and WARNING", default='INFO')
    p.add_argument('--obs', '-o', help='Path to the observation file (default taken from rc-file')
    p.add_argument('--emis', '-e', help='Path to the (pre-processed) emission/flux file)')
    p.add_argument('--setuponly', '-s', action='store_true', help='use this flag to do the setup but not launch the actual optimization (for debug purpose)')
    args = p.parse_args()

    optimize(args.rc, args.obs, args.emis, args.setuponly, verbosity=args.verbosity)
