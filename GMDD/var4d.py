#!/usr/bin/env python

import os
from datetime import datetime
from argparse import ArgumentParser
import lumia
from lumia.obsdb.footprintdb import obsdb
from lumia.formatters import lagrange
from lumia.interfaces import Interface
from lumia.Tools.logging_tools import logger
from lumia.control import monthlyFlux
from lumia.precon import preconditioner_ray as precon
from lumia.Uncertainties import *

def optimize(rcfile, setuponly=False, verbosity='INFO'):

    # Set verbosity level
    logger.setLevel(verbosity)

    # Create basic objects
    rcf = lumia.rc(rcfile)
    obsfile = rcf.get('observations.filename')

    # Read additional basic settings from rc-file:
    start = datetime(*rcf.get('time.start'))
    end = datetime(*rcf.get('time.end'))

    # Load the observations database
    db = obsdb(filename=obsfile, start=start, end=end)
    # bugfix for  rcf.get('footprints.setup', default=True)
    bFootprintsSetup=rcf.getAlt('footprints','setup', default=True)
    if(bFootprintsSetup):
        db.setupFootprints(path=rcf.get('footprints.path'), cache=rcf.get('footprints.cache'))

    # Eventual refinement of the obs sites selection:
    # bugfix for rcf.get("observations.use_sites", default=False):
    bObsUseSites=rcf.getAlt("observations","use_sites", default=False)
    if (bObsUseSites):
        db.SelectSites(rcf.get("observations.use_sites"))

    # Load the pre-processed emissions:
    # categories = dict.fromkeys(rcf.get('emissions.categories') + rcf.get('emissions.categories.extras', default=[]))
    sRcf=rcf.getAlt('emissions','categories','extras', default=[])
    categories = dict.fromkeys(rcf.get('emissions.categories') + sRcf)
    for cat in categories :
        categories[cat] = rcf.get(f'emissions.{cat}.origin')
    emis = lagrange.ReadArchive(rcf.get('emissions.prefix'), start, end, categories=categories)

    # Initialize the obs operator (transport model)
    model = lumia.transport(rcf, obs=db, formatter=lagrange)

    # Initialize the data container (control)
    ctrl = monthlyFlux.Control(rcf, preconditioner=precon)

    # Create the "Interface" (to convert between control vector and model driver structure)
    interface = Interface(ctrl.name, model.name, rcf, ancilliary=emis)

    # ... Should this to to the optimizer?
    ctrl.setupPrior(interface.StructToVec(emis))#, lsm_from_file=rcf.get('emissions.lsm.file')))
    # bugfix for errtype = rcf.get('optim.err.type', default='monthlyPrior')
    errtype=rcf.getAlt('optim','err','type', default='monthlyPrior')
    if errtype == 'monthlyPrior' :
        unc = PercentMonthlyPrior
    elif errtype == 'hourlyPrior' : 
        unc = PercentHourlyPrior
    elif errtype == 'annualPrior' : 
        unc = PercentAnnualPrior
    elif errtype == 'true_error' : 
        unc = ErrorFromTruth
    elif errtype == 'hourlyPrior_scaled':
        unc = PercentHourlyPrior_homogenized
    err = unc(rcf, interface)(emis)
    ctrl.setupUncertainties(err)

    # Initialize the optimization and run it
    #from numpy import zeros
    opt = lumia.optimizer.Optimizer(rcf, ctrl, model, interface)
    if not setuponly :
        opt.Var4D()
    return opt

if __name__ == '__main__' :

    # Read arguments
    p = ArgumentParser()
    p.add_argument('rc', help="Main configuration file (i.e. rc-file) of the inversion run")
    p.add_argument('--verbosity', help="verbosity of the run (i.e. level of logging). Choose between DEBUG, INFO (default) and WARNING", default='INFO')
    p.add_argument('--setuponly', '-s', action='store_true', help='use this flag to do the setup but not launch the actual optimization (for debug purpose)')
    args = p.parse_args()

    opt = optimize(args.rc, setuponly=args.setuponly, verbosity=args.verbosity)
