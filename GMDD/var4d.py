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
    tracers = rcf.rcfGet('run.tracers',  default=['co2'])
    tracer=tracers[0]
    if (isinstance(rcf['observations'][tracer]['file']['tracer'], list)):
        trac=rcf['observations'][tracer]['file']['tracer']
        tracer=trac[0]
    else:
        tracer=rcf['observations'][tracer]['file']['tracer']        
    #obsfile = rcf.rcfGet('observations.filename')
    obsfile = rcf.rcfGet(f'observations.{tracer}.file.path')

    # Read additional basic settings from rc-file:
    start = datetime(*rcf.rcfGet('time.start'))
    end = datetime(*rcf.rcfGet('time.end'))

    # Load the observations database
    db = obsdb(filename=obsfile, start=start, end=end)
    # bugfix for  rcf.rcfGet('footprints.setup', default=True)
    # bFootprintsSetup=rcf.getAlt('footprints','setup', default=True)
    bFootprintsSetup= rcf.rcfGet('footprints.setup', default=True)
    if(bFootprintsSetup):
        db.setupFootprints(path=rcf.rcfGet('footprints.path'), cache=rcf.rcfGet('footprints.cache'))

    # Eventual refinement of the obs sites selection:
    bObsUseSites=rcf.rcfGet("observations.use_sites", default=False)
    #bObsUseSites=rcf.getAlt("observations","use_sites", default=False)
    if (bObsUseSites):
        db.SelectSites(rcf.rcfGet("observations.use_sites"))
    tracers = rcf.rcfGet('run.tracers',  default=['CO2'])
    # Load the pre-processed emissions:
    # categories = dict.fromkeys(rcf.rcfGet('emissions.categories') + rcf.rcfGet('emissions.categories.extras', default=[]))
    sRcf=categories = rcf.rcfGet(f'emissions.{tracers[0]}.categories.extras', default=[])  #rcf.getAlt('emissions','categories','extras', default=[])
    categories = dict.fromkeys(rcf.rcfGet(f'emissions.{tracers[0]}.categories') + sRcf)
    for cat in categories :
        categories[cat] = rcf.rcfGet(f'emissions.{tracers[0]}.{cat}.origin')
    emis = lagrange.ReadArchive(rcf.rcfGet(f'emissions.{tracers[0]}.prefix'), start, end, categories=categories)

    # Initialize the obs operator (transport model)
    model = lumia.transport(rcf, obs=db, formatter=lagrange)

    # Initialize the data container (control)
    ctrl = monthlyFlux.Control(rcf, preconditioner=precon)

    # Create the "Interface" (to convert between control vector and model driver structure)
    interface = Interface(ctrl.name, model.name, rcf, ancilliary=emis)

    # ... Should this to to the optimizer?
    ctrl.setupPrior(interface.StructToVec(emis))#, lsm_from_file=rcf.rcfGet('emissions.lsm.file')))
    errtype = rcf.rcfGet('optim.err.type', default='monthlyPrior')
    # errtype=rcf.getAlt('optim','err','type', default='monthlyPrior')
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
