#!/usr/bin/env python

import lumia
from argparse import ArgumentParser
from lumia.obsdb.footprintdb import obsdb
from datetime import datetime
from lumia.formatters import lagrange
from lumia.interfaces import Interface
from lumia.Tools.logging_tools import logger
from lumia.obsdb.backgroundDb import backgroundDb
from lumia.obsdb.invdb import invdb
import os

def optimize(rcfile, obs=None, emfile=None, setuponly=False, verbosity='INFO'):

    # Set verbosity level
    logger.setLevel(verbosity)

    # Create basic objects
    rcf = lumia.rc(rcfile)
    obsfile = rcf.get('observations.input_file') if obs is None else args.obs

    # Read additional basic settings from rc-file:
    start = datetime(*rcf.get('time.start'))
    end = datetime(*rcf.get('time.end'))

    # Add "tag" based on dates:
    rcf.setkey('tag', f'{start.strftime("%Y%m%d%H")}-{end.strftime("%Y%m%d%H")}')

    # Load the observations database
    db = obsdb(filename=obsfile, start=start, end=end)
    db.setupFootprints(path=rcf.get('footprints.path'), cache=rcf.get('footprints.cache'))

    # Setup background and uncertainties if needed:
    if rcf.get('obs.setup_bg'):
        db = backgroundDb(db=db)
        db.read_backgrounds(path=rcf.get('backgrounds.path'))

    if rcf.get('obs.setup_uncertainties'):
        db = invdb(db=db)
        db.setupUncertainties(
            err_obs_min=rcf.get('obs.err_obs_min'), err_obs_fac=rcf.get('obs.err_obs_fac', default=1),
            err_mod_min=rcf.get('obs.err_mod_min'), err_mod_fac=rcf.get('obs.err_mod_fac', default=1),
            err_bg_min=rcf.get('obs.err_bg_min'), err_bg_fac=rcf.get('obs.err_bg_fac', default=1),
            err_tot_min=rcf.get('obs.err_min'), err_tot_max=rcf.get('obs.err_max', None),
            err_bg_field='err_profile_bg'
        )

    # Load the pre-processed emissions:
    if emfile is None :
        categories = dict.fromkeys(rcf.get('emissions.categories'))
        for cat in categories :
            categories[cat] = rcf.get(f'emissions.{cat}.origin')
        emis = lagrange.ReadArchive(rcf.get('emissions.prefix'), start, end, categories=categories)
    else :
        emis = lagrange.ReadStruct(emfile)

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
    else :
        return opt

if __name__ == '__main__' :

    # Read arguments
    p = ArgumentParser()
    p.add_argument('rc', help="Main configuration file (i.e. rc-file) of the inversion run")
    p.add_argument('--verbosity', help="verbosity of the run (i.e. level of logging). Choose between DEBUG, INFO (default) and WARNING", default='INFO')
    p.add_argument('--obs', '-o', help='Path to the observation file (default taken from rc-file')
    p.add_argument('--emis', '-e', help='Path to the (pre-processed) emission/flux file)')
    p.add_argument('--setuponly', '-s', action='store_true', help='use this flag to do the setup but not launch the actual optimization (for debug purpose)')
    args = p.parse_args()

    opt = optimize(args.rc, obs=args.obs, emfile=args.emis, setuponly=args.setuponly, verbosity=args.verbosity)
