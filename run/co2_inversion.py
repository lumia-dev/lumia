#!/usr/bin/env python

import sys
from argparse import ArgumentParser
from pathlib import Path
from pandas import Timestamp
from pandas.tseries.frequencies import to_offset

import lumia
from lumia.models import footprints


# Argument parser
p = ArgumentParser()
p.add_argument('--machine', '-m', 
        help='Name of the section of the yaml file to be used as "machine". It should contain the machine-specific settings (paths, number of CPUs, paths to secrets, etc.)')
p.add_argument('config', type=Path, 
        help='Path to the config file (yaml file)')
p.add_argument('--verbosity', '-v', default='DEBUG', type=lumia.setup_logging,
        help='logging level. This needs to be one of TRACE, DEBUG, INFO, SUCCESS, WARNING, ERROR or CRITICAL')
p.add_argument('--start', default=None, 
        help='Start of the period for which the emissions should be optimized. This needs to be a string understandable by pandas.Timestamp')
p.add_argument('--end', default=None, 
        help='End of the period for which the emissions should be optimized. This needs to be a string understandable by pandas.Timestamp')
p.add_argument('--spinup', default=None, type=str, 
        help='Length of the buffer at the beginning of the simulation (period that will be included in the inversion, but trimmed from the results. This needs to be a string undertood by pandas.tseries.frequencies.to_offset')
p.add_argument('--spindown', default=None, type=str, 
        help='Length of the buffer at the end of the simulation (period that will be included in the inversion, but trimmed from the results. This needs to be a string undertood by pandas.tseries.frequencies.to_offset')
args = p.parse_args(sys.argv[1:])


#====================================================
# Process the configuration file and the arguments
# After this section, the settings are considered read-only (but this is not enforced!)
#
# Load the config file:
dconf = lumia.settings.read(
    args.config, 
    machine=args.machine, 
    run = {
        'start': args.start,
        'end': args.end,
        'spinup': args.spinup,
        'spindown': args.spindown
     }
)


# Append the start and end times to the tag:
dconf.run.tag = f'{dconf.run.tag}/{Timestamp(dconf.run.start):%Y%m%d}-{Timestamp(dconf.run.end):%Y%m%d}'


# Save the settings
lumia.settings.write(dconf, Path(dconf.run.paths.output) / 'config.yaml')


#====================================================

# Handle the spin-up / spin-down:
spinup = to_offset(dconf.run.get('spinup', '0h'))
spindown = to_offset(dconf.run.get('spindown', '0h'))
dconf.run.start = str(Timestamp(dconf.run.start) - spinup)
dconf.run.end = str(Timestamp(dconf.run.end) + spindown)


# Setup the observations
obs = lumia.Observations.from_tar(dconf.observations.file)
obs.settings.update(**dconf.observations.get("uncertainties", {}))


# Setup the emissions
emis = lumia.Data.from_dconf(dconf, start=dconf.run.start, end=dconf.run.end)


# Setup the transport model
transport = lumia.Transport(**dconf.model)


# Setup the mapping
mapping = footprints.Mapping.init(dconf, emis)


# Setup the prior uncertainties
prior = lumia.PriorConstraints.setup(dconf.run.paths, mapping)


# Setup the optimizer
opt = lumia.Optimizer(
    prior=prior, 
    model=transport, 
    mapping=mapping, 
    observations=obs, 
    settings=dconf.run.optimizer
)


x_opt = opt.solve()
opt.vectors.loc[:, 'state_preco_apos'] = x_opt
opt.vectors.loc[:, 'state_apos'] = opt.xc_to_x(x_opt)
apos = mapping.vec_to_struct(opt.vectors.state_apos.values)
opt.vectors.to_xarray().to_netcdf(Path(dconf.run.paths.output) / 'states.nc')
