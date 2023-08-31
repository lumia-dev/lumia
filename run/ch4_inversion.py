#!/home/gmonteil/.conda/envs/coco2_wp5/bin/python
#SBATCH -t 2:00:00 -n 48 --exclusive

from omegaconf import OmegaConf
from lumia import Observations
from lumia.models import footprints
from lumia.prior import PriorConstraints
from lumia.optimizer import Var4D
from argparse import ArgumentParser
from pathlib import Path
import sys
from loguru import logger
from numpy import zeros_like
from pandas import Timestamp
from pandas.tseries.frequencies import to_offset


p = ArgumentParser()
p.add_argument('--machine', '-m', help='Name of the section of the yaml file to be used as "machine". It should contain the machine-specific settings (paths, number of CPUs, paths to secrets, etc.)')
p.add_argument('config', type=Path, help='Path to the config file (yaml file)')
p.add_argument('--verbosity', '-v', help='Logging level', default='INFO')
p.add_argument('--start', default=None)
p.add_argument('--end', default=None)
p.add_argument('--spinup', default=None, type=str)
p.add_argument('--spindown', default=None, type=str)
args = p.parse_args(sys.argv[1:])


# Setup logging
logger.remove()
logger.add(
    sys.stdout, 
    format='<g>{elapsed}</> | <level>{level: <8}</level> | <c>{file.path}</>:<c>{line})</> | {message}',
    level=args.verbosity
)


# Load the config file:
dconf = OmegaConf.load(args.config)


# Ensure we read the good "machine" section:
dconf['machine'] = dconf[args.machine]


# Handle arguments:
if args.start :
    dconf.run.start = args.start
if args.end :
    dconf.run.end = args.end


# Append the start and end times to the tag:
dconf.run.tag = f'{dconf.run.tag}/{Timestamp(dconf.run.start):%Y%m%d}-{Timestamp(dconf.run.end):%Y%m%d}'


# save the settings
Path(dconf.run.paths.output).mkdir(parents=True, exist_ok=True)
OmegaConf.save(config=dconf, f=Path(dconf.run.paths.output) / 'config.yaml')


# Handle the spin-up / spin-down:
if args.spinup:
    dconf.run.spinup = args.spinup
if args.spindown:
    dconf.run.spindown = args.spindown
spinup = to_offset(dconf.run.get('spinup', '0h'))
spindown = to_offset(dconf.run.get('spindown', '0h'))
dconf.run.start = str(Timestamp(dconf.run.start) - spinup)
dconf.run.end = str(Timestamp(dconf.run.end) + spindown)


# Setup the observations
obs = Observations.from_tar(dconf.observations.file)
obs.select_times(tmin=dconf.run.start, tmax=dconf.run.end)
obs_hour = obs.observations.time.dt.hour
select = zeros_like(obs_hour, dtype=bool)
for site in obs.sites.itertuples():
    start, end = site.assim_start, site.assim_end
    site_selector = obs.observations.site == site.Index
    if start > end :
        # if the time window starts before midnight and ends after, use a OR operator
        select[site_selector] = ((obs_hour >= start) | (obs_hour <= end)).loc[site_selector]
    else :
        # else, use a regular AND operator
        select[site_selector] = ((obs_hour >= start) & (obs_hour <= end)).loc[site_selector]
obs.observations = obs.observations[select]
for k, v in dconf.observations.get('rename', {}).items():
    obs.observations.loc[:, v] = obs.observations.loc[:, k]
obs.settings.update(**dconf.observations.uncertainties)


# Setup the emissions
emis = footprints.Data.from_dconf(dconf, dconf.run.start, dconf.run.end)


# Setup the transport model
transport = footprints.Transport(**dconf.model)


# Setup the prior/mapping
# sensi_map = transport.calc_sensi_map(emis)
mapping = footprints.Mapping.init(dconf, emis) #, sensi_map=sensi_map)
prior = PriorConstraints.setup(dconf.run.paths, mapping)


# Setup the optimizer
opt = Var4D(prior=prior, model=transport, mapping=mapping, observations=obs, settings=dconf.congrad)


# Run the inversion
opt.vectors.loc[:, 'state_preco_apos'] = opt.solve()
opt.vectors.loc[:, 'state_apos'] = opt.xc_to_x(opt.vectors.state_preco_apos.values)
opt.vectors.to_xarray().to_netcdf(Path(dconf.run.paths.output) / 'states.nc')


# Validation:
apri = footprints.Data.from_file(Path(dconf.run.paths.output) / 'emissions.apri.nc')
apos = footprints.Data.from_file(Path(dconf.run.paths.output) / 'emissions.apos.nc')
valid = Observations.from_tar(dconf.observations.validation_file)
valid.select_times(tmin=dconf.run.start, tmax=dconf.run.end)
for k, v in dconf.observations.get('rename', {}).items():
    valid.observations.loc[:, v] = valid.observations.loc[:, k]
transport.setup_observations(valid)
transport.calc_departures(apri, step='validation_apri')
transport.calc_departures(apos, step='validation_apos')
valid.observations = valid.observations.dropna(subset='mix_validation_apri')
valid.sites = valid.sites.loc[valid.observations.site.drop_duplicates()]
valid.save_tar(Path(dconf.run.paths.output) / 'observations.valid.tar.gz')


# Save also the category-specific emissions:
emis.to_intensive()
emis.to_netcdf(Path(dconf.run.paths.output) / 'emissions.apri_allfields.nc')
