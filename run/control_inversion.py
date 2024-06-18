import lumia
from pathlib import Path
import pdb
import numpy as np
from lumia.utils.housekeeping import documentThisRun,  getTracer
import sys

conf= lumia.read_config("control_inversion.yaml",machine="cosmos")
lumia.settings.write(conf, Path(conf.run.paths.output) / 'config_control_inversion.yaml')

"Loading obs"
# obs = lumia.Observations.from_tar(conf.observations.file.path)
try:
    tracer=getTracer(conf.run.tracers)
    if('co2' in tracer):
        obs = lumia.Observations.from_tar(conf.observations.co2.file.path)
    else:
        obs = lumia.Observations.from_tar(conf.observations.ch4.file.path)
except:
    try:
        obs = lumia.Observations.from_tar(conf.observations.file.path)
    except:
        print(f'Fatal error: Key observations.{tracer}.file.path not found in yaml config file. Did you forget the tracer key level?')
        sys.exit(-5)

#obs.observations.loc[:, 'obs'] = np.random.normal(obs.observations['mix_truth'].values, obs.observations['err'].values)
obs.observations.loc[:, 'obs'] = obs.observations['mix_truth'].values
"Done loading obs"
emis = lumia.Data.from_dconf(conf, conf.run.start, conf.run.end)
"Done loading emissions"
transport = lumia.Transport(**conf.model)
"Done running the transport"
# brings model to vector 
mapping = lumia.Mapping.init(conf, emis)
"Done running with the mapping"
prior = lumia.PriorConstraints.setup(conf.run.paths, mapping)

"run inversion"
opt = lumia.Optimizer(
   prior = prior,
   model = transport, 
   mapping = mapping,
   observations = obs,
   settings = conf.run.optimizer
)
pdb.set_trace()
x_apos = opt.solve()
opt.vectors.loc[:, 'state_preco_apos'] = x_apos
opt.vectors.loc[:, 'state_apos'] = opt.xc_to_x(x_apos)
apos = mapping.vec_to_struct(opt.vectors.state_apos.values)
opt.vectors.to_xarray().to_netcdf(Path(conf.run.paths.output) / 'states.nc')
emis.to_intensive() # Deal with the units # check later!   
emis.to_netcdf(Path(conf.run.paths.output) / 'emissions.apri.nc')
apos.to_netcdf(Path(conf.run.paths.output) / 'emissions.apos.nc')
obs.save_tar(Path(conf.run.paths.output) / 'observations.apos.tar.gz')



import lumia
from pathlib import Path
import pdb
import numpy as np
from lumia.utils.housekeeping import documentThisRun,  getTracer
import sys


args=None
ymlConfigFile="control_inversion.yaml"
myMachine="skuggfaxe"
# Do the housekeeping like documenting the current git commit version of this code, date, time, user, platform etc.
thisScript='LumiaMaster'
(newYmlFile, oldDiscoveredObservations)=documentThisRun(ymlConfigFile, thisScript,  args, myMachine=myMachine)  # from housekeepimg.py
print(f'updated configuratrion yaml file written to {newYmlFile}')

# oldDiscoveredObservations is not needed in LumiaDA, only in lumiaGUI
# Now the config.yml file has all the details for this particular run


conf= lumia.read_config(ymlConfigFile,machine=myMachine)
#conf= lumia.read_config("LumiaDA-2024-05-15T16_46-config.yml",machine="skuggfaxe")
#lumia.settings.write(conf, Path(conf.run.paths.output) / 'config_control_inversion.yaml')
p1=Path(conf.run.thisRun.uniqueOutputPrefix)
p2=Path(conf.run.thisRun.uniqueOutputPrefix) +'config_control_inversion.yaml'
print(f'p1={p1},  p2={p2}')
lumia.settings.write(conf, Path(conf.run.thisRun.uniqueOutputPrefix)+'config_control_inversion.yaml')

"Loading obs"
#obs = lumia.Observations.from_tar(conf.observations.file.path)
tracer=getTracer(conf.run.tracers)
        
#obs.observations.loc[:, 'obs'] = np.random.normal(obs.observations['mix_truth'].values, obs.observations['err'].values)
obs.observations.loc[:, 'obs'] = obs.observations['mix_truth'].values
"Done loading obs"
emis = lumia.Data.from_dconf(conf, conf.run.start, conf.run.end)
print(f'emis={emis}')
"Done loading emissions"
transport = lumia.Transport(**conf.model)
"Done running the transport"
# brings model to vector 
mapping = lumia.Mapping.init(conf, emis)
"Done running with the mapping"
print(f'conf.run.paths={conf.run.paths}')
prior = lumia.PriorConstraints.setup(conf.run.paths, mapping)

p=Path(conf.run.thisRun.uniqueOutputPrefix) +'emissions.apri.nc'
print(f'dconf.run.thisRun.uniqueOutputPrefix+emissions.apri.nc={p}')
"run inversion"
opt = lumia.Optimizer(
   prior = prior,
   model = transport, 
   mapping = mapping,
   observations = obs,
   settings = conf.run.optimizer
)
x_apos = opt.solve()
opt.vectors.loc[:, 'state_preco_apos'] = x_apos
opt.vectors.loc[:, 'state_apos'] = opt.xc_to_x(x_apos)
apos = mapping.vec_to_struct(opt.vectors.state_apos.values)
opt.vectors.to_xarray().to_netcdf(Path(conf.run.paths.output) / 'states.nc')
emis.to_intensive() # Deal with the units # check later!   
#emis.to_netcdf(Path(conf.run.paths.output) / 'emissions.apri.nc')
#apos.to_netcdf(Path(conf.run.paths.output) / 'emissions.apos.nc')
#obs.save_tar(Path(conf.run.paths.output) / 'observations.apos.tar.gz')
emis.to_netcdf(Path(conf.run.thisRun.uniqueOutputPrefix) +'emissions.apri.nc')
apos.to_netcdf(Path(conf.run.thisRun.uniqueOutputPrefix) + 'emissions.apos.nc')
obs.save_tar(Path(conf.run.thisRun.uniqueOutputPrefix) + 'observations.apos.tar.gz')
