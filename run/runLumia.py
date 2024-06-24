from pathlib import Path
from lumia.utils.housekeeping import documentThisRun,  getTracer
import os
import sys
from argparse import ArgumentParser
from loguru import logger
import lumia

@logger.catch(reraise=True)
def main():
    
    p = ArgumentParser()
    p.add_argument('--machine', '-m', help='Name of the section of the yaml file to be used as "machine". It should contain the machine-specific settings (paths, number of CPUs, paths to secrets, etc.)')
    p.add_argument('--config', '-c',  default="control_inversion.yaml", type=Path,  help='Path to the config file (yaml file)')
    p.add_argument('--start', default=None, 
            help='Start of the period for wh-ch the emissions should be optimized. This needs to be a string understandable by pandas.Timestamp, e.g. 2018-01-01')
    p.add_argument('--end', default=None, 
            help='End of the period for which the emissions should be optimized. This needs to be a string understandable by pandas.Timestamp, e.g. 2018-12-31')
    p.add_argument('--spinup', default=None, type=str, 
            help='Length of the buffer at the beginning of the simulation (period that will be included in the inversion, but trimmed from the results. This needs to be a string undertood by pandas.tseries.frequencies.to_offset')
    p.add_argument('--spindown', default=None, type=str, 
            help='Length of the buffer at the end of the simulation (period that will be included in the inversion, but trimmed from the results. This needs to be a string undertood by pandas.tseries.frequencies.to_offset')
    #p.add_argument('--forward', default=False, action='store_true')
    p.add_argument('--serial', '-s', action='store_true', default=False, help="Run on a single CPU")
    p.add_argument('--verbosity', '-v', default='DEBUG', 
            help='logging level. This needs to be one of TRACE, DEBUG, INFO, SUCCESS, WARNING, ERROR or CRITICAL') # type=lumia.setup_logging,
    args = p.parse_args(sys.argv[1:])
    
    if (args.machine is None):
        print("Lumia: Fatal error: no machine provided. Select one of the machines defined in your yaml config file - add machines as needed in that file.")
        sys.exit(-2)
    myMachine=args.machine
    if(args.config is None):
        print("Lumia: Warning: no user configuration (yaml) file provided. Defaulting to control_inversion.yaml in the working directory.")
        ymlConfigFile="control_inversion.yaml"    
    else:
        ymlConfigFile=str(args.config)
        
    # Do the housekeeping like documenting the current git commit version of this code, date, time, user, platform etc.
    thisScript='LumiaMaster'
    (ymlConfigFile, oldDiscoveredObservations, myMachine)=documentThisRun(ymlConfigFile, thisScript,  args, myMachine=myMachine)  # from housekeepimg.py
    # oldDiscoveredObservations is not needed in LumiaDA, only in lumiaGUI
    # Now the config.yml file has all the details for this particular run
    
    conf= lumia.read_config(ymlConfigFile,myMachine=myMachine)
    lumia.settings.write(conf, Path(ymlConfigFile)) # update the config file with the selected machine
    
    "Loading obs"
    tracer=getTracer(conf.run.tracers)
    try:
        print(f'conf.run.tracers={conf.run.tracers}')
    except:
        pass
    try:
        print(f'emissions.co2.path={conf.emissions.co2.path}')
    except:
        pass
    try:
        print(f'emissions.co2.prefix={conf.emissions.co2.prefix}')
    except:
        pass
    if('co2' in tracer):
        obs = lumia.Observations.from_tar(conf.observations.co2.file.path)
    else:
        obs = lumia.Observations.from_tar(conf.observations.ch4.file.path)
    # obs = lumia.Observations.from_tar(conf.observations.file.path)
    #obs.observations.loc[:, 'obs'] = np.random.normal(obs.observations['mix_truth'].values, obs.observations['err'].values)
    obs.observations.loc[:, 'obs'] = obs.observations['mix_truth'].values
    logger.info("Done loading obs")
    emis = lumia.Data.from_dconf(conf, conf.run.start, conf.run.end)
    logger.info(f'emis={emis}')
    logger.info("Done loading emissions")
    
    transport = lumia.Transport(**conf.model)
    logger.info("Done running the transport model")
    # brings model to vector 
    mapping = lumia.Mapping.init(conf, emis)
    logger.info("Done running with the mapping")
    logger.debug(f'conf.run.paths={conf.run.paths}')
    prior = lumia.PriorConstraints.setup(conf.run.paths, mapping)
    
    logger.info("Now run the inversion...")
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
    emis.to_netcdf(Path(conf.run.thisRun.uniqueOutputPrefix +'emissions.apri.nc'))
    apos.to_netcdf(Path(conf.run.thisRun.uniqueOutputPrefix + 'emissions.apos.nc'))
    obs.save_tar(Path(conf.run.thisRun.uniqueOutputPrefix + 'observations.apos.tar.gz'))
    p=str(conf.run.thisRun.uniqueOutputPrefix)
    p=os.path.dirname(p)
    logger.info(f'All output written to {p}')
    logger.info('Lumia run completed. Done.')

main()
