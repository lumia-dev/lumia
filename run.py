#!/usr/bin/env python3

import os
import sys
from pandas import Timestamp
from argparse import ArgumentParser
import lumia
import lumia.GUI.housekeeping as hk
from rctools import RcFile as rc
from loguru import logger
from lumia.formatters import xr

p = ArgumentParser()
p.add_argument('--forward', default=False, action='store_true')
p.add_argument('--model-adjtest', default=False, action='store_true', dest='adjtestmod')
p.add_argument('--adjtest', default=False, action='store_true', dest='adjtest')
p.add_argument('--serial', '-s', action='store_true', default=False, help="Run on a single CPU")
p.add_argument('--gradtest', default=False, action='store_true', dest='gradtest')
p.add_argument('--noobs', default=False, action='store_true', help="Run without an observations database, on the basis of the footprints available.")
p.add_argument('--optimize', default=False, action='store_true')
p.add_argument('--prepare_emis', default=False, action='store_true', dest='emis', help="Use this command to prepare an emission file without actually running the transport model or an inversion.")
p.add_argument('--start', default=None, help="Start of the simulation. Overwrites the value in the rc-file")
p.add_argument('--end', default=None, help="End of the simulation. Overwrites the value in the rc-file")
# p.add_argument('--gui', dest='gui', default=False, action='store_true',  help="An optional graphical user interface is called at the start of Lumia to ease its configuration.")
p.add_argument('--setkey', action='append', help="use to override some rc-keys (ignored by the GUI)")
p.add_argument('--tag', default='')
p.add_argument('--rcf')   # what used to be the resource file (now yaml file) - only yaml format is supported
p.add_argument('--ymf')   # yaml configuration file where the user plans his or her Lumia run: parameters, input files etc.
p.add_argument('--verbosity', '-v', default='INFO')
args = p.parse_args(sys.argv[1:])


# Set the verbosity in the logger (loguru quirks ...)
logger.remove()
logger.add(sys.stderr, level=args.verbosity)

    
ymlFile=None
if(args.rcf is None):
    if(args.ymf is None):
        print("Lumia: Fatal error: no user configuration (yaml) file provided.")
        sys.exit(1)
    else:
        ymlFile = args.ymf
else:            
    ymlFile = args.rcf
if (not os.path.isfile(ymlFile)):
    logger.error(f"Fatal error in lumia.run.py: User specified configuration file {ymlFile} does not exist. Abort.")
    sys.exit(-3)
       

# Do the housekeeping like documenting the current git commit version of this code, date, time, user, platform etc.
thisScript='LumiaDA'
sCmd=hk.documentThisRun(ymlFile, thisScript,  args)  # from housekeepimg.py
# Now the config.yml file has all the details for this particular run

# Shall we call the GUI to tweak some parameters before we start the ball rolling?
# if args.gui:
#     try:
#         returnValue=os.system(sCmd)
#     except:
#         logger.error(f"Calling LumiaGUI failed. {returnValue} Execution stopped.")
#         sys.exit(42)
#     logger.info("LumiaGUI window closed")
#     if(os.path.isfile("LumiaGui.stop")):
#         logger.error("The user canceled the call of Lumia or something went wrong in the GUI. Execution aborted. Lumia was not called.")
#         sys.exit(42)

# Now read the yaml configuration file - whether altered by the GUI or not
try:
    rcf=rc(ymlFile)
except:
    logger.error(f"Unable to read user provided configuration file {ymlFile}. Please check file existance and its data format. Abort")
    sys.exit(-2)

if args.setkey :
    for kv in args.setkey :
        k, v = kv.split(':')
        rcf.setkey(k, v)

# Default paths, common to all runs within this container, normally
sTmpPrfx=rcf[ 'run']['thisRun']['uniqueTmpPrefix']
defaults = {
    # Global paths
    'path.data': '/data',
    'run.paths.temp': os.path.join('/temp', args.tag),
    'run.paths.footprints': '/footprints',
    'correlation.inputdir': '/data/corr',

    # Run-dependent paths
    'tag': args.tag,
    'run.paths.output': os.path.join('/output', args.tag),
    'var4d.communication.file': sTmpPrfx+'congrad.nc',
    'emissions.*.archive': 'rclone:lumia:fluxes/nc/',
    'emissions.*.path': '/data/fluxes/nc',
    'model.transport.exec': '/lumia/transport/multitracer.py',
    'transport.output': 'T',
    'transport.output.steps': ['forward']
}

# for tr in rcf.rcfGet('run.tracers', tolist='force'):
LLst=rcf['run']['tracers']
for tr in LLst: 
#for tr in list(rcf['run']['tracers']):      # or  list(rcf.rcfGet('run.tracers'))
    defaults[f'emissions.{tr}.archive'] = f'rclone:lumia:fluxes/nc/${{emissions.{tr}.region}}/${{emissions.{tr}.interval}}/'

# Read simulation time
if args.start is None :
    start = Timestamp(rcf.rcfGet('time.start'))
else :
    start = Timestamp(args.start)
    rcf.setkey('time.start', start.strftime('%Y,%m,%d'))
if args.end is None :
    end = Timestamp(rcf.rcfGet('time.end'))
else :
    end = Timestamp(args.end)
    rcf.setkey('time.end', end.strftime('%Y,%m,%d'))


# Create subfolder based on the inversion time:
defaults['run.paths.output'] = os.path.join(defaults['run.paths.output'], f'{start:%Y%m%d}-{end:%Y%m%d}')
defaults['run.paths.temp'] = os.path.join(defaults['run.paths.temp'], f'{start:%Y%m%d}-{end:%Y%m%d}')

rcf.set_defaults(**defaults)

logger.info(f"Temporary files will be stored in {rcf.rcfGet('run.paths.temp')}")
#logger.info(f"Temporary files will be stored in {rcf['run']['paths']['temp']}")

sNow=rcf[ 'run']['thisRun']['uniqueIdentifierDateTime']
sOut=rcf.rcfGet('run.paths.output')
sOutputPrfx=rcf[ 'run']['thisRun']['uniqueOutputPrefix']
if not os.path.exists(f'{sOutputPrfx}config.yml'):
    sCmd=f'cp {ymlFile} {sOutputPrfx}config.yml'
    try:
        os.system(sCmd)
    except:
        logger.error("Fatal error: the following system command failed. Please check write permissions and disk space in the target directory.")
        logger.error(sCmd)
        sys.exit(-1)
        

# Load observations
if args.noobs :
    from lumia.obsdb.runflex import obsdb
    db = obsdb(rcf.rcfGet('paths.footprints'), start, end)
elif args.forward or args.optimize or args.adjtest or args.gradtest or args.adjtestmod:
    try:
        tracer='co2'
        try:
            if (isinstance(rcf['run']['tracers'], str)):
                tracer=rcf['run']['tracers']
            else:
                trac=rcf['run']['tracers']
                tracer=trac[0]
        except:
            tracer='co2'
    except:
        logger.error('Key run.tracers not retrievable from stated yaml config file')
    sLocation=rcf['observations'][tracer]['file']['location']
    # Create a proper output filename for all the combined observations. Need tracer and output directory etc.
    try:
        sLogCfgPath=""
        if ((rcf['run']['paths']['output'] is None) or len(rcf['run']['paths']['output']))<1:
            sLogCfgPath="./"
        else:
            sLogCfgPath=rcf['run']['paths']['output']+"/"
        sOut=sTmpPrfx+"_dbg_AllObsData-withBg-"+tracer+".csv"
    except:
        sOut='_dbg_obsDataAll3.csv'
    if ('CARBONPORTAL' in sLocation):
        from lumia.obsdb.obsCPortalDb import obsdb
        db = obsdb.from_CPortal(rcf=rcf, useGui=args.gui, ymlFile=ymlFile)
        db.observations.to_csv( sOut, encoding='utf-8', mode='w', sep=',')
    else:
        from lumia.obsdb.InversionDb import obsdb
        db = obsdb.from_rc(rcf)
        db.observations.to_csv( sOut[:-4]+'-local.csv', encoding='utf-8', mode='w', sep=',')
else :
    # if we just want to write the emissions ...
    db = None

# Load the pre-processed emissions like fossil/EDGAR, marine, vegetation, ...:
# sKeyword='LPJGUESS'
emis = xr.Data.from_rc(rcf, start, end)
logger.info('emis data read.')
emis.print_summary()


# Create model instance
# model.db.observations needs to marry up the co2 observational data (inclduing the backgroundCO2) with the emissions for biosphere, fossil, ocean 
model = lumia.transport(rcf, obs=db, formatter=xr)

# Do a model run (or something else ...).
if args.forward :
    logger.info(f" run_args.forward(): before calling .calcDepartures(emis, forward) with emis={emis}")
    model.calcDepartures(emis, 'forward')


# Setup uncertainties if needed:
if args.optimize or args.gradtest :
    if rcf.rcfGet('observations.uncertainty.frequency') == 'dyn':
        logger.info(f" run_args.optimize_dyn(): before calling .calcDepartures(emis, apri) with emis={emis}")
        model.calcDepartures(emis, 'apri')   # goes via obsoperator_init() -> obsoperator.calcDepartures() -> obsoperator.runForward()
        db.setup_uncertainties_dynamic(
            'mix_apri',
            rcf.rcfGet('observations.uncertainty.dyn.freq', default='7D'),
            rcf.rcfGet('observations.uncertainty.obs_field', default='err_obs')
        )
    else :
        db.setup_uncertainties()

if args.optimize or args.adjtest or args.gradtest :
    from lumia.interfaces.multitracer import Interface

    sensi = model.calcSensitivityMap(emis)
    control = Interface(rcf, model_data=emis, sensi_map=sensi)
    opt = lumia.optimizer.Optimizer(rcf, model, control)

    if args.optimize :
        logger.info(f"Entering run_args.optimize_149 and calling opt.Var4D() with opt={opt}")
        opt.Var4D()
        if rcf.rcfGet('observation.validation_file', default=False):
            obs_valid = obsdb.from_rc(rcf, filekey='observation.validation_file', setupUncertainties=False)
            model.run_forward(control.model_data, obs_valid, step='validation')
        else:
            logger.info("No observation.validation_file found.")

    elif args.adjtest :
        opt.AdjointTest()
    elif args.gradtest :
        opt.GradientTest()

elif args.emis :
    model.writeStruct(emis, rcf['run']['paths']['output'], 'modelData')

elif args.adjtestmod :
    # Test only the adjoint of the CTM (skip the lumia stuff ...).
    model.adjoint_test(emis)
print("Done.")
