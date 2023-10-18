#!/usr/bin/env python3

import os
import sys
from pandas import Timestamp
from argparse import ArgumentParser
import lumia
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
p.add_argument('--gui', dest='gui', default=False, action='store_true',  help="An optional graphical user interface is called at the start of Lumia to ease its configuration.")
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
       

# Shall we call the GUI to tweak some parameters before we start the ball rolling?
if args.gui:
        #(updatedYmlContents) = callLumiaGUI(ymlContents, sLogCfgPath)
    # callLumiaGUI(rcf, args.start,  args.end )
    script_directory = os.path.dirname(os.path.abspath(sys.argv[0]))
    sCmd ='python3 '+script_directory+'/lumia/GUI/lumiaGUI.py '
    for entry in sys.argv[1:]:
        if (len(entry)>0):
            sCmd+=' '+entry
    try:
        returnValue=os.system(sCmd)
    except:
        logger.error(f"Calling LumiaGUI failed. {returnValue} Execution stopped.")
        sys.exit(42)
    logger.info("LumiaGUI window closed")
    if(os.path.isfile("LumiaGui.stop")):
        logger.error("The user canceled the call of Lumia or soemthing went wrong in the GUI. Execution aborted. Lumia was not called.")
        sys.exit(42)

# Now read the yaml configuration file - whether altered by the GUI or not
try:
    rcf=rc(ymlFile)
except:
    logger.error(f"Unable to read user provided configuration file {ymlFile}. Abort")
    sys.exit(-2)


if args.setkey :
    for kv in args.setkey :
        k, v = kv.split(':')
        rcf.setkey(k, v)

# Default paths, common to all runs within this container, normally
defaults = {
    # Global paths
    'path.data': '/data',
    'run.paths.temp': os.path.join('/temp', args.tag),
    'run.paths.footprints': '/footprints',
    'correlation.inputdir': '/data/corr',

    # Run-dependent paths
    'tag': args.tag,
    'run.paths.output': os.path.join('/output', args.tag),
    'var4d.communication.file': '${run.paths.temp}/congrad.nc',
    'emissions.*.archive': 'rclone:lumia:fluxes/nc/',
    'emissions.*.path': '/data/fluxes/nc',
    'model.transport.exec': '/lumia/transport/multitracer.py',
    'transport.output': 'T',
    'transport.output.steps': ['forward'],
}

# for tr in rcf.get('run.tracers', tolist='force'):
for tr in list(rcf['run']['tracers']):      # or  list(rcf.get('run.tracers'))
    defaults[f'emissions.{tr}.archive'] = f'rclone:lumia:fluxes/nc/${{emissions.{tr}.region}}/${{emissions.{tr}.interval}}/'

# Read simulation time
if args.start is None :
    start = Timestamp(rcf.get('time.start'))
else :
    start = Timestamp(args.start)
    rcf.setkey('time.start', start.strftime('%Y,%m,%d'))
if args.end is None :
    end = Timestamp(rcf.get('time.end'))
else :
    end = Timestamp(args.end)
    rcf.setkey('time.end', end.strftime('%Y,%m,%d'))


# Save  all details of the configuration and the version of the software used:
from datetime import datetime
current_date = datetime.now()
sNow=current_date.isoformat("T","minutes")
sCmd=("mkdir -p "+rcf['run']['paths']['output'])
try:
    os.system(sCmd)
except:
    print(".")
if len(rcf['run']['paths']['output'])<1:
    sLogCfgFile="./Lumia-runlog-"+sNow[:-3]+"config.yml"
else:
    sLogCfgFile=rcf['run']['paths']['output']+"/Lumia-runlog-"+sNow+"-config.yml"
# Shall we call the GUI to tweak some parameters before we start the ball rolling?

sCmd="cp "+args.ymf+" "+sLogCfgFile
os.system(sCmd)
myFile = open(sLogCfgFile, mode="a")
myFile.write('\n')
myFile.write('softwareUsed:\n')
myFile.write('    lumia:\n')
myFile.write('        branch : gitkraken://repolink/778bf0763fae9fad55be85dde4b42613835a3528/branch/LumiaDA?url=git%40github.com%3Alumia-dev%2Flumia.git\n')
myFile.write('        commit : gitkraken://repolink/778bf0763fae9fad55be85dde4b42613835a3528/commit/5e5e9777a227631d6ceeba4fd8cff9b241c55de1?url=git%40github.com%3Alumia-dev%2Flumia.git\n')
myFile.write('    runflex:\n')
myFile.write('        branch : gitkraken://repolink/b9411fbf7aeeb54d7bb34331a98e2cc0b6db9d5f/branch/v2?url=https%3A%2F%2Fgithub.com%2Flumia-dev%2Frunflex.git\n')
myFile.write('        commit : gitkraken://repolink/b9411fbf7aeeb54d7bb34331a98e2cc0b6db9d5f/commit/aad612b36a247046120bda30c8837acb5dec4f26?url=https%3A%2F%2Fgithub.com%2Flumia-dev%2Frunflex.git\n')
myFile.close()



# Create subfolder based on the inversion time:
defaults['run.paths.output'] = os.path.join(defaults['run.paths.output'], f'{start:%Y%m%d}-{end:%Y%m%d}')
defaults['run.paths.temp'] = os.path.join(defaults['run.paths.temp'], f'{start:%Y%m%d}-{end:%Y%m%d}')

rcf.set_defaults(**defaults)

logger.info(f"Temporary files will be stored in {rcf.get('run.paths.temp')}")
logger.info(f"Temporary files will be stored in {rcf['run']['paths']['temp']}")


# lumia.paths.setup(rcf)


# Load observations
if args.noobs :
    from lumia.obsdb.runflex import obsdb
    db = obsdb(rcf.get('paths.footprints'), start, end)
elif args.forward or args.optimize or args.adjtest or args.gradtest or args.adjtestmod:
    sLocation=rcf['observations']['file']['location']
    if ('CARBONPORTAL' in sLocation):
        from lumia.obsdb.obsCPortalDb import obsdb
        db = obsdb.from_CPortal(rcf)
        db.observations.to_csv('obsDataAll3.csv', encoding='utf-8', mode='w', sep=',')
    else:
        from lumia.obsdb.InversionDb import obsdb
        db = obsdb.from_rc(rcf)
        db.observations.to_csv('obsDataAllFromLocal3.csv', encoding='utf-8', mode='w', sep=',')
else :
    # if we just want to write the emissions ...
    db = None

# TODO: Done. I have moved " Load the pre-processed emissions" to after Load observations block again - just for testing so we can 
# work on reading co2 emissions via DA before being bugged down by reading observations in the debugger....
# Load the pre-processed emissions like fossil/EDGAR, marine, vegetation, ...:
emis = xr.Data.from_rc(rcf, start, end)
emis.print_summary()

# Create model instance
model = lumia.transport(rcf, obs=db, formatter=xr)

# Do a model run (or something else ...).
if args.forward :
    logger.info(f" run_args.forward(): before calling .calcDepartures(emis, forward) with emis={emis}")
    model.calcDepartures(emis, 'forward')


# Setup uncertainties if needed:
if args.optimize or args.gradtest :
    if rcf.get('observations.uncertainty.frequency') == 'dyn':
        logger.info(f" run_args.optimize_dyn(): before calling .calcDepartures(emis, apri) with emis={emis}")
        model.calcDepartures(emis, 'apri')   # goes via obsoperator_init() -> obsoperator.calcDepartures() -> obsoperator.runForward()
        db.setup_uncertainties_dynamic(
            'mix_apri',
            rcf.get('observations.uncertainty.dyn.freq', default='7D'),
            rcf.get('observations.uncertainty.obs_field', default='err_obs')
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
        if rcf.get('observation.validation_file', default=False):
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