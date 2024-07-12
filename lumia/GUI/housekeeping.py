#!/usr/bin/env python3

LATESTGITCOMMIT_LumiaDA='d3c8c976e8d4b55ab590ad1bfa77b092a7d3d7c5'
LATESTGITCOMMIT_Runflex='aad612b36a247046120bda30c8837acb5dec4f26'

import os
import sys
import getpass
import platform
#import distro
import pathlib
import hashlib
import re
import yaml
from datetime import datetime
from pandas import  Timestamp  # , to_datetime
from loguru import logger


def setKeyVal_Nested_CreateIfNecessary(myDict, keyLst,   value=None,  bNewValue=False):
    ''' Creates the nested key keyLst in the dictionary myDict if it does not already exist.
        If the key already exists, then the key value is overwritten only if bNewValue is set
    '''
    nKeys=len(keyLst)
    i=int(1)
    for key in keyLst:
        if key not in myDict:
            if(i==nKeys):
                myDict[key] = value
            else:
                myDict[key] = {}
        elif((i==nKeys) and (bNewValue)):
            myDict[key] = value
        i+=1
        myDict = myDict[key]



def runSysCmd(sCmd,  ignoreError=False):
    try:
        os.system(sCmd)
    except:
        if(ignoreError==False):
            logger.error(f"Error: Failed to execute system command >>{sCmd}<<. Please check your write permissions and possibly disk space etc.")
        return False
    return True


def getTracer(ymlEntryTracer,  abortOnError=False):
    # Find out the first (only) tracer being used
    tracer='co2'
    try:
        if (isinstance(ymlEntryTracer, str)):
            tracer=ymlEntryTracer
        else:
            trac=ymlEntryTracer
            tracer=trac[0]
    except:
        logger.error('Key run.tracers not retrievable from stated yaml config file. Please make sure your config file has the key run.tracers set to co2 or ch4.')
        tracer='co2'
        if(abortOnError):
            sys.exit(-7)
        else:
            logger.warning('Proceeding with the assumption tracer==co2')
    return(tracer.lower())


def caclulateSha256Filehash(myfile):
    try:
        sha256 = hashlib.sha256()
        with open(myfile, 'rb') as g:
            sha256.update(g.read())
        sha256Value=sha256.hexdigest()
        logger.debug(f'Local file {myfile} has the sha256 hash of {sha256Value}')
    except:
        sha256Value = 'UNKNOWN'
    return(sha256Value)

current_date = datetime.now()
sNow=current_date.isoformat("T","seconds") # sNow is the time stamp for all log files of a particular run
# colons from the time are not without problems in directory and file names. Better to play it safe and replace them with underscores
sNow=re.sub(':', '_', sNow)
sNow=sNow[:-3] # minutes is good enough....don't need seconds if a run takes hours...

def documentThisRun(ymlFile,  parentScript='Lumia', args=None):
    # current version of the yml config files:
    nThisConfigFileVersion= int(6)
    nThisConfigFileSubVersion=int(2)
    # Now read the yaml configuration file - whether altered by the GUI or not
    if (args is None):
        logger.error('You need to provide some arguments')
        sys.exit(-3)
    ymlContents=None
    try:
        #rcf=rc(ymlFile)
        # Read the yaml configuration file
        tryAgain=False
        try:
            with open(ymlFile, 'r') as file:
                ymlContents = yaml.safe_load(file)
            # Create a backup ymlFile in case something goofs up or if the user cancels the run so we can revert back.
            sCmd="cp "+ymlFile+' '+ymlFile+'.bac' # create a backup file.
            os.system(sCmd)
        except:
            tryAgain=True
        if(tryAgain==True):
            sCmd="cp "+ymlFile+'.bac '+ymlFile # recover from most recent backup file.
            os.system(sCmd)
            try:
                with open(ymlFile, 'r') as file:
                    ymlContents = yaml.safe_load(file)
                sCmd="cp "+ymlFile+' '+ymlFile+'.bac' # create a backup file.
                os.system(sCmd)
            except:
                tryAgain=True
                logger.error(f"Abort! Unable to read yaml configuration file {ymlFile} - failed to read its contents with yaml.safe_load()")
                sys.exit(1)
    except:
        logger.error(f"Unable to read user provided configuration file {ymlFile}. Please check file existance and its data format. Abort")
        sys.exit(-2)
    # Save  all details of the configuration and the version of the software used:

    # Determine start/end times
    if args.start is None :
        try:            
            start=Timestamp(ymlContents['run']['time']['start'])
        except:
            try:
                start=Timestamp(ymlContents['observations']['start'])
            except:
                try:
                    start=Timestamp(ymlContents['time']['start'])    # should be a string like start: '2018-01-01 00:00:00'
                except:
                    logger.error(f'missing key run.time.start in the user provided yaml file {ymlFile}.')
                logger.error(f'No valid start time found in the keys run.time.start nor observations.start nor time.start of your yml file {ymlFile}. Please fix or use the commandline option --start.')
    else:
        start= Timestamp(args.start)
    sStart= start.strftime('%Y-%m-%d')+' 00:00:00'
    setKeyVal_Nested_CreateIfNecessary(ymlContents, ['run', 'time',  'start'],   value= sStart, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, ['observations',  'start'],   value='${run.time.start}', bNewValue=True)
    if args.end is None :
        try:            
            end=Timestamp(ymlContents['run']['time']['end'])
        except:
            try:
                end=Timestamp(ymlContents['observations']['end'])
            except:
                try:
                    end=Timestamp(ymlContents['time']['end'])    # should be a string like start: '2018-01-01 00:00:00'
                except:
                    logger.error(f'No valid end time found in the keys run.time.end nor observations.end nor time.end of your yml file {ymlFile}. Please fix or use the commandline option --end.')
    else:
        end= Timestamp(args.end)
    sEnd= end.strftime('%Y-%m-%d')+' 23:59:59'
    #ymlContents['observations']['end'] = sEnd+'%Y-%m-%d 23:59:59Z'
    #ymlContents['run']['time']['end'] = sEnd+'%Y-%m-%d 23:59:59Z'
    try:
        timeStep=ymlContents['run']['time']['timestep']
    except:
        try:
            timeStep=ymlContents['run']['timestep']
            #This is the part of the code which filters out the undesired keys
            #ymlContents = filter(lambda x: x['name']!='temp_key2', ymlContents) 
        except:
            logger.warning(f'Key run.time.timestep not found in your ymlFile {ymlFile}. Assuming it is 1h. This may be updated when reading observational or emission data.')
            timeStep='1h'
    setKeyVal_Nested_CreateIfNecessary(ymlContents, ['run', 'time',  'end'],   value= str(sEnd), bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, ['observations',  'end'],   value= '${run.time.end}', bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, ['run', 'time',  'timestep'],   value= str(timeStep), bNewValue=True)
    if (args.serial):
        setKeyVal_Nested_CreateIfNecessary(ymlContents, ['model', 'options',  'serial'],   value=True, bNewValue=True)
    myCom=""
    # Get the local git hash so we have some clue of what version of LUMIA we may be using...
    localRepo='UNKNOWN    '
    sLocalGitRepos='UNKNOWN    '
    branch='UNKNOWN    '
    repoUrl='UNKNOWN    '
    myCom='UNKNOWN    '
    scriptName=sys.argv[0]
    script_directory = os.path.dirname(os.path.abspath(scriptName))
    scriptTail=scriptName[-6:]
    if('.ipynb' in scriptTail):
        logger.info('Local git information is not available from this python notebook. This is not an issue.')
    else: 
        try:
            import git
            # https://github.com/lumia-dev/lumia/commit/6be5dd54aa5a16b136c2c1e2685fc8abf2beb404
            if('LumiaGUI' in parentScript):
                # The correct .git info is found in the LumiaDA root where run.py lives, 2 directories up from here
                lumiaGUIdir=pathlib.Path(script_directory)
                oneLevelUp=lumiaGUIdir.parent
                lumiaDA_directory=oneLevelUp.parent
                try:
                    localRepo = git.Repo(lumiaDA_directory, search_parent_directories=True)
                except:
                    localRepo = git.Repo(script_directory, search_parent_directories=True)
            else:        
                localRepo = git.Repo(script_directory, search_parent_directories=True)
            logger.debug(f'localRepo={localRepo}')
            try:
                sLocalGitRepos=localRepo.working_tree_dir # /home/arndt/dev/lumia/lumiaDA/lumia
                logger.debug(f'Found localRepo.working_tree_dir info at : {sLocalGitRepos}')
            except:
                logger.debug('Failed to find localRepo.working_tree_dir info')
            try:
                branch=localRepo.head.ref # repo.head.ref=LumiaDA
                print(f'Local git info suggests that the branch name is : {branch}')
            except:
                logger.debug('Failed to find localRepo.working_tree_dir info')
            try:
                repoUrl=localRepo.remotes.origin.url  # git@github.com:lumia-dev/lumia.git
                logger.debug(f'Local git info suggests that the remote github url is : {repoUrl}')
            # repo.head.commit=6be5dd54aa5a16b136c2c1e2685fc8abf2beb404
            except:
                logger.debug('Failed to find localRepo.remotes.origin.url info')
            try:
                myCom=str(localRepo.head.commit)
                logger.debug(f'Local git info suggests that the latest commit is : {myCom}')
                #myComB=localRepo.head.commit(branch)
            except:
                logger.debug('Failed to find localRepo.head.commit info')
            remoteCommitUrl=repoUrl[:-4]+'/commit/'+str(localRepo.head.commit)
            logger.debug(f'Which you should also be able to get from : {remoteCommitUrl}')
            # https://github.com/lumia-dev/lumia/commit/6be5dd54aa5a16b136c2c1e2685fc8abf2beb404
        except:
            remoteCommitUrl='UNKNOWN    '
            logger.info('Cannot find information about the local git repository. \nGit information logged in the log files of this run relies on what was written into this source file by the programmers alone.')
    
    if(LATESTGITCOMMIT_LumiaDA not in myCom):
        if('UNKNOWN' in myCom):
            logger.info(f"\nWarning: Cannot verify whether the present version of lumiaGUI with git commit hash \nLATESTGITCOMMIT_LumiaDA ({LATESTGITCOMMIT_LumiaDA}) taken from said variable at the top of this \nlumia.GUI.housekeeping.py file is actually the latest version or not due to a missing local .git info tree.")
        else:                
            logger.error(f"\nError: There is a mismatch between the current local or remote git commit hash ({myCom}) and \nthe LATESTGITCOMMIT_LumiaDA ({LATESTGITCOMMIT_LumiaDA}) variable at the top of this lumia.GUI.housekeeping.py file. \nPlease check if there is a newer version on github or whether you forgot to push your latest local commit to the remote github.\nPlease resolve the conflict before proceeding.")
        #sys.exit(-5)

    wrongOrMissingVersion=False
    nVers=0
    nSubVers=0
    try:
        print(ymlContents['thisConfigFile']['dataformat']['version'])
        nVers=int(ymlContents['thisConfigFile']['dataformat']['version'])
    except:
        wrongOrMissingVersion=True
    if not (nVers==nThisConfigFileVersion):
        wrongOrMissingVersion=True
    try:
        nSubVers=int(ymlContents[ 'thisConfigFile']['dataformat']['subversion'])
    except:
        wrongOrMissingVersion=True
    if (nSubVers<1)or(nSubVers>nThisConfigFileSubVersion):
        wrongOrMissingVersion=True
    if(wrongOrMissingVersion):    
        logger.error(f'Wrong format of input Lumia config yml file. Your configuration file needs to be of major version=={nThisConfigFileVersion} and sub-version>0.')
        sys.exit(-3)
    # Document what kind of system the run was carried out on
    try:        
        sUsername=os.getlogin()  # The user's login name - works only if run from terminal
    except:
        try:
            sUsername=getpass.getuser()
        except:
            sUsername="UNKNOWN" 
    logger.info(f'lumiaGUI is run by user {sUsername}')
    # sysName=platform.system() # Linux
    #sysReleaseVersion=platform.release()  # 5.15.0-89-generic #99-Ubuntu SMP Mon Oct 30 20:42:41 UTC 2023
    myPlatformCore=platform.platform()  # Linux-5.15.0-89-generic-x86_64-with-glibc2.35
    myPlatformFlavour=platform.version() #99-Ubuntu SMP Mon Oct 30 20:42:41 UTC 2023
    myPlatformInfo=platform.freedesktop_os_release() # {'NAME': 'Debian GNU/Linux', 'ID': 'debian', 'PRETTY_NAME': 'Debian GNU/Linux 12 (bookworm)',
    #  'VERSION_ID': '12',  'VERSION': '12 (bookworm)', 'VERSION_CODENAME': 'bookworm', 'HOME_URL': 'https://www.debian.org/', 'SUPPORT_URL': 
    # 'https://www.debian.org/support', 'BUG_REPORT_URL': 'https://bugs.debian.org/'}
    myPlatformName=myPlatformInfo['NAME']  # Debian GNU/Linux
    myPlatformVersion=myPlatformInfo['VERSION_ID'] # 12
    myPlatformPrettyName=myPlatformInfo['PRETTY_NAME']  # Debian GNU/Linux 12 (bookworm)
    myPlatformArchitecture=platform.machine() # x86_64
    if('#' in myPlatformFlavour[:1]): # A literal 'hash' in this string can cause unnecessary problems in the yaml file
        myPlatformFlavour=myPlatformFlavour[1:] # as it might be interpreted as a comment. Best be ridd of it
    # All output is written into  subdirectories named after the run.thisRun.uniqueIdentifierDateTime key
    # Create these subdirectories. This also ensures early on that we can write to the intended locations
    try:
        sOutpDir=ymlContents['run']['paths']['output']
    except:
        sOutpDir="./output"
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'paths',  'output' ],   value=sOutpDir, bNewValue=True)
    if(len(sOutpDir)>0):
        sCmd=("mkdir -p "+sOutpDir)
    try:
        os.system(sCmd)
    except:
        sys.exit(f'Abort. Failed to create user-requested output directory {sOutpDir}. Please check the key run.paths.output in your {ymlFile} file as well as your write permissions.')
    sOutputPrfx=parentScript+'-'+sNow+os.path.sep+parentScript+'-'+sNow+'-'
    sTmpPrfx=sOutputPrfx # same structure below the Temp and Output directories
    if ((len(sOutpDir)>0) and (sOutpDir[-1]!=os.path.sep)):
        sOutpDir=sOutpDir+os.path.sep
    sCmd=("mkdir -p "+sOutpDir+parentScript+'-'+sNow)
    try:
        os.system(sCmd)
    except:
        sys.exit(f'Abort. Failed to create user-requested output sub-directory {sOutpDir}LumiaDA-{sNow}. Please check the key run.paths.output in your {ymlFile} file as well as your write permissions.')
    sOutputPrfx=sOutpDir+sOutputPrfx
    try:
        sTmpDir=ymlContents['run']['paths']['temp']
    except:
        sTmpDir="./tmp"
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'paths',  'temp' ],   value=sTmpDir, bNewValue=True)
    if(len(sOutpDir)>0):
        sCmd=("mkdir -p "+sTmpDir)
    try:
        if not('LumiaGUI' in parentScript): # lumiaGUI only writes to the outputDir not the sTmpDir directory
            os.system(sCmd)
    except:
        sys.exit(f'Abort. Failed to create user-requested temp directory {sTmpDir}. Please check the key run.paths.output in your {ymlFile} file as well as your write permissions.')
    if ((len(sTmpDir)>0) and (sTmpDir[-1]!=os.path.sep)):
        sTmpDir=sTmpDir+os.path.sep
    sCmd=("mkdir -p "+sTmpDir+parentScript+'-'+sNow)
    try:
        os.system(sCmd)
    except:
        sys.exit(f'Abort. Failed to create user-requested temp sub-directory {sTmpDir}LumiaDA-{sNow}. Please check the key run.paths.output in your {ymlFile} file as well as your write permissions.')
    sTmpPrfx=sTmpDir+sTmpPrfx

    tracer=getTracer(ymlContents['run']['tracers'])
    # Before setting a new output path, grab the name of the latest (existing) dicoveredObsData if it exists
    oldDiscoveredObservations='DiscoveredObservations-'+tracer+'.csv'
    try:
        oldDiscoveredObservations=ymlContents['observations'][tracer]['file']['dicoveredObsData']
    except:
        try:
            sOldSlctObs=ymlContents['observations'][tracer]['file']['selectedObsData']
            oldDiscoveredObservations=sOldSlctObs[:-24]+oldDiscoveredObservations
        except:
            pass
    
   # Make sure these keys exist. Do not overwrite existing values.
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [  'path',  'data'],   value='./data', bNewValue=False) # for runflex ./data/meteo/ea.eurocom025x025/
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [  'run',  'paths',  'temp'],   value='/temp', bNewValue=False)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'paths',  'footprints'],   value='/footprints', bNewValue=False)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, ['correlation',  'inputdir'],   value='/data/corr', bNewValue=False )
    # Run-dependent paths
    #setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'paths',  'tag'],   value=os.path.join('/output', args.tag), bNewValue=False)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'var4d',  'communication',  'file'],   value='congrad.nc', bNewValue=False)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'emissions',  '*',  'archive'],   value='rclone:lumia:fluxes/nc/', bNewValue=False)
    #setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'emissions',  '*',  'path'],   value= '/data/fluxes/nc', bNewValue=False)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'model',  'transport',  'exec'],   value='/lumia/transport/multitracer.py', bNewValue=False)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'transport',  'output'],   value= 'T', bNewValue=False)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'transport',  'steps'],   value='forward', bNewValue=False)
    
    # if there are local files involved, calculate their sha256 checksum - The carbon portal PID is actually also the sha256 of the datafile concerned
    if 'LOCAL' in ymlContents['observations'][tracer]['file']['location']:
        myfile=ymlContents['observations'][tracer]['file']['path']
        sha256Value=caclulateSha256Filehash(myfile)
    else:
        sha256Value='NotApplicable'
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'observations',  tracer,  'file',  'sha256Value'],   value=sha256Value, bNewValue=True)
    for cat in {'biosphere', 'fossil', 'ocean'}:
        if 'LOCAL' in ymlContents['emissions'][tracer]['location'][cat]:
            origin = ymlContents['emissions'][tracer]['categories'][cat]['origin']
            #regionGrid=ymlContents['emissions'][tracer]['region'] 
            #sRegion="lon0=%.3f, lon1=%.3f, lat0=%.3f, lat1=%.3f, dlon=%.3f, dlat=%.3f, nlon=%d, nlat=%d"%(regionGrid.lon0, regionGrid.lon1,  regionGrid.lat0,  regionGrid.lat1,  regionGrid.dlon,  regionGrid.dlat,  regionGrid.nlon,  regionGrid.nlat)
            myPath2FluxData1=ymlContents['emissions'][tracer]['path']  
            myPath2FluxData1='/home/arndt/nateko/data/icos/DICE/fluxes/nc/'
            myPath2FluxData3=ymlContents['emissions'][tracer]['interval'] 
            myPath2FluxData3='1h/'
            myPath2FluxData2='eurocom025x025'
            try:
                myPath2FluxData2=ymlContents['emissions'][tracer]['regionName']
                myPath2FluxData2='eurocom025x025'
            except:
                logger.warning(f'Warning: No key emissions.{tracer}.regionName found in user defined resource file (used in pathnames). I shall try to guess it...')
                mygrid=ymlContents['emissions'][tracer]['region'] 
                if((250==int(mygrid.dlat*1000)) and (250==int(mygrid.dlon*1000)) and (abs((0.5*(mygrid.lat0+mygrid.lat1))-53)<mygrid.dlat)and (abs((0.5*(mygrid.lon0+mygrid.lon1))-10)<mygrid.dlon)):
                    myPath2FluxData2='eurocom025x025' # It is highly likely that the region is centered in Europe and has a lat/lon grid of a quarter degree
                else:
                    logger.error(f'Abort. My guess of eurocom025x025 was not a very good guess. Please provide a emissions.{tracer}.regionName key in your yml configuration file and try again.', flush=True)
                    sys.exit(1)
            if ((len(myPath2FluxData1)>0) and (myPath2FluxData1[-1]!=os.path.sep)):
                myPath2FluxData1=myPath2FluxData1+os.path.sep
            myPath2FluxData=myPath2FluxData1+myPath2FluxData2+os.path.sep+myPath2FluxData3
            if (os.path.sep!=myPath2FluxData[-1]):     # Does the path end in a directory separator (forward or back-slash depending on OS)?
                myPath2FluxData=myPath2FluxData+os.path.sep
            # TODO: sha256 and PIDs are not calculated proper for the apriori emissions data - needs fixing
            if((origin is None)or(origin == '') or ('None' == origin)):
                localEmisFile = os.path.join(myPath2FluxData, ymlContents['emissions'][tracer]['prefix']  )
            else:
                localEmisFile = os.path.join(myPath2FluxData, ymlContents['emissions'][tracer]['prefix'] + origin + '.2018.nc')
            logger.debug(f"localEmisFile={localEmisFile} ")
            setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'emissions',  tracer,  'categories',  cat, 'localFile'],   value=localEmisFile, bNewValue=True)
            sha256Value=caclulateSha256Filehash(localEmisFile)
        else:
            sha256Value='NotApplicable'
            try:
                ymlContents[ 'emissions'][ tracer]['categories'][cat]['localFile']='NotApplicable'
            except:
                pass  # if the key does not exists, then that's fine too
                
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'emissions',  tracer,  'categories',  cat, 'sha256Value'],   value=sha256Value, bNewValue=True)
        
    myMachine=platform.node()
    pyVers= 'Python 3.10.10' # sys.version()
    pyVersion='Python environment version is '+str(pyVers)
    #pyVersion=pyVersion.strip('\n')
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'hostName' ],   value=myMachine, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'platformCore' ],   value=myPlatformCore, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'platformFlavour' ],   value=myPlatformFlavour, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'platformArchitecture' ],   value=myPlatformArchitecture, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'platformName' ],   value=myPlatformName, bNewValue=True)    
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'platformPrettyName' ],   value=myPlatformPrettyName, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'platformVersion' ],   value=myPlatformVersion, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'PythonVersion' ],   value=pyVersion, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'uniqueIdentifierDateTime'],   value=sNow, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'uniqueOutputPrefix'],   value=sOutputPrfx, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'uniqueTmpPrefix'],   value=sTmpPrfx, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'username'],   value=sUsername, bNewValue=True)
    # Lumia version
    #setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'branch'],   value='gitkraken://repolink/778bf0763fae9fad55be85dde4b42613835a3528/branch/LumiaDA?url=git%40github.com%3Alumia-dev%2Flumia.git',  bNewValue=True)
    #setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'commit'],   value='gitkraken://repolink/778bf0763fae9fad55be85dde4b42613835a3528/commit/5e5e9777a227631d6ceeba4fd8cff9b241c55de1?url=git%40github.com%3Alumia-dev%2Flumia.git',  bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'git',  'branch'],   value=parentScript, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'git',  'url'],   value='git@github.com:lumia-dev/lumia.git', bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'git',  'commit'],   value=LATESTGITCOMMIT_LumiaDA, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'git',  'location'],   value='git@github.com:lumia-dev/lumia/commit/'+LATESTGITCOMMIT_LumiaDA, bNewValue=True)
    # runflex
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [  'softwareUsed',  'runflex',  'branch'],   value='gitkraken://repolink/b9411fbf7aeeb54d7bb34331a98e2cc0b6db9d5f/branch/v2?url=https%3A%2F%2Fgithub.com%2Flumia-dev%2Frunflex.git',  bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'runflex',  'commit'],   value='gitkraken://repolink/b9411fbf7aeeb54d7bb34331a98e2cc0b6db9d5f/commit/aad612b36a247046120bda30c8837acb5dec4f26?url=https%3A%2F%2Fgithub.com%2Flumia-dev%2Frunflex.git',  bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'runflex', 'git', 'branch'],   value='v2', bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'runflex', 'git', 'url'],   value='git@github.com:lumia-dev/runflex.git', bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'runflex', 'git', 'commit'],   value=LATESTGITCOMMIT_Runflex, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'runflex', 'git', 'location'],   value='git@github.com:lumia-dev/runflex/commit/'+LATESTGITCOMMIT_Runflex, bNewValue=True)

    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'thisConfigFile',  'dataformat', 'version'],   value=nThisConfigFileVersion, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'thisConfigFile',  'dataformat', 'subversion'],   value=nThisConfigFileSubVersion, bNewValue=True)
    # If LumiaGUI was run beforehand, than input files are known and specified in the config file and ['observations'][tracer]['file']['discoverData'] is set to False
    # else, LumiaDA has to go and hunt for ObsData on the carbon portal the old fashioned way ('discoverData'==True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, ['observations',  'file', tracer, 'discoverData'],   value=True, bNewValue=False) # only create if not exist.
    setKeyVal_Nested_CreateIfNecessary(ymlContents, ['observations', 'file', tracer, 'selectedObsData'],   value='None', bNewValue=False)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, ['observations', 'file', tracer,'selectedPIDs'],   value='None', bNewValue=False)
    
    # We also need to copy the 2 files from LumiaGUI that give us the list of input PIDs, so they have the same unique identifier
    #  as this run and end up in the correct folder.
    bDiscoverData=ymlContents['observations'][tracer]['file']['discoverData']
    stopExecution=False
    bErr=False
    try:
        selectedObsData=ymlContents['observations'][tracer]['file']['selectedObsData']
    except:
        if('LumiaGUI' in parentScript):
            pass
        else:
            logger.error(f'Key observations.{tracer}.file.selectedObsData not found in yml config file {ymlFile}. Please run LumiaGUI.py with your yml config file before calling LumiaDA in order to create that file.')
            bErr=True
    if((bErr==False) and ((selectedObsData is None) or (len(selectedObsData)<3))):
        if 'CARBONPORTAL' in ymlContents['observations'][tracer]['file']['location']:
            logger.warning(f'Key observations.{tracer}.file.selectedObsData: No meaningful file name provided for this key in your yml config file {ymlFile}. Please run LumiaGUI.py with your yml config file before calling LumiaDA in order to create that file.')
            bErr=True
    if((bErr) and (bDiscoverData==False)):
        stopExecution=True
    else:
        if 'CARBONPORTAL' in ymlContents['observations'][tracer]['file']['location']:
            keepThis=f'selected-ObsData-{tracer}.csv'
            newFnameSelectedObsData=sOutputPrfx+keepThis
            sCmd=f'cp {selectedObsData} {newFnameSelectedObsData}'
            runSysCmd(sCmd)
            setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'observations',  tracer, 'file', 'selectedObsData' ],   value=newFnameSelectedObsData, bNewValue=True)

    if 'CARBONPORTAL' in ymlContents['observations'][tracer]['file']['location']:
        try:
            selectedPIDs=ymlContents['observations'][tracer]['file']['selectedPIDs']
        except:
            if('LumiaGUI' in parentScript):
                pass
            else:
                logger.error(f'Key observations.{tracer}.file.selectedPIDs not found in yml config file {ymlFile}. Please run LumiaGUI.py with your yml config file before calling LumiaDA in order to create that file.')
                bErr=True
        if((bErr==False) and ((selectedPIDs is None) or (len(selectedPIDs)<3))):
            logger.warning(f'Key observations.{tracer}.file.selectedPIDs: No meaningful file name provided for this key in your yml config file {ymlFile}. Please run LumiaGUI.py with your yml config file before calling LumiaDA in order to create that file.')
            bErr=True
    if((bErr) and (bDiscoverData==False)):
        stopExecution=True
    else:
        if 'CARBONPORTAL' in ymlContents['observations'][tracer]['file']['location']:
            # the value is something like ./output/LumiaDA-2024-01-08T10_00-selected-ObsData-co2.csv.   Strip the Lumia-2024-01-08T10_00- part from it
            keepThis=f'selected-PIDs-{tracer}.csv'
            newFnameSelectedPIDs=sOutputPrfx+keepThis
            sCmd=f'cp {selectedPIDs} {newFnameSelectedPIDs}'
            runSysCmd(sCmd)
            setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'observations',  tracer, 'file', 'selectedPIDs' ],   value=newFnameSelectedPIDs, bNewValue=True)

    if(stopExecution):
        sys.exit(-1) # you cannot call LumiaDA telling it to use a list of PIDs that you have not provided in the yml file.
    
    # Make explicitly stated communication and temporal files use the unique identifier for file names and directory locations:
    #congrad:
    #  communication_file: ${run.paths.temp}/congrad.nc
    # var4d:
    #  file: /home/arndt/nateko/data/icos/DICE/tmp/congrad.nc
    # These 2 keys always point to the same file, only that the var4d one is calculated later using the ${run.paths.temp} placeholder's value
    congradFile=sTmpPrfx+'congrad.nc'
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'var4d', 'communication', 'file'],   value=congradFile, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'congrad', 'communication_file'],   value=congradFile, bNewValue=True)

    
    # Now update the configuration file writing everything out and hand control back to the main program....
    try:
        with open(ymlFile, 'w') as outFile:  # we are updating/replacing the configuration file
            yaml.dump(ymlContents, outFile)    
    except:
        logger.error(f'failed to update the Lumia configuration file. Is the file {ymlFile} or the corresponding file system write protectd?')
        sys.exit(-19)
    sNewYmlFileName=f'{sOutputPrfx}config.yml' # v{nVers}.{nSubVers}-{tracer}
    sCmd=f'cp {ymlFile} {sNewYmlFileName}'
    rValue=0
    try:
        rValue=os.system(sCmd)
    except:
        sys.exit(f'Abort. os.popen({sCmd}) returned an error. Future reproducibility of this Lumia run is compromised, because I cannot document the Lumia configuration you are using.')
        sys.exit(-7)
    if(rValue!=0):
        sys.exit(f'Abort. os.popen({sCmd}) returned an error. Future reproducibility of this Lumia run is compromised, because I cannot document the Lumia configuration you are using.')
        sys.exit(-7)
    sCmd=f'cp {ymlFile}.bac {sNewYmlFileName}.bac'  # copy also the .bac file in case the user pulls out and we want to revert to the original ymlFIle
    rValue=os.system(sCmd)

    # Document the Python environment
    sCmd=(f'pip list > {sOutputPrfx}python-environment-pipLst.txt')
    rValue=0
    try:
        rValue=os.system(sCmd)
    except:
        sys.exit(f'Abort. os.popen({sCmd}) returned an error. Future reproducibility of this Lumia run is compromised, because I cannot document the Python environment you are using.')
        sys.exit(-7)
    if(rValue!=0):
        sys.exit(f'Abort. os.popen({sCmd}) returned an error. Future reproducibility of this Lumia run is compromised, because I cannot document the Python environment you are using.')
        sys.exit(-7)
    
    return(sNewYmlFileName, oldDiscoveredObservations)



