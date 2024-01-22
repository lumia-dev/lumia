#!/usr/bin/env python3

LATESTGITCOMMIT_LumiaDA='e8e5d4a35f6ecedd7697032b67428a08bc906d82' 
LATESTGITCOMMIT_Runflex='aad612b36a247046120bda30c8837acb5dec4f26'

import os
import sys
import platform
import re
import git
import yaml
from datetime import datetime
from pandas import to_datetime,  Timestamp
# from rctools import RcFile as rc
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

script_directory = os.path.dirname(os.path.abspath(sys.argv[0]))   

def runSysCmd(sCmd,  ignoreError=False):
    try:
        os.system(sCmd)
    except:
        if(ignoreError==False):
            sTxt=f"Fatal Error: Failed to execute system command >>{sCmd}<<. Please check your write permissions and possibly disk space etc."
            logger.warning(sTxt)
        return False
    return True



current_date = datetime.now()
sNow=current_date.isoformat("T","seconds") # sNow is the time stamp for all log files of a particular run
# colons from the time are not without problems in directory and file names. Better to play it safe and replace them with uynderscores
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
            #ymlContents['observations']['start']=ymlContents['run']['time']['start']
        except:
            try:
                start=Timestamp(ymlContents['observations']['start'])
                #ymlContents['run']['time']['start']=ymlContents['observations']['start']
            except:
                logger.error(f'No valid start time found in the keys run.time.start nor observations.start of your yml file {ymlFile}. Please fix or use the commandline option --start.')
    else:
        start= Timestamp(args.start)
    sStart= start.strftime('%Y,%m,%d')+' 00:00:00.000Z'
    #ymlContents['run']['time']['start']= sStart+' 00:00:00.000Z'
    #ymlContents['observations']['start']= sStart+' 00:00:00.000Z'
    setKeyVal_Nested_CreateIfNecessary(ymlContents, ['run', 'time',  'start'],   value= str(sStart), bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, ['observations',  'start'],   value= str(sStart), bNewValue=True)
        
    if args.end is None :
        try:            
            end=Timestamp(ymlContents['run']['time']['end'])
            # ymlContents['observations']['end']=ymlContents['run']['time']['end']
        except:
            try:
                end=Timestamp(ymlContents['observations']['end'])
                #ymlContents['run']['time']['end']=ymlContents['observations']['end']
            except:
                logger.error(f'No valid end time found in the keys run.time.end nor observations.end of your yml file {ymlFile}. Please fix or use the commandline option --end.')
    else:
        end= Timestamp(args.end)
    sEnd= end.strftime('%Y,%m,%d')+'%Y-%m-%d 23:59:59Z'
    #ymlContents['observations']['end'] = sEnd+'%Y-%m-%d 23:59:59Z'
    #ymlContents['run']['time']['end'] = sEnd+'%Y-%m-%d 23:59:59Z'
    setKeyVal_Nested_CreateIfNecessary(ymlContents, ['run', 'time',  'end'],   value= str(sEnd), bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, ['observations',  'end'],   value= str(sEnd), bNewValue=True)
    
 
    myCom=""
    # Get the local git hash so we have some clue of what version of LUMIA we may be using...
    try:
        # https://github.com/lumia-dev/lumia/commit/6be5dd54aa5a16b136c2c1e2685fc8abf2beb404
        localRepo = git.Repo(script_directory, search_parent_directories=True)
        sLocalGitRepos=localRepo.working_tree_dir # /home/arndt/dev/lumia/lumiaDA/lumia
        print(f'Found local git repository info at : {sLocalGitRepos}')
        branch=localRepo.head.ref # repo.head.ref=LumiaDA
        print(f'Local git info suggests that the branch name is : {branch}')
        repoUrl=localRepo.remotes.origin.url  # git@github.com:lumia-dev/lumia.git
        print(f'Local git info suggests that the remote github url is : {repoUrl}')
        # repo.head.commit=6be5dd54aa5a16b136c2c1e2685fc8abf2beb404
        myCom=str(localRepo.head.commit)
        #myComB=localRepo.head.commit(branch)
        remoteCommitUrl=repoUrl[:-4]+'/commit/'+str(localRepo.head.commit)
        print(f'Local git info suggests that the latest commit is : {myCom}')
        # https://github.com/lumia-dev/lumia/commit/6be5dd54aa5a16b136c2c1e2685fc8abf2beb404
        print(f'Which you should also be able to get from : {remoteCommitUrl}')
    except:
        logger.info('Cannot find information about the local git repository. \nGit information logged in the log files of this run relies on what was written into this source file by the programmers alone.')
    
    if(LATESTGITCOMMIT_LumiaDA not in myCom):
        logger.error(f"Error: There is a mismatch between the current \nlocal git commit hash ({myCom}) and \nthe LATESTGITCOMMIT_LumiaDA ({LATESTGITCOMMIT_LumiaDA}) variable at the top of this run.py file. \nPlease resolve the conflict before proceeding.")
        #sys.exit(-5)

    try:
        with open(ymlFile, 'r') as file:
            ymlContents = yaml.safe_load(file)
    except:
        logger.error(f"Abort! Unable to read yaml configuration file {ymlFile} - failed to read its contents with yaml.safe_load()")
        sys.exit(1)

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
    sUsername=os.getlogin()  # The user's login name
    # sysName=platform.system() # Linux
    #sysReleaseVersion=platform.release()  # 5.15.0-89-generic #99-Ubuntu SMP Mon Oct 30 20:42:41 UTC 2023
    myPlatformCore=platform.platform()  # Linux-5.15.0-89-generic-x86_64-with-glibc2.35
    myPlatformFlavour=platform.version() #99-Ubuntu SMP Mon Oct 30 20:42:41 UTC 2023
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

    # Find out the first (only) tracer being used
    tracer='co2'
    try:
        if (isinstance(ymlContents['run']['tracers'], str)):
            tracer=ymlContents['run']['tracers']
        else:
            trac=ymlContents['run']['tracers']
            tracer=trac[0]
    except:
        tracer='co2'
    
    myMachine=platform.node()
    pyVers= 'Python 3.10.10' # sys.version()
    pyVersion='Python environment version is '+str(pyVers)
    #pyVersion=pyVersion.strip('\n')
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'uniqueIdentifierDateTime'],   value=sNow, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'uniqueOutputPrefix'],   value=sOutputPrfx, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'uniqueTmpPrefix'],   value=sTmpPrfx, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'username'],   value=sUsername, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'platformCore' ],   value=myPlatformCore, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'platformFlavour' ],   value=myPlatformFlavour[2:-1], bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'PythonVersion' ],   value=pyVersion, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'hostName' ],   value=myMachine, bNewValue=True)
    # Lumia version
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'git',  'branch'],   value=parentScript, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'git',  'url'],   value='git@github.com:lumia-dev/lumia.git', bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'git',  'commit'],   value=LATESTGITCOMMIT_LumiaDA, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'git',  'location'],   value='git@github.com:lumia-dev/lumia/commit/'+LATESTGITCOMMIT_LumiaDA, bNewValue=True)
    # runflex
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'runflex',  'git',  'branch'],   value='v2', bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'runflex',  'git',  'url'],   value='git@github.com:lumia-dev/runflex.git', bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'runflex',  'git',  'commit'],   value=LATESTGITCOMMIT_Runflex, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'runflex',  'git',  'location'],   value='git@github.com:lumia-dev/runflex/commit/'+LATESTGITCOMMIT_Runflex, bNewValue=True)

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
        logger.warning(f'Key observations.{tracer}.file.selectedObsData: No meaningful file name provided for this key in your yml config file {ymlFile}. Please run LumiaGUI.py with your yml config file before calling LumiaDA in order to create that file.')
        bErr=True
    if((bErr) and (bDiscoverData==False)):
        stopExecution=True
    else:
        keepThis=f'selected-ObsData-{tracer}.csv'
        newFnameSelectedObsData=sOutputPrfx+keepThis
        sCmd=f'cp {selectedObsData} {newFnameSelectedObsData}'
        runSysCmd(sCmd)
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'observations',  tracer, 'file', 'selectedObsData' ],   value=newFnameSelectedObsData, bNewValue=True)

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
    sNewYmlFileName=f'{sOutputPrfx}v{nVers}.{nSubVers}-{tracer}-config.yml'
    sCmd=f'cp {ymlFile} {sNewYmlFileName}'
    runSysCmd(sCmd)
    return(sNewYmlFileName)



