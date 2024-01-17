#!/usr/bin/env python3

LATESTGITCOMMIT_LumiaDA='bc5734ed71b77746d625bcf50b7ad994cd4ad0e5' #'c7b8a69cf88c0b44a41d632f57c4cdcdd6d6efe9' # 
LATESTGITCOMMIT_Runflex='aad612b36a247046120bda30c8837acb5dec4f26'

import os
import sys
import platform
import re
import git
# from pandas import Timestamp
import yaml
#import lumia
from rctools import RcFile as rc
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
            # self.CancelAndQuit(sTxt)


from datetime import datetime
current_date = datetime.now()
sNow=current_date.isoformat("T","seconds") # sNow is the time stamp for all log files of a particular run
# colons from the time are not without problems in directory and file names. Better to play it safe and replace them with uynderscores
sNow=re.sub(':', '_', sNow)
sNow=sNow[:-3] # minutes is good enough....don't need seconds if a run takes hours...

def documentThisRun(ymlFile, args):
    # Now read the yaml configuration file - whether altered by the GUI or not
    try:
        rcf=rc(ymlFile)
    except:
        logger.error(f"Unable to read user provided configuration file {ymlFile}. Please check file existance and its data format. Abort")
        sys.exit(-2)
    # Save  all details of the configuration and the version of the software used:
 
    #if len(rcf['run']['paths']['output'])<1:
    #    sLogCfgFile="./Lumia-runlog-"+sNow[:-3]+"config.yml"
    #else:
    #    sLogCfgFile=rcf['run']['paths']['output']+"/Lumia-runlog-"+sNow+"-config.yml"
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

    # Document what kind of system the run was carried out on
    sUsername=os.getlogin()  # The user's login name
    # sysName=platform.system() # Linux
    #sysReleaseVersion=platform.release()  # 5.15.0-89-generic #99-Ubuntu SMP Mon Oct 30 20:42:41 UTC 2023
    myPlatformCore=platform.platform()  # Linux-5.15.0-89-generic-x86_64-with-glibc2.35
    myPlatformFlavour=platform.version() #99-Ubuntu SMP Mon Oct 30 20:42:41 UTC 2023
    # All output is written into  subdirectories named after the run.thisRun.uniqueIdentifierDateTime key
    # Create these subdirectories. This also ensures early on that we can write to the intended locations
    try:
        sOutpDir=rcf['run']['paths']['output']
    except:
        sOutpDir="./output"
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'paths',  'output' ],   value=sOutpDir, bNewValue=True)
    if(len(sOutpDir)>0):
        sCmd=("mkdir -p "+sOutpDir)
    try:
        os.system(sCmd)
    except:
        sys.exit(f'Abort. Failed to create user-requested output directory {sOutpDir}. Please check the key run.paths.output in your {ymlFile} file as well as your write permissions.')
    sOutputPrfx='LumiaDA-'+sNow+os.path.sep+'LumiaDA-'+sNow+'-'
    sTmpPrfx=sOutputPrfx # same structure below the Temp and Output directories
    if ((len(sOutpDir)>0) and (sOutpDir[-1]!=os.path.sep)):
        sOutpDir=sOutpDir+os.path.sep
    sCmd=("mkdir -p "+sOutpDir+'LumiaDA-'+sNow)
    try:
        os.system(sCmd)
    except:
        sys.exit(f'Abort. Failed to create user-requested output sub-directory {sOutpDir}LumiaDA-{sNow}. Please check the key run.paths.output in your {ymlFile} file as well as your write permissions.')
    sOutputPrfx=sOutpDir+sOutputPrfx
    try:
        sTmpDir=rcf['run']['paths']['temp']
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
    sCmd=("mkdir -p "+sTmpDir+'LumiaDA-'+sNow)
    try:
        os.system(sCmd)
    except:
        sys.exit(f'Abort. Failed to create user-requested temp sub-directory {sTmpDir}LumiaDA-{sNow}. Please check the key run.paths.output in your {ymlFile} file as well as your write permissions.')
    sTmpPrfx=sTmpDir+sTmpPrfx
    
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
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'git',  'branch'],   value='LumiaDA', bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'git',  'url'],   value='git@github.com:lumia-dev/lumia.git', bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'git',  'commit'],   value=LATESTGITCOMMIT_LumiaDA, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'git',  'location'],   value='git@github.com:lumia-dev/lumia/commit/'+LATESTGITCOMMIT_LumiaDA, bNewValue=True)
    # runflex
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'runflex',  'git',  'branch'],   value='v2', bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'runflex',  'git',  'url'],   value='git@github.com:lumia-dev/runflex.git', bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'runflex',  'git',  'commit'],   value=LATESTGITCOMMIT_Runflex, bNewValue=True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'runflex',  'git',  'location'],   value='git@github.com:lumia-dev/runflex/commit/'+LATESTGITCOMMIT_Runflex, bNewValue=True)

    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'thisConfigFile',  'dataformat', 'version'],   value=int(6), bNewValue=True)
    # If LumiaGUI was run beforehand, than input files are known and specified in the config file and ['observations'][tracer]['file']['discoverData'] is set to False
    # else, LumiaDA has to go and hunt for ObsData on the carbon portal the old fashioned way ('discoverData'==True)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, ['observations',  'file', 'discoverData'],   value=True, bNewValue=False)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, ['observations', 'file', 'selectedObsData'],   value='None', bNewValue=False)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, ['observations', 'file', 'selectedPIDs'],   value='None', bNewValue=False)
    
    # We also need to copy the 2 files from LumiaGUI that give us the list of input PIDs, so they have the same unique identifier
    #  as this run and end up in the correct folder.
    tracer='co2'
    try:
        if (isinstance(rcf['run']['tracers'], str)):
            tracer=rcf['run']['tracers']
        else:
            trac=rcf['run']['tracers']
            tracer=trac[0]
    except:
        tracer='co2'
    try:
        selectedObsData=rcf['observations'][tracer]['file']['selectedObsData']
    except:
        logger.error(f'Key observations.{tracer}.file.selectedObsData not found in yml config file {ymlFile}. Please run LumiaGUI.py with your yml config file before calling LumiaDA in order to create that file.')
    fname = os.path.basename(selectedObsData)
    if (len(fname)>23):
        fname=fname[23:]
    newFnameselectedObsData=sOutputPrfx+fname
    sCmd=f'cp {selectedObsData} {newFnameselectedObsData}'
    runSysCmd(sCmd)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'observations',  tracer, 'file', 'selectedObsData' ],   value=newFnameselectedObsData, bNewValue=True)

    try:
        selectedPIDs=rcf['observations'][tracer]['file']['selectedPIDs']
    except:
        logger.error(f'Key observations.{tracer}.file.selectedObsData not found in yml config file {ymlFile}. Please run LumiaGUI.py with your yml config file before calling LumiaDA in order to create that file.')
    # the value is something like ./output/Lumia-2024-01-08T10_00-selected-ObsData-co2.csv.   Strip the Lumia-2024-01-08T10_00- part from it
    fname = os.path.basename(selectedPIDs)
    if (len(fname)>23):
        fname=fname[23:]
    newFnameSelectedPIDs=sOutputPrfx+fname
    sCmd=f'cp {selectedPIDs} {newFnameSelectedPIDs}'
    runSysCmd(sCmd)
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'observations',  tracer, 'file', 'selectedPIDs' ],   value=newFnameSelectedPIDs, bNewValue=True)

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
    # prepare the call of LumiaGUI
    # TODO: this shall become obsolete. LumiaGUI is to be called stand-alone before running LumiaDA
    # sCmd ='python3 '+script_directory+'/lumia/GUI/lumiaGUI.py '
    # for entry in sys.argv[1:]:
    #     if (len(entry)>0):
    #         sCmd+=' '+entry
    # sCmd+=' --sNow='+sNow
    # return(sCmd)



