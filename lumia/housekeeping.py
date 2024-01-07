#!/usr/bin/env python3

LATESTGITCOMMIT_LumiaDA='c4a52b50b290b54ffcf34bda9b325459ffc30de3' #'c7b8a69cf88c0b44a41d632f57c4cdcdd6d6efe9' # 
LATESTGITCOMMIT_Runflex='aad612b36a247046120bda30c8837acb5dec4f26'

import os
import sys
import platform
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


from datetime import datetime
current_date = datetime.now()
sNow=current_date.isoformat("T","seconds") # sNow is the time stamp for all log files of a particular run

def documentThisRun(ymlFile, args):
    # Now read the yaml configuration file - whether altered by the GUI or not
    try:
        rcf=rc(ymlFile)
    except:
        logger.error(f"Unable to read user provided configuration file {ymlFile}. Please check file existance and its data format. Abort")
        sys.exit(-2)
    # Save  all details of the configuration and the version of the software used:
    sCmd=("mkdir -p "+rcf['run']['paths']['output'])
    try:
        os.system(sCmd)
    except:
        print(".")
    #if len(rcf['run']['paths']['output'])<1:
    #    sLogCfgFile="./Lumia-runlog-"+sNow[:-3]+"config.yml"
    #else:
    #    sLogCfgFile=rcf['run']['paths']['output']+"/Lumia-runlog-"+sNow+"-config.yml"

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

    myMachine=platform.node()
    pyVers= 'Python 3.10.10' # sys.version()
    pyVersion='Python environment version is '+str(pyVers)
    #pyVersion=pyVersion.strip('\n')
    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'uniqueIdentifierDateTime'],   value=sNow, bNewValue=True)
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
    
    
    with open(ymlFile, 'w') as outFile:  # we are updating/replacing the configuration file
        yaml.dump(ymlContents, outFile)
    
    # prepare the call of LumiaGUI
    # TODO: this shall become obsolete. LumiaGUI is to be called stand-alone before running LumiaDA
    sCmd ='python3 '+script_directory+'/lumia/GUI/lumiaGUI.py '
    for entry in sys.argv[1:]:
        if (len(entry)>0):
            sCmd+=' '+entry
    sCmd+=' --sNow='+sNow
    return(sCmd)



