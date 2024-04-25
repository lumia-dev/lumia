#!/usr/bin/env python

import os
import sys
import argparse
from loguru import logger
#import subprocess
from glob import glob

os.system("")
class style():
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    RESET = '\033[0m'


def runSysCmd(sCmd,  ignoreError=False):
    try:
        os.system(sCmd)
    except:
        if(ignoreError==False):
            logger.error(f"Error: Failed to execute system command >>{sCmd}<<. Please check your write permissions and possibly disk space etc.")
        return False
    return True

@logger.catch(reraise=True)
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--startDir', '-s', default='./',  help='from where we start searching for directories to be reviewed for deletion, the parent folder of the ./output and ./tmp folders. Default is current working directory ./')
    p.add_argument('--outputDir', '-o', default='output',  help='Name of the Lumia output sub-folder. (use quotes to set to empty string)')
    p.add_argument('--tmpDir', '-t', default='tmp',  help='Name of the Lumia tmp sub-folder.')
    p.add_argument('--ignoreKeep', '-i', default=False, action='store_true',  help='If set, the keep flag is ignored and all folders are shown.')
    args, unknown = p.parse_known_args(sys.argv[1:])
    
    # Set the verbosity in the logger (loguru quirks ...)
    logger.remove()
    #logger.add(sys.stderr, level=args.verbosity)
    log_level = "DEBUG"
    log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS zz}</green> | <level>{level: <8}</level> | <yellow>Line {line: >4} ({file}):</yellow> <b>{message}</b>"
    logger.add(sys.stderr, level=log_level, format=log_format, colorize=True, backtrace=True, diagnose=True)
    myStartDir=args.startDir
    if not (myStartDir[-1]==os.path.sep):
        myStartDir=myStartDir+os.path.sep
    myOutputDir=args.outputDir
    if((myOutputDir is None) or (len(myOutputDir)<2)):
        myOutputDir=''
    myTmpDir=args.tmpDir
    if((myTmpDir is None) or (len(myTmpDir)<2)):
        myTmpDir=''
    sOutputPrfx=myStartDir+myOutputDir # 'output'
    sTmpPrfx=myStartDir+myTmpDir+os.path.sep  #tmp/'
    #[x[0] for x in os.walk(directory)]

    print('\nBeware, this is a DANGEROUS program.')
    print('It helps you erase directory trees.')
    print('There is NO UNDO function.')
    print('Files and directories that you erase here can only be recovered with forensic software for disk space that has not been overwritten since.')
    print('You have been warned. Be alert. If in doubt, always say no')
    
    dLst=glob(sOutputPrfx+"/*/", recursive = False)
    dLst.sort()
    n=1
    nMax=len(dLst)
    for sDir in dLst:
        show=True
        if(len(sDir)<12):
            show=False
            msg=f'ignoring suspicious folder {sDir}'
            try:
                print(style.RED + "{0}".format(msg) + style.RESET)
            except:
                print(msg)
        if not(args.ignoreKeep):
            if os.path.isfile(f'{sDir}{os.path.sep}keep'):
                show=False
                msg=f'Folder {n} of {nMax}: skipping folder {sDir} with keep flag.'
                try:
                    print(style.GREEN + "{0}".format(msg) + style.RESET)
                except:
                    print(msg)
        if(show):
            msg=f'Folder {n} of {nMax}:'
            try:
                print(style.YELLOW + "{0}".format(msg) + style.RESET)
            except:
                print(msg)
            containingFolder=os.path.basename(os.path.dirname(sDir))
            bHaveEmisAposResults=False
            sisterDir=sTmpPrfx+containingFolder
            sCmd='ls -l '+sisterDir
            if os.path.isfile(f'{sDir}{os.path.sep}{containingFolder}-emissions_apos.nc'):
                bHaveEmisAposResults=True
            try:
                if(os.path.exists(sisterDir)):
                    print(sisterDir)
                    runSysCmd(sCmd)
                    if os.path.isfile(f'{sisterDir}{os.path.sep}{containingFolder}-emissions_apos.nc'):
                        bHaveEmisAposResults=True
            except:
                pass
            sCmd='ls -l '+sDir
            try:
                print(sDir)
                runSysCmd(sCmd)
            except:
                pass
            if(bHaveEmisAposResults):
                msg='There is an *-emissions_apos.nc output file present.'
                try:
                    print(style.YELLOW + "{0}".format(msg) + style.RESET)
                except:
                    print(msg)
            answer='n'
            if(os.path.exists(sisterDir)):
                answer = input("Erase this directory pair and their respective contents? n(o)/y(es)/k(eep) [default=no/any key]")
            else:
                answer = input("Erase this directory and its contents? n(o)/y(es)/k(eep) [default=no/any key]")
            if answer.lower() in ["k","keep"]:
                sCmd=f'touch {sDir}{os.path.sep}keep'
                runSysCmd(sCmd)
                if(os.path.exists(sisterDir)):
                    sCmd=f'touch {sisterDir}{os.path.sep}keep'
                    runSysCmd(sCmd)
            elif answer.lower() in ["y","yes"]:
                print(f"Erasing {sDir} and all its contents. This cannot be undone.")
                answer = input("Are you sure? n(o)/y(es) [default=no/any key]")
                if answer.lower() in ["y","yes"]:
                    try:
                        if(os.path.exists(sisterDir)):
                            sCmd='rm -rf '+sisterDir
                            print(sCmd)
                            runSysCmd(sCmd)
                    except:
                        pass
                    try:
                        sCmd='rm -rf '+sDir
                        print(sCmd)
                        runSysCmd(sCmd)
                    except:
                        pass
        n+=1

main()
