#!/usr/bin/env python

import os
import sys
import argparse
from loguru import logger
from pandas import DataFrame  #, to_datetime, concat , read_csv, Timestamp, Timedelta, offsets  #, read_hdf, Series  #, read_hdf, Series
from icoscp.cpb.dobj import Dobj
from icoscp.cpb import metadata

def testPID(pidLst, sOutputPrfx=''):
    nBadies=0
    nBadMeta=0
    nTotal=0
    nGoodPIDs=0
    nBadIcoscpMeta=0
    for pid in pidLst:
        #allGood=True
        metaOk='failed'
        fileOk='failed'
        fName='data record not found'
        metafName='no meta data'
        nTotal+=1
        goOn=True
        metaDataRetrieved=False
        datafileFound=False
        #DobjFailed=False
        icosMetaFailed=False
        icoscpMetaOk='   yes'
        url="https://meta.icos-cp.eu/objects/"+pid
        try:
            dob = Dobj("https://meta.icos-cp.eu/objects/"+pid)
            dobjOk='   yes'
        except:
            nBadies+=1
            #DobjFailed=True
            dobjOk='failed'
            #allGood=False
            logger.error(f'Failed to create the data object Dobj for {url}')
            goOn=False
        try:
            pidMetadata = metadata.get(url)  
            icoscpMetaOk='   yes'
            mdata=[]
            ndr=0
            try:
                d=pidMetadata['specificInfo']['acquisition']['station']['id']
            except:
                d=''
                ndr+=1
            mdata.append(d)
            try:
                d=pidMetadata['specificInfo']['acquisition']['station']['countryCode']
            except:
                d=''
                ndr+=1
            mdata.append(d)
            try:
                d=pidMetadata['coverageGeo']['geometry']['coordinates'][1]
            except:
                d=''
                ndr+=1
            mdata.append(d)
            try:
                d=pidMetadata['coverageGeo']['geometry']['coordinates'][0]
            except:
                d=''
                ndr+=1
            mdata.append(d)
            try:
                d=pidMetadata['coverageGeo']['geometry']['coordinates'][2]
            except:
                d=''
                ndr+=1
            mdata.append(d)
            try:
                d=pidMetadata['specificInfo']['acquisition']['samplingHeight']
            except:
                d=''
                ndr+=1
            mdata.append(d)
            try:
                d=pidMetadata['size']
            except:
                d=''
                ndr+=1
            mdata.append(d)
            try:
                d=pidMetadata['specificInfo']['nRows']
            except:
                d=''
                ndr+=1
            mdata.append(d)
            try:
                d=pidMetadata['specification']['dataLevel']
            except:
                d=''
                ndr+=1
            mdata.append(d)
            try:
                d=pidMetadata['specificInfo']['acquisition']['interval']['start']
            except:
                d=''
                ndr+=1
            mdata.append(d)
            try:
                d=pidMetadata['specificInfo']['acquisition']['interval']['stop']
            except:
                d=''
                ndr+=1
            mdata.append(d)
            try:
                d=pidMetadata['specificInfo']['productionInfo']['dateTime']
            except:
                d=''
                ndr+=1
            mdata.append(d)
            try:
                d=pidMetadata['accessUrl']
            except:
                d=''
                ndr+=1
            mdata.append(d)
            try:
                d=pidMetadata['fileName']
            except:
                d=''
                ndr+=1
            mdata.append(d)
            try:
                d=pidMetadata['specification']['self']['label']
            except:
                d=''
                ndr+=1
            mdata.append(d)
            print(f'icoscpMetadata for pid {pid}=\n{mdata}')
            if(ndr>1):
                icosMetaFailed=True
                icoscpMetaOk='failed'
        except:
            pass
        if(goOn):
            try:
                pidMeta = dob.meta()
                metaDataRetrieved=True
                metaOk='   yes'
                fName=pidMeta.fileName
                metafName=pidMeta.fileName
                logger.info(f'Reading of metadata successful. Found file {fName} for pid {pid}.') 
            except:
                nBadMeta+=1
                logger.error(f'Failed to read the meta data for data object Dobj at {url}')
        fName='/data/dataAppStorage/netcdfTimeSeries/'+pid
        if(os.path.exists(fName)):
            datafileFound=True
        else:
            fn='/data/dataAppStorage/asciiAtcProductTimeSer/'+pid
            if(os.path.exists(fName)):
                datafileFound=True
            else:
                fName='/data/dataAppStorage/asciiAtcTimeSer/'+pid
                if(os.path.exists(fn)):
                    datafileFound=True
                else:
                    fName='/data/dataAppStorage/asciiAtcProductTimeSer/'+pid
                    if(os.path.exists(fName)):
                        datafileFound=True
        if(datafileFound):
            fileOk='   yes'
        data=[pid,dobjOk, metaOk,  icoscpMetaOk,  fileOk, metafName, fName,  url]+mdata
        if((datafileFound) and (metaDataRetrieved) and(not icosMetaFailed)):
            if(nGoodPIDs==0):
                columnNames=['pid', 'Dobj.ok','dobjMeta.ok', 'icoscpMeta.ok', 'file.ok', 'metafName', 'fName','url', 'stationID', 'country', 'latitude','longitude','altitude','samplingHeight','size', 'nRows','dataLevel','obsStart','obsStop','productionTime','accessUrl','fileName','dataSetLabel'] 
                dfgood=DataFrame(data=[data], columns=columnNames)
            else:
                dfgood.loc[len(dfgood)] = data
            nGoodPIDs+=1
        else:
            if(nBadies==0):
                columnNames=['pid', 'Dobj.ok','dobjMeta.ok', 'icoscpMeta.ok', 'file.ok', 'metafName', 'fName','url', 'stationID', 'country', 'latitude','longitude','altitude','samplingHeight','size', 'nRows','dataLevel','obsStart','obsStop','productionTime','accessUrl','fileName','dataSetLabel'] 
                dfbad=DataFrame(data=[data], columns=columnNames)
                print(f'Data records with some issues:\r\n{columnNames}')
            else:
                dfbad.loc[len(dfbad)] = data
            print(f'{data}')
            nBadies+=1
        nTotal+=1
    if(nBadies > 0):
        logger.warning(f"A total of {nBadies} PIDs out of {nTotal} data objects had some issues,  thereof {nBadMeta} had bad dobjMetaData and {nBadIcoscpMeta} had bad icoscMetaData.")
        dfbad.to_csv(sOutputPrfx+'bad-PIDs-testresult.csv', encoding='utf-8', mode='w', sep=',')
    if(nGoodPIDs > 0):
        dfgood.to_csv(sOutputPrfx+'good-PIDs-testresult.csv', encoding='utf-8', mode='w', sep=',')
        

def readLstOfPids(pidFile):
    with open(pidFile) as file:
       selectedDobjLst = [line.rstrip() for line in file]
    # read the observational data from all the files in the dobjLst. These are of type ICOS ATC time series
    if((selectedDobjLst is None) or (len(selectedDobjLst)<1)):
        logger.error(f"Fatal Error! User specified pidFile {pidFile} is empty.")
        sys.exit(-1)
    return(selectedDobjLst)


def runPidTest(pidFile, sOutputPrfx):
    pidLst=readLstOfPids(pidFile)
    testPID(pidLst, sOutputPrfx)
    print('Done. testPID task completed.')


p = argparse.ArgumentParser()
p.add_argument('--pidLst', default=None,  help='Name of a text file listing the carbon portal PIDs (as a single column) that you wish to test.')
#p.add_argument('--fDiscoveredObs', dest='fDiscoveredObservations', default=None,  help="If step2 is set you must specify the .csv file that lists the observations discovered by LUMIA, typically named DiscoveredObservations.csv")   # yaml configuration file where the user plans his or her Lumia run: parameters, input files etc.
p.add_argument('--verbosity', '-v', dest='verbosity', default='INFO')
p.add_argument('--sOutputPrfx', '-o', default='', help='Output file names will be prefixed with your string -- your string may contain a path or a partial name or both')
args, unknown = p.parse_known_args(sys.argv[1:])

# Set the verbosity in the logger (loguru quirks ...)
logger.remove()
logger.add(sys.stderr, level=args.verbosity)


if(args.pidLst is None):
    logger.error("LumiaGUI: Fatal error: no user configuration (yaml) file provided.")
    sys.exit(1)
else:
    pidFile = args.pidLst

if (not os.path.isfile(pidFile)):
    logger.error(f"Fatal error in testPID: User specified pidLst file {pidFile} does not exist. Abort.")
    sys.exit(-3)

runPidTest(pidFile, args.sOutputPrfx)
