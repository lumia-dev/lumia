#!/usr/bin/env python

import os
import sys
import argparse
from loguru import logger
from pandas import DataFrame,  read_csv  #, to_datetime, concat , read_csv, Timestamp, Timedelta, offsets  #, read_hdf, Series  #, read_hdf, Series
#from icoscp.cpb.dobj import Dobj
from icoscp.cpb import metadata
from icoscp_core.icos import meta as coreMeta

def runSysCmd(sCmd,  ignoreError=False):
    try:
        os.system(sCmd)
    except:
        if(ignoreError==False):
            sTxt=f"Fatal Error: Failed to execute system command >>{sCmd}<<. Please check your write permissions and possibly disk space etc."
            logger.warning(sTxt)
        return False
    return True


def  getMetaDataFromPid_via_icosCore(pid, icosStationLut):
    mdata=[]
    ndr=0
    bAcceptable=True  
    url="https://meta.icos-cp.eu/objects/"+pid
    sid='   '
    try:
        # first try the low-level icscp-core library to query the metadata
        pidMetadataCore =  coreMeta.get_dobj_meta(url)
        #columnNames=['pid', 'dobjMeta.ok', 'icoscpMeta.ok', 'file.ok', 'metafName', 'fName','url', 
        # 'stationID', 'country', 'latitude','longitude','altitude','samplingHeight','is-ICOS','size', 'nRows','dataLevel',
        # 'obsStart','obsStop','productionTime','accessUrl','fileName','dataSetLabel'] 
        try:
            d=pidMetadataCore.specificInfo.acquisition.station.id
            sid=d[:3]
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read stationid from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.acquisition.station.countryCode
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read countryCode from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.acquisition.station.location.lat
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station latitude from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.acquisition.station.location.lon
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station longitude from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.acquisition.station.location.alt
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station altitude from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.acquisition.samplingHeight
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station samplingHeight from metadata')
        mdata.append(d)
        try:
            #       projectUrl=pidMetadataCore.specification.project.self.uri 
            # is the station an ICOS station? values are {'1','2','A','no'} class 1, 2, or A(ssociated) or else 'no' meaning it is not an ICOS station
            d='no'
            try:
                value=icosStationLut[sid]
                d=value
            except:
                pass
        except:
            ndr+=1
            logger.debug('Failed to read ICOS flag from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.size
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read size from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.nRows
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read nRows from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specification.dataLevel
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read dataLevel from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.acquisition.interval.start
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read acquisition start from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.acquisition.interval.stop
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read acquisition stop from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.productionInfo.dateTime
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read productionInfo from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.accessUrl
        except:
            d=''
            #ndr+=1  # we got here via the url, so no drama if we don't have a value we already know
        mdata.append(d)
        try:
            d=pidMetadataCore.fileName
        except:
            d=''
            #ndr+=1 # informativ but it is not being used
        mdata.append(d)
        try:
            d=pidMetadataCore.specification.self.label
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read data label from metadata')
        mdata.append(d)
    except:
        print(f'Failed to get the metadata using icoscp_core.icos.coreMeta.get_dobj_meta(url) for url={url}')
        return(None,  False)  # Tell the calling parent routine that things went south...

    if(ndr>1):
        bAcceptable=False
    return (mdata, bAcceptable)


def  getMetaDataFromPid_via_icoscp(pid, icosStationLut):
    mdata=[]
    ndr=0
    bAcceptable=True  
    url="https://meta.icos-cp.eu/objects/"+pid
    sid='   '
    try:
        # first try the low-level icscp-core library to query the metadata
        pidMetadata =  metadata.get(url)
        #columnNames=['pid', 'dobjMeta.ok', 'icoscpMeta.ok', 'file.ok', 'metafName', 'fName','url', 
        # 'stationID', 'country', 'latitude','longitude','altitude','samplingHeight','is-ICOS','size', 'nRows','dataLevel',
        # 'obsStart','obsStop','productionTime','accessUrl','fileName','dataSetLabel'] 
        try:
            d=pidMetadata['specificInfo']['acquisition']['station']['id']
            sid=d[:3]
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read stationid from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['acquisition']['station']['countryCode']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read countryCode from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['acquisition']['station']['location']['lat']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station latitude from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['acquisition']['station']['location']['lon']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station longitude from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['acquisition']['station']['location']['alt']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station altitude from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['acquisition']['samplingHeight']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station samplingHeight from metadata')
        mdata.append(d)
        try:
            #       projectUrl=pidMetadataCore.specification.project.self.uri 
            # is the station an ICOS station? values are {'1','2','A','no'} class 1, 2, or A(ssociated) or else 'no' meaning it is not an ICOS station
            d='no'
            try:
                value=icosStationLut[sid]
                d=value
            except:
                pass
        except:
            pass
        try:
            projectUrl=pidMetadata['specification']['project']['self']['uri'] 
            d=False
            if(projectUrl == 'http://meta.icos-cp.eu/resources/projects/icos'):
                d=True
            #d=pidMetadata['specification']['project']['self']['uri'] == 'http://meta.icos-cp.eu/resources/projects/icos' # does the self project url point to being an ICOS project?
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read ICOS flag from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['size']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read size from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['nRows']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read nRows from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specification']['dataLevel']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read dataLevel from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['acquisition']['interval']['start']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read acquisition start from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['acquisition']['interval']['stop']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read acquisition stop from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['productionInfo']['dateTime']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read productionInfo from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['accessUrl']
        except:
            d=''
            #ndr+=1  # we got here via the url, so no drama if we don't have a value we already know
        mdata.append(d)
        try:
            d=pidMetadata['fileName']
        except:
            d=''
            #ndr+=1 # informativ but it is not being used
        mdata.append(d)
        try:
            d=pidMetadata['specification']['self']['label']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read data label from metadata')
        mdata.append(d)
    except:
        print(f'Failed to get the metadata using icoscp.cpb.metadata.get(url) for url={url}')
        return(None,  False)  # Tell the calling parent routine that things went south...
    if(ndr>1):
        bAcceptable=False
    return (mdata, bAcceptable)


def testPID(pidLst, sOutputPrfx=''):
    nBadies=0
    nBadMeta=0
    nTotal=0
    nGoodPIDs=0
    nBadIcoscpMeta=0
    # Make a lookup table to know which stations are considered ICOS stations:
    from icoscp_core.icos import meta
    from icoscp_core.sparql import as_string, as_uri
    statClassQuery = 'select * where{?station <http://meta.icos-cp.eu/ontologies/cpmeta/hasStationClass> ?class}'
    # The table lists the ICOS station classes as described here:
    # https://www.icos-cp.eu/about/join-icos/process-stations#toc-station-classes-class-1-class-2-associated-stations
    statClassLookup = {
        as_uri('station', row): as_string('class', row)
        for row in meta.sparql_select(statClassQuery).bindings            
        # for row in meta.sparql_select(stat_class_query).bindings
    }
    # is the station an ICOS station? values are {'1','2','A','no'} class 1, 2, or A(ssociated) or else 'no' meaning it is not an ICOS station
    icosStationLut={}
    for key, value in  statClassLookup.items():
        if('/resources/stations/AS_' in key):
            mkey=key[-3:]
            icosStationLut[mkey]= value[0]  # Associated becomes 'A' one-letter-code
            
    
    #print(f'statClassLookup={statClassLookup}')
    #station_basic_meta_lookup = {s.uri: s for s in meta.list_stations()}
    #print(f'station_basic_meta_lookup={station_basic_meta_lookup}')
    
    for pid in pidLst:
        metaOk='failed'
        fileOk='failed'
        fName='data record not found'
        metafName='no meta data'
        metaDataRetrieved=True
        datafileFound=False
        icosMetaOk=False
        url="https://meta.icos-cp.eu/objects/"+pid
        (mdata, icosMetaOk)=getMetaDataFromPid_via_icosCore(pid,  icosStationLut)
        if(mdata is None):
            logger.error(f'Attempting fallback method for data object {url}. Fingers crossed...')
            (mdata, icosMetaOk)=getMetaDataFromPid_via_icoscp(pid, icosStationLut)
            if(mdata is None):
                logger.error(f'Failed: Obtaining the metadata for data object {url} was unsuccessful. Thus this data set cannot be used.')
        if(mdata is None):
            metaDataRetrieved=False
        if(icosMetaOk):
            icoscpMetaOk='   yes'
        else:
            icoscpMetaOk='    no'

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
        data=[pid,metaOk,  icoscpMetaOk,  fileOk, metafName, fName,  url]+mdata
        if((datafileFound) and (metaDataRetrieved) and(icosMetaOk)):
            if(nGoodPIDs==0):
                columnNames=['pid', 'dobjMeta.ok', 'icoscpMeta.ok', 'file.ok', 'metafName', 'fName','url', 'stationID', 'country', 'latitude','longitude','altitude','samplingHeight','is-ICOS','size', 'nRows','dataLevel','obsStart','obsStop','productionTime','accessUrl','fileName','dataSetLabel'] 
                dfgood=DataFrame(data=[data], columns=columnNames)
            else:
                dfgood.loc[len(dfgood)] = data
            nGoodPIDs+=1
        else:
            if(nBadies==0):
                columnNames=['pid', 'dobjMeta.ok', 'icoscpMeta.ok', 'file.ok', 'metafName', 'fName','url', 'stationID', 'country', 'latitude','longitude','altitude','samplingHeight','is-ICOS','size', 'nRows','dataLevel','obsStart','obsStop','productionTime','accessUrl','fileName','dataSetLabel'] 
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
        logger.info(f'Bad PIDs with some issues have been written to {sOutputPrfx}bad-PIDs-testresult.csv')
    if(nGoodPIDs > 0):
        dfgood.to_csv(sOutputPrfx+'good-PIDs-testresult.csv', encoding='utf-8', mode='w', sep=',')
        logger.info(f'Good PIDs ()with all queried properties found) have been written to {sOutputPrfx}good-PIDs-testresult.csv')
        

def readLstOfPids(pidFile):
    with open(pidFile) as file:
       selectedDobjLst = [line.rstrip() for line in file]
    # read the observational data from all the files in the dobjLst. These are of type ICOS ATC time series
    if((selectedDobjLst is None) or (len(selectedDobjLst)<1)):
        logger.error(f"Fatal Error! User specified pidFile {pidFile} is empty.")
        sys.exit(-1)
    return(selectedDobjLst)


def runPidTest(pidFile, sOutputPrfx):
    if('.csv'==pidFile[-4:]):
        dfs = read_csv (pidFile)
        try:
            pidLst = dfs["pid"].values.tolist()
        except:
            logger.error(f"Failed to extract the >>pid<< column from your input csv file {pidFile}.")
            sys.exit(-1)
    else:
        try:
            pidLst=readLstOfPids(pidFile)
        except:
            logger.error(f"Failed to read the PIDs from your input file {pidFile}.")
            sys.exit(-1)
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

'''
...some wisdom from the horse's mouth...

Hi all,

Arndt, "meta" is a property on a Dobj class, not a function, you must not use parentheses after it.

When it comes to the 'coverageGeo' property, it's an arbitrary GeoJSON (which is an official standard, actually).
Depending on the data object, the GeoJSON may look differently. So, you cannot always count on availability of "geometry" property.

ObservationStation geo-location -- obtaining latitude, longitude, altitude reliably:
==========================
coverageGeo is an attribute of "meta" property, but yes, I would probably mostly recommend using for the purposes of visualization, not robust station location lookup. 
For the latter, one should keep in mind that in general, not all data object have geostationary station they are associated with. But, for atmospheric station measurement
data, when using icoscp lib, you could write something like

station_location = dob.meta['specificInfo']['acquisition']['station']['location']

When using icoscp_core lib, one take two approaches. If one is only interested in a single data object, the full example code could be

from icoscp_core.icos import meta
dob_meta = meta.get_dobj_meta('https://meta.icos-cp.eu/objects/uWgpKU-q44efg-KX7_H3GHAY')
station_location = dob_meta.specificInfo.acquisition.station.location

But when working with many objects, it's more performant to list all stations first, and make a lookup table from station URI to the station object:

station_lookup = {s.uri: s for s in meta.list_stations()}

This dictionary can then be used to quickly look up all kinds of metadata information about a station, based on station URI. Then one does not need to fetch full metadata 
for every data object one works with (this is relatively slow operation, especially if you work with many objects).

is-ICOS-flag:
==============
Arndt, answering your question about ICOS/non-ICOS data object discrimination while I am at it: when using icoscp, you can write

is_icos = (dob.meta['specification']['project']['self']['uri'] == 'http://meta.icos-cp.eu/resources/projects/icos')

(mind the "http", not "https", in the URL), and for icoscp_core the syntax would be

is_icos = (dob_meta.specification.project.self.uri == 'http://meta.icos-cp.eu/resources/projects/icos')

(again, no need for the quotes and brackets, plus you get autocomplete support)

Hope this helps. Best regards,
/Oleg.
'''
