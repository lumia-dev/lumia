#!/usr/bin/env python
# coding: utf-8


import sys
import os
import re
import datetime
import housekeeping as hk
from loguru import logger
import numpy as np
from pandas import DataFrame  #, concat
# from icoscp.sparql import sparqls, runsparql
try:
    import myCarbonPortalTools
except:
    try:
        import lumia.GUI.myCarbonPortalTools as myCarbonPortalTools
    except:
        logger.error('Failed to import lumia.GUI.myCarbonPortalTools.py')
        sys.exit(1)
    
from icoscp.sparql.runsparql import RunSparql
from icoscp.cpb.dobj import Dobj
from icoscp.collection import collection
from icoscp.cpb import metadata
from icoscp_core.icos import meta as coreMeta

bDEBUG =False

# GLOBALS
#
# The pre-processed data used by Lumia (as a-priori) is described e.g. here:
# https://meta.icos-cp.eu/objects/sNkFBomuWN94yAqEXXSYAW54
#
# Observational data available on the ICOS data portal you can discover via the online tool at
# old: https://data.icos-cp.eu/portal/#%7B%22filterCategories%22%3A%7B%22theme%22%3A%5B%22atmosphere%22%5D%2C%22level%22%3A%5B2%5D%2C%22valType%22%3A%5B%22co2MixingRatio%22%5D%2C%22stationclass%22%3A%5B%22ICOS%22%5D%2C%22project%22%3A%5B%22icos%22%5D%2C%22type%22%3A%5B%22atcCo2L2DataObject%22%5D%2C%22variable%22%3A%5B%22http%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fco2atcMoleFrac%22%5D%7D%2C%22filterTemporal%22%3A%7B%22df%22%3A%222017-12-31%22%2C%22dt%22%3A%222018-01-31%22%7D%2C%22tabs%22%3A%7B%22resultTab%22%3A1%7D%7D

# You can try out sparql queries and export the ones you are happy with, with the button
# "Open the SPARQL query basic search result" page - the icon looks like an arrow pointing up and then to the right out of a stylized box.

# However, it is usually better to keep the url rather than the exported resulting script, as the url is better suited to discover new data, whenever new data sets are added. 
# Also, you may want to tidy up the resulting query in a similar fashion to the example provided here, that Oleg prettied up for me:
#
# https://data.icos-cp.eu/portal/#%7B%22filterCategories%22%3A%7B%22valType%22%3A%5B%22co2MixingRatioMolMol%22%2C%22co2MixingRatio%22%5D%2C%22theme%22%3A%5B%22atmosphere%22%5D%7D%7D
#
'''
prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
prefix prov: <http://www.w3.org/ns/prov#>
prefix xsd: <http://www.w3.org/2001/XMLSchema#>
select ?dobj ?samplingHeight ?spec ?fileName ?size ?submTime ?timeStart ?timeEnd
where {
    VALUES ?spec {
        <http://meta.icos-cp.eu/resources/cpmeta/atcCo2L2DataObject>
        <http://meta.icos-cp.eu/resources/cpmeta/atcCo2Product>
        <http://meta.icos-cp.eu/resources/cpmeta/ObspackTimeSerieResult>
    }
    ?dobj cpmeta:hasObjectSpec ?spec .
    ?dobj cpmeta:hasSizeInBytes ?size .
    ?dobj cpmeta:hasName ?fileName .
    ?dobj cpmeta:wasSubmittedBy/prov:endedAtTime ?submTime .
    ?dobj cpmeta:wasAcquiredBy / prov:startedAtTime ?timeStart .
    ?dobj cpmeta:wasAcquiredBy / prov:endedAtTime ?timeEnd .
    OPTIONAL{?dobj cpmeta:wasAcquiredBy / cpmeta:hasSamplingHeight ?samplingHeight }
    FILTER NOT EXISTS {[] cpmeta:isNextVersionOf ?dobj}
FILTER( !(?timeStart > '2018-12-30T23:00:00.000Z'^^xsd:dateTime || ?timeEnd < '2017-12-31T23:00:00.000Z'^^xsd:dateTime) )

}
order by desc(?submTime)
'''



# *****************************************************************************************
def getTimeForSparqlQuery(pdTime,  startOrEndTime=None,  timeStep=None):
    '''
    Function getEndTimeForSparqlQuery
    
    @param pdTime pandas.timeStamp  date/time from/until when observations will be queried
    @returns a DateTimeStamp string of format 'YYYY-01-01T00:00:01.000Z'
    @timeStep is the run.time.timestep defined in the user's rc configuration file'
  '''
    if((pdTime is None) or (startOrEndTime is None)):
        logger.error('Start date of observation interval or startOrEndTime operator not provided.')
        sys.exit(-1)
    if('start' in startOrEndTime):
        operation='subtractTime'
    else:
        operation='addTime'
    delta=int(60)
    if((timeStep is not None) and (len(timeStep)>1)):
        if('h' in timeStep):
            delta=int(timeStep[:-1])*60
        elif('m' in timeStep):
            delta=int(timeStep[:-1])
        elif('d' in timeStep):
            delta=int(timeStep[:-1])*60*24
    if('add' in operation):
        endTime=pdTime + datetime.timedelta(minutes=delta)            
    else:
        endTime=pdTime - datetime.timedelta(minutes=delta)            
    sSparqlTime=str(endTime)
    sSparqlTime=sSparqlTime[:10]+'T'+sSparqlTime[11:19]+'.000Z'
    return sSparqlTime


# ***********************************************************************************************
def getCo2DryMolFractionObjectsFromSparql(pdTimeStart: datetime=None, pdTimeEnd: datetime=None, timeStep=None,iVerbosityLv=1):
    '''
    Function 

    @param pdTimeStart :  from when on we want to get the observations
    @type datetime
    @param pdTimeEnd : until when on we want to get the observations
    @type datetime
    @param iVerbosityLv : defines how much detail of program progress is printed to stdout (defaults to 1)
    @type integer between 0 and 3 (optional)
    @return :  dobj (the data object returned by the SPARQL query)
    @rtype dobj (a structured dictionary of strings)
    '''
    sTimeStart=getTimeForSparqlQuery(pdTimeStart,  startOrEndTime='startTime',  timeStep=timeStep)
    sTimeEnd=getTimeForSparqlQuery(pdTimeEnd,  startOrEndTime='endTime',  timeStep=timeStep)
    query = '''
    prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
    prefix prov: <http://www.w3.org/ns/prov#>
    prefix xsd: <http://www.w3.org/2001/XMLSchema#>
    select ?dobj ?samplingHeight ?spec ?fileName ?size ?submTime ?timeStart ?timeEnd
    where {
        VALUES ?spec {
            <http://meta.icos-cp.eu/resources/cpmeta/atcCo2L2DataObject>
            <http://meta.icos-cp.eu/resources/cpmeta/atcCo2Product>
            <http://meta.icos-cp.eu/resources/cpmeta/ObspackTimeSerieResult>
        }
        ?dobj cpmeta:hasObjectSpec ?spec .
        ?dobj cpmeta:hasSizeInBytes ?size .
        ?dobj cpmeta:hasName ?fileName .
        ?dobj cpmeta:wasSubmittedBy/prov:endedAtTime ?submTime .
        ?dobj cpmeta:wasAcquiredBy / prov:startedAtTime ?timeStart .
        ?dobj cpmeta:wasAcquiredBy / prov:endedAtTime ?timeEnd .
        OPTIONAL{?dobj cpmeta:wasAcquiredBy / cpmeta:hasSamplingHeight ?samplingHeight }
        FILTER NOT EXISTS {[] cpmeta:isNextVersionOf ?dobj}
    FILTER( !(?timeStart > \''''+sTimeEnd+'''\'^^xsd:dateTime || ?timeEnd < \''''+sTimeStart+'''\'^^xsd:dateTime) ) 
    }
    order by desc(?submTime)
    '''
    # example: sFileName='VPRM_ECMWF_NEE_2020_CP.nc'
    logger.debug(f'SPARQL query= {query}')    
    dobj = RunSparql(query,output_format='nc').run()
    # logger.debug(f'dobj= {dobj}')
    return(dobj)



# ***********************************************************************************************
def getDobjFromSparql(tracer='CO2', pdTimeStart: datetime=None, pdTimeEnd: datetime=None, timeStep=None,  sDataType=None,  iVerbosityLv=1):
    '''
    Function  -- for historical and possible future use with ch4. 
    Presently Lumia only uses the function getCo2DryMolFractionObjectsFromSparql()

    @param tracer :    the name of the tracer like co2, ch4, etc.
    @type string 
    @param pdTimeStart :  from when on we want to get the observations
    @type datetime
    @param pdTimeEnd : until when on we want to get the observations
    @type datetime
    @param sDataType optional name of the data type. Default is 'atcCo2L2DataObject'  #'ICOS ATC CO2 Release'
    @type string
    @param iVerbosityLv : defines how much detail of program progress is printed to stdout (defaults to 1)
    @type integer between 0 and 3 (optional)
    @return :  dobj (the data object returned by the SPARQL query)
    @rtype dobj (a structured dictionary of strings)
    '''
    # sTimeStart=getStartTimeForSparqlQuery(pdTimeStart, timeStep)
    sTimeStart=getTimeForSparqlQuery(pdTimeStart,  startOrEndTime='startTime',  timeStep=timeStep)
    # sTimeEnd=getEndTimeForSparqlQuery(pdTimeEnd, timeStep)
    sTimeEnd=getTimeForSparqlQuery(pdTimeEnd,  startOrEndTime='endTime',  timeStep=timeStep)
    LcTracer=tracer.lower()
    if(sDataType is None):
        sDataType='atcCo2L2DataObject'  #'ICOS ATC CO2 Release'

    #=findDobjFromPartialNameAndDate(sKeyword, timeStart, timeEnd, iRequestedYear)
    query = '''
    prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
    prefix prov: <http://www.w3.org/ns/prov#>
    prefix xsd: <http://www.w3.org/2001/XMLSchema#>
    select ?dobj ?hasNextVersion ?spec ?fileName ?size ?submTime ?timeStart ?timeEnd
    where {
        VALUES ?spec {<http://meta.icos-cp.eu/resources/cpmeta/'''+sDataType+'''>}
        ?dobj cpmeta:hasObjectSpec ?spec .
        BIND(EXISTS{[] cpmeta:isNextVersionOf ?dobj} AS ?hasNextVersion)
        VALUES ?station {<http://meta.icos-cp.eu/resources/stations/AS_PAL> <http://meta.icos-cp.eu/resources/stations/AS_TRN> <http://meta.icos-cp.eu/resources/stations/AS_GAT> <http://meta.icos-cp.eu/resources/stations/AS_HPB> <http://meta.icos-cp.eu/resources/stations/AS_IPR> <http://meta.icos-cp.eu/resources/stations/AS_OPE> <http://meta.icos-cp.eu/resources/stations/AS_KIT> <http://meta.icos-cp.eu/resources/stations/AS_SMR> <http://meta.icos-cp.eu/resources/stations/AS_SAC> <http://meta.icos-cp.eu/resources/stations/AS_ZEP> <http://meta.icos-cp.eu/resources/stations/AS_TOH> <http://meta.icos-cp.eu/resources/stations/AS_KRE> <http://meta.icos-cp.eu/resources/stations/AS_SVB> <http://meta.icos-cp.eu/resources/stations/AS_HTM> <http://meta.icos-cp.eu/resources/stations/AS_JFJ> <http://meta.icos-cp.eu/resources/stations/AS_PUY> <http://meta.icos-cp.eu/resources/stations/AS_NOR> <http://meta.icos-cp.eu/resources/stations/AS_LIN>}
                ?dobj cpmeta:wasAcquiredBy/prov:wasAssociatedWith ?station .
        ?dobj cpmeta:hasSizeInBytes ?size .
    ?dobj cpmeta:hasName ?fileName .
    ?dobj cpmeta:wasSubmittedBy/prov:endedAtTime ?submTime .
    ?dobj cpmeta:hasStartTime | (cpmeta:wasAcquiredBy / prov:startedAtTime) ?timeStart .
    ?dobj cpmeta:hasEndTime | (cpmeta:wasAcquiredBy / prov:endedAtTime) ?timeEnd .
        FILTER NOT EXISTS {[] cpmeta:isNextVersionOf ?dobj}
    FILTER( !(?timeStart > \''''+sTimeEnd+'''\'^^xsd:dateTime || ?timeEnd < \''''+sTimeStart+'''\'^^xsd:dateTime) ) 
        {
            {FILTER NOT EXISTS {?dobj cpmeta:hasVariableName ?varName}}
            UNION
            {
                ?dobj cpmeta:hasVariableName ?varName
                FILTER (?varName = "'''+LcTracer+'''")
            }
        }
    }
    order by desc(?submTime)
    offset 0 limit 20
    '''
    # example: sFileName='VPRM_ECMWF_NEE_2020_CP.nc'
    logger.debug(f'SPARQL query= {query}')    
    dobj = RunSparql(query,output_format='nc').run()
    # logger.debug(f'dobj= {dobj}')
    return(dobj)



# ***********************************************************************************************
def extractFnamesFromDobj(dobj, iVerbosityLv=1):
    '''
    Function 

    @param dobj the object returned from the SPARQL query that contains the PIDs of all the files we want to extract
    @cpDir Location where to look for the existance of the files we expect to find from the SPARQL query
    @return a list of strings. Each strings contains the name of one file on the carbon portal that we later need to read.
    @rtype List[strings]
    
    At the bottom of this python file you may find an example of a dobj for co2 observations for 20180101 - 20180201
    It is from such a list of strings that we need to extract each and every PID for the individual files,
    at least one per observation site.
    
    The relevant section looks typically as in this example:
        "dobj" : {
          "type" : "uri",
          "value" : "https://meta.icos-cp.eu/objects/LLz6BZr6LCt1Pt0w-U_DLxWZ"
        },
    And we want to extract "LLz6BZr6LCt1Pt0w-U_DLxWZ"
    '''
    #sFileNameOnCarbonPortal=None
    sPID=''
    fNameLst=[]
    try:
        if len(dobj.split('/')) > 1:
            # 
            bGrabNextUrl=False
            words=dobj.split("\"") # split at quotation marks
            cwLst=[]
            for word in words:
                if(re.search('[a-zA-Z]', word) is not None):
                    cwLst.append(word)  # 
                    if('dobj' in word):
                        bGrabNextUrl=True
                    if((bGrabNextUrl==True) and ('https' in word)):
                        sPID=word.split('/')[-1]  # Grab the last part of the url without directories
                        # Grab the PID from the end of the http value string, may look like gibberish "nBGgNpQxPYXBYiBuGGFp2VRF"
                        bGrabNextUrl=False
                        #  /data/dataAppStorage/asciiAtcProductTimeSer/LLz6BZr6LCt1Pt0w-U_DLxWZ.cpb
                        fNameLst.append(sPID)
    except:
        logger.error("No valid observational data found in SPARQL query dobj=")
        logger.error(f"{dobj}")
        return(None)
    return fNameLst




# ***********************************************************************************************
def discoverObservationsOnCarbonPortal(tracer='CO2', pdTimeStart: datetime=None, pdTimeEnd: datetime=None, 
                                            timeStep=None,  ymlContents=None,  sDataType=None, printProgress=False,  iVerbosityLv=1):
    """
    Function discoverObservationsOnCarbonPortal
    
    @param tracer :    the name of the tracer like co2, ch4, etc.
    @type string 
    @param pdTimeStart :  from when on we want to get the observations
    @type datetime
    @param pdTimeEnd : until when on we want to get the observations
    @type datetime
    @param iVerbosityLv : defines how much detail of program progress is printed to stdout (defaults to 1)
    @type integer between 0 and 3 (optional)
    @return ???:  the actual data object? file list? mind you, columns are different....
    @rtype 
 
    discoverObservationsOnCarbonPortal attempts to find matching ICOS CO2 observations in form of their individual unique-identifier (PID)
    for the requested data records. The latter should refer to a list of level2 netcdf file (by name) on the ICOS data portal. 
    The function relies on a sparql query and tries to read the requested cpb files from the carbon portal using the icoscp package. 
    Returns (...-dataset) if successful; (None) if unsuccessful.
    """
    if((tracer=='CO2') or (tracer=='co2')):
        dobj=getCo2DryMolFractionObjectsFromSparql(pdTimeStart=pdTimeStart, pdTimeEnd=pdTimeEnd,  timeStep=timeStep, iVerbosityLv=iVerbosityLv)
    else:        
        dobj=getDobjFromSparql(tracer=tracer, pdTimeStart=pdTimeStart, pdTimeEnd=pdTimeEnd, timeStep=timeStep,  sDataType=sDataType,  iVerbosityLv=iVerbosityLv)
    dobjLst=extractFnamesFromDobj(dobj, iVerbosityLv=iVerbosityLv)
    #logger.debug(f"dobjLst={dobjLst}")
    # remove any possible duplicates from the list of objects
    finalDobjLst=set(dobjLst)
    lf=len(finalDobjLst)
    l=len(dobjLst)
    n=l-lf
    if(n>0):
        logger.debug(f"removed {n} duplicates")
    logger.info(f"Found {lf} valid data objects on the carbon portal (dry mole fraction observation files for chosen tracer) Removing duplicates (if any)...")
    if(0>1): #obsolete bit of code with new SPARQL query...but let's first test the new query thoroughly before removing this snipet
        cpCollections=collection.getIdList()
        for collitem in cpCollections.values:
            pid=collitem[0].split('/')[-1]
           #logger.info(f"Checking collection pid= {pid}")
            if(pid in finalDobjLst):
                logger.info(f"Removing collection {pid}")
                finalDobjLst.remove(pid)
    # collpid="ZZwlNi4a8_AFsscsxp603t5t" # European Obspack compilation
    # This  data package contains high accuracy CO2 dry air mole fractions  from 65 ICOS and non-ICOS European observatories 
    # at in total 143 observation levels since 1972
    # Collections are now rejected by default in myCarbonPortalTools.getMetaDataFromPid_via_icoscp_core()
    lf=len(finalDobjLst)
    logger.info(f"Found {lf} valid data objects on the carbon portal (dry mole fraction observation files for chosen tracer).")
    
    # Create a dataframe with meaningful parameters from which a user may filter subsets
    with open( r'ListOfAllValidPIDs.txt', 'w') as fp:
        for item in finalDobjLst:
            fp.write("%s\n" % item)
    i=0
    df=DataFrame()
    bSelected=True
    nObj=len(finalDobjLst)
    step=int(0.5+(nObj/50.0))
    if (step<1):
        step=1
        logger.error('finalDobjLst is empty.')
    #print('|_________________________________________________|')
    if(printProgress):
        myCarbonPortalTools.printProgressBar(0, nObj, prefix = 'Gathering meta data progress:', suffix = 'Done', length = 50)
    nBadDataSets=0
    badPids=[]
    # icosStationLut=myCarbonPortalTools.createIcosStationLut()
    for n, pid in enumerate(finalDobjLst):
        url="https://meta.icos-cp.eu/objects/"+pid
        (mdata, bDataSuccessfullyRead)=myCarbonPortalTools.getMetaDataFromPid_via_icoscp_core(pid)
        '''
        returns a list of these objects: ['stationID', 'country', 'IcosClass','latitude','longitude','altitude','samplingHeight','size', 
                'nRows','dataLevel','obsStart','obsStop','productionTime','accessUrl','fileName','dClass','dataSetLabel'] 
        '''
        
        '''
        pidMetadata = metadata.get("https://meta.icos-cp.eu/objects/"+pid)
        if pidMetadata is not None:
            bDataSuccessfullyRead=True
            isICOS=False
            if(pidMetadata['specification'].get('keywords', None) is not None):
                isICOS = any('ICOS' in sKwrd for sKwrd in pidMetadata['specification'].get('keywords', None))
            if(not isICOS):
                if(pidMetadata['references'].get('keywords', None) is not None):
                    isICOS = any('ICOS' in sKwrd for sKwrd in pidMetadata['references'].get('keywords', None))
            if(isICOS==False):
                logger.debug(f"This pidMetadata does not say that it is ICOS: {pid}")
            try:
                data=[pid, bSelected,  pidMetadata['specificInfo']['acquisition']['station']['id'], 
                            pidMetadata['specificInfo']['acquisition']['station']['countryCode'],
                            isICOS, 
                            pidMetadata['coverageGeo']['geometry']['coordinates'][1], 
                            pidMetadata['coverageGeo']['geometry']['coordinates'][0], 
                            pidMetadata['coverageGeo']['geometry']['coordinates'][2],
                            pidMetadata['specificInfo']['acquisition']['samplingHeight'], 
                            pidMetadata['size'], 
                            pidMetadata['specificInfo']['nRows'], 
                            pidMetadata['specification']['dataLevel'], 
                            pidMetadata['specificInfo']['acquisition']['interval']['start'],
                            pidMetadata['specificInfo']['acquisition']['interval']['stop'],
                            pidMetadata['specificInfo']['productionInfo']['dateTime'], 
                            pidMetadata['accessUrl'],
                            pidMetadata['fileName'], int(0), 
                            pidMetadata['specification']['self']['label']],
                            pidMetadata['specificInfo']['acquisition']['station']['org']['name']
            except:
                bDataSuccessfullyRead=False
                badPids.append(pid)
                nBadDataSets+=1
                logger.error(f'ERROR: pid={pid} failed to read correctly,  most likely due to issues in the metadata.')
            '''
        if(bDataSuccessfullyRead):
            data=[pid, bSelected]+mdata
            if(i==0):
                '''
                stationID=pidMetadata['specificInfo']['acquisition']['station']['id']
                country=pidMetadata['specificInfo']['acquisition']['station']['countryCode']
                ICOSclass: keyWrds (any 'ICOS' 'CO2'/'CH4'   in pidMetadata['specification']['keywords'] / pidMetadata['references']['keywords'] (List)  
                # Tracer: keyWrds (any  'CO2'/'CH4'   in pidMetadata['specification']['keywords'] / pidMetadata['references']['keywords'] (List)  
                lon=pidMetadata['coverageGeo']['geometry']['coordinates'][0]
                lat=pidMetadata['coverageGeo']['geometry']['coordinates'][1]
                alt=pidMetadata['coverageGeo']['geometry']['coordinates'][2]
                samplingHeight = pidMetadata['specificInfo']['acquisition']['samplingHeight']
                size=pidMetadata['size']   
                nRows=pidMetadata['specificInfo']['nRows']
                dataLevel=pidMetadata['specification']['dataLevel']
                obsStart=pidMetadata['specificInfo']['acquisition']['interval']['start']
                obsStop=pidMetadata['specificInfo']['acquisition']['interval']['stop']
                  #tmporalCoverage=pidMetadata['references']['temporalCoverageDisplay']
                productionTime=pidMetadata['specificInfo']['productionInfo']['dateTime']
                sUrl=pidMetadata['accessUrl']
                fileName=pidMetadata['fileName']
                dataSetLabel=pidMetadata['specification']['self']['label']  
                stationName=
                '''
                columnNames=['pid', 'selected','stationID', 'country', 'IcosClass','latitude','longitude','altitude','samplingHeight','size', 
                        'nRows','dataLevel','obsStart','obsStop','productionTime','accessUrl','fileName','dClass','dataSetLabel', 'stationName'] 
                if(len(columnNames)==len(data)):
                    df=DataFrame(data=[data], columns=columnNames)
                else:
                    logger.error(f'len(df.columns)={len(df.columns)} does not match len(data)={len(data)}')
            else:
                #data=[pid, pidMetadata['specificInfo']['acquisition']['station']['id'], pidMetadata['coverageGeo']['geometry']['coordinates'][0], pidMetadata['coverageGeo']['geometry']['coordinates'][1], pidMetadata['coverageGeo']['geometry']['coordinates'][2], pidMetadata['specificInfo']['acquisition']['samplingHeight'], pidMetadata['size'], pidMetadata['specification']['dataLevel'], pidMetadata['references']['temporalCoverageDisplay'], pidMetadata['specificInfo']['productionInfo']['dateTime'], pidMetadata['accessUrl'], pidMetadata['fileName'], int(0), pidMetadata['specification']['self']['label']]
                if(len(df.columns)==len(data)):
                    df.loc[len(df)] = data
                else:
                    badPids.append(pid)
                    nBadDataSets+=1
                    logger.error(f"Rejected: Corrupted data set: carbon portal data record {url} is missing critical meta data and cannot be used.")
            i+=1
        else:
            badPids.append(pid)
            nBadDataSets+=1
            logger.error(f'Rejected: Obtaining the metadata for data object {url} was unsuccessful. Thus this data set cannot be used.')

        if((printProgress) and (n % step ==0)):
            myCarbonPortalTools.printProgressBar(n, nObj, prefix = 'Gathering meta data progress:', suffix = 'Done', length = 50)
    
    #df['dClass'] = int(0) # initialise unknown data quality
    #df['dClass'] = np.where(df.dataSetLabel.str.contains("Obspack", flags=re.IGNORECASE), int(4), int(0))
    #df['dClass'] = np.where(df.dataSetLabel.str.contains("Release", flags=re.IGNORECASE), int(3), df['dClass'] )
    #df['dClass'] = np.where(df.dataSetLabel.str.contains("product", flags=re.IGNORECASE), int(2), df['dClass'] )
    #df['dClass'] = np.where(df.dataSetLabel.str.contains("NRT "), int(1), df['dClass'] )
    myCarbonPortalTools.printProgressBar(nObj, nObj, prefix = 'Gathering meta data progress:', suffix = 'Done', length = 50)
    #sTmpPrfx=ymlContents[ 'run']['thisRun']['uniqueTmpPrefix'] 
    #df.to_csv(sTmpPrfx+'_dbg_dfValidObsUnsorted.csv', mode='w', sep=',')
    if(nBadDataSets > 0):
        logger.warning(f'{nBadDataSets} of {i} data records failed to read correctly and had to be discarded.')
    sTmpPrfx=ymlContents[ 'run']['thisRun']['uniqueTmpPrefix']
    df.to_csv(sTmpPrfx+'_dbg_collectedObs.csv', mode='w', sep=',')
    dfCountStations=df.drop_duplicates(['stationID'], keep='first') 
    nObsDataRecords = len(df)
    nTotalStations=len(dfCountStations)
    
    # Only include observations made within the selected geographical region
    Lat0=ymlContents['run']['region']['lat0']
    Lat1=ymlContents['run']['region']['lat1'] 
    Lon0=ymlContents['run']['region']['lon0']
    Lon1=ymlContents['run']['region']['lon1']
    filtered = ((df['latitude'] >= Lat0) &
           (df['latitude'] <= Lat1) & 
           (df['longitude'] >= Lon0) & 
           (df['longitude'] <= Lon1))

    dfq= df[filtered]

    dfCountStations2=dfq.drop_duplicates(['stationID'], keep='first') 
    nObsDataRecords2 = len(dfq)
    nTotalStations2=len(dfCountStations2)
    nRemovedStations=nTotalStations - nTotalStations2
    ndiff=nObsDataRecords - nObsDataRecords2
    logger.info(f"{nTotalStations} observation sites found. Thereof {nRemovedStations} fall outside the geographical region selected.")
    logger.info(f"{nObsDataRecords} observational data records found. Thereof {ndiff} fall outside the geographical region selected.")
    logger.info(f"{nTotalStations2} observation sites remaining. {nObsDataRecords2} valid observational data records remaining.")

    dfq.sort_values(by = ['country','stationID', 'dClass', 'samplingHeight', 'productionTime'], inplace = True, ascending = [True, True, False, False, False])
    #dfq.to_csv(sTmpPrfx+'_dbg_dfValidObs.csv', mode='w', sep=',')
    dfqdd=dfq.drop_duplicates(['stationID', 'dClass', 'samplingHeight'], keep='first')  # discards older  'productionTime' datasets
    logger.info("Dropping duplicates and deprecated data sets that have been replaced with newer versions.")
    # But we are still keeping all sampling heights.
    sOutputPrfx=ymlContents[ 'run']['thisRun']['uniqueOutputPrefix']
    tracer=hk.getTracer(ymlContents['run']['tracers'])
    fDiscoveredObservations=sOutputPrfx+"DiscoveredObservations-"+tracer+".csv"
    nObsDataRecords2 = len(dfqdd)
    logger.info(f"{nObsDataRecords2} valid observational data records remaining from {nTotalStations2} stations across Europe.")
    dfqdd.to_csv(fDiscoveredObservations, mode='w', sep=',')
    selectedDobjCol=dfqdd['pid']
    selectedDobjLst = selectedDobjCol.iloc[1:].tolist()
    logger.warning(f'badPIDs= {badPids}')
    return(finalDobjLst, selectedDobjLst, dfqdd, fDiscoveredObservations, badPids)



# *****************************************************************************************************************************
def chooseAmongDiscoveredObservations(bWithGui=True, tracer='CO2', ValidObs=None, ymlFile=None, fDiscoveredObservations=None, 
                                                                            sNow='', selectedObsFile='',   bSkipGui=False,  iVerbosityLv=1):
# *****************************************************************************************************************************

    # Shall we call the GUI to tweak some parameters before we start the ball rolling?
    if bWithGui:
        #(updatedYmlContents) = callLumiaGUI(ymlContents, sLogCfgPath)
        # callLumiaGUI(rcf, args.start,  args.end )
        script_directory = os.path.dirname(os.path.abspath(sys.argv[0]))
        sCmd ='python3 '+script_directory+'/lumia/GUI/lumiaGUI.py --step2 --DiscoveredObs='+fDiscoveredObservations
        if (len(sNow)>16):  # LumiaDA-2024-01-20T00_52
            sNow=sNow[:16]
        sCmd+=' --sNow='+sNow
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
            logger.error("The user canceled the call of Lumia or something went wrong in the Refinement GUI. Execution aborted. Lumia was not called.")
            sys.exit(42)
    ValidObs.read_csv(selectedObsFile)
    # Read the ymlFile
    # Apply all filters found in the ymlFile

    # Write the resulting list of chosen obsDataSetgs to "ObservationsSelected4Lumia.csv"
    # chosenObs=ValidObs.where(ValidObs['selected']==True)
    chosen = (ValidObs['selected']==True)
    chosenObs= ValidObs[chosen]
    
    selectedDobjCol=chosenObs['pid']
    selectedDobjLst = selectedDobjCol.iloc[1:].tolist()
    return(selectedDobjLst, chosenObs)


