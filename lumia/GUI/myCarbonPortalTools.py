from loguru import logger
import re
from icoscp.cpb import metadata
from icoscp_core.icos import meta as coreMeta


def createIcosStationLut():
    '''
    Function createIcosStationLut
    @return  icosStationLut LookupTable of the level of certification as an ICOS station. Stations not listed are not affiliated with ICOS
    @rtype dictionary with the 3-letter station codes (Atmospheric stations only) as keys and the ICOS class as a one-letter string. 
                    Possible values are ['1','2','A'] representing ICOS stations of class 1, 2, or Associated, respectively
    The LUT lists the ICOS station classes as described here:
    https://www.icos-cp.eu/about/join-icos/process-stations#toc-station-classes-class-1-class-2-associated-stations
    '''
    # Make a lookup table to know which stations are considered ICOS stations:
    from icoscp_core.sparql import as_string, as_uri
    statClassQuery = 'select * where{?station <http://meta.icos-cp.eu/ontologies/cpmeta/hasStationClass> ?class}'
    statClassLookup = {
        as_uri('station', row): as_string('class', row)
        for row in coreMeta.sparql_select(statClassQuery).bindings
    }
    # is the station an ICOS station? values are {'1','2','A','no'} class 1, 2, or A(ssociated) 
    icosStationLut={}
    for key, value in  statClassLookup.items():
        if('/resources/stations/AS_' in key):
            mkey=key[-3:]
            icosStationLut[mkey]= value[0]  # Associated becomes 'A' one-letter-code
    return(icosStationLut)


def  getMetaDataFromPid_via_icoscp_core(pid, icosStationLut):
    '''
    Function getMetaDataFromPid_via_icoscp_core

    @param pid persistent identifier of a data record held on the carbon portal 
    @type string (fixed length)
    @param icosStationLut LookupTable of the level of certification as an ICOS station. Stations not listed are not affiliated with ICOS
    @type dictionary with the 3-letter station codes (Atmospheric stations only) as keys and the ICOS class as a one-letter string. 
    @return a list of agreed values extracted from the metadata of the PID provided
                values are returned for these keys: ['stationID', 'country', 'isICOS','latitude','longitude','altitude','samplingHeight','size', 
                                        'nRows','dataLevel','obsStart','obsStop','productionTime','accessUrl','fileName','dClass','dataSetLabel'] 
    @rtype list of mostly strings and 2 integers
    '''
    mdata=[]
    ndr=0
    bCoreLib=True  # if set to True, the coreMeta.get_dobj_meta(url) from the icoscp_core.icos package is tried first
        # if it fails, metadata.get(url) from the icoscp.cpb package is used as a fallback. If set to False, the order is reversed.
    bAcceptable=True  
    url="https://meta.icos-cp.eu/objects/"+pid
    sid='   '
    try:
        # Try the first method, e.g. the the low-level icscp-core library to query the metadata
        pidMetadata =  coreMeta.get_dobj_meta(url) if (bCoreLib) else metadata.get(url)
    except:
        try:
            bCoreLib=not bCoreLib # try the fallback method
            pidMetadata =  coreMeta.get_dobj_meta(url) if (bCoreLib) else metadata.get(url)
        except:
            print(f'Failed to get the metadata using icoscp.cpb for url={url}')
            return(None,  False)  # Tell the calling parent routine that things went south...
        print(f'Failed to get the metadata using icoscp_core.icos.coreMeta.get_dobj_meta(url) for url={url}')
        return(None,  False)  # Tell the calling parent routine that things went south...

    # 'stationID'
    try:
        d=pidMetadata.specificInfo.acquisition.station.id  if (bCoreLib) else  pidMetadata['specificInfo']['acquisition']['station']['id']
        sid=d[:3]
    except:
        d=''
        ndr+=1
        logger.debug('Failed to read stationid from metadata')
    mdata.append(d)
    # 'country'
    try:
        d=pidMetadata.specificInfo.acquisition.station.countryCode  if (bCoreLib) \
            else  pidMetadata['specificInfo']['acquisition']['station']['countryCode']
    except:
        d=''
        ndr+=1
        logger.debug('Failed to read countryCode from metadata')
    mdata.append(d)
    # 'isICOS'
    try:
        #       projectUrl=pidMetadata.specification.project.self.uri 
        # is the station an ICOS station? values are {'1','2','A','no'} class 1, 2, or A(ssociated) or else 'no' meaning it is not an ICOS station
        d='no'
        value=icosStationLut[sid]  
        d=value
    except:
        ndr+=1
        #logger.debug(f'Station {sid} is not an ICOS station it seems, because it is not listed in the icosStationLut.')
    mdata.append(d)
    # 'latitude'
    try:
        d=pidMetadata.specificInfo.acquisition.station.location.lat  if (bCoreLib)\
            else  pidMetadata['specificInfo']['acquisition']['station']['location']['lat']
    except:
        d=''
        ndr+=1
        logger.debug('Failed to read station latitude from metadata')
    mdata.append(d)
    # 'longitude'
    try:
        d=pidMetadata.specificInfo.acquisition.station.location.lon  if (bCoreLib)\
            else  pidMetadata['specificInfo']['acquisition']['station']['location']['lon']
    except:
        d=''
        ndr+=1
        logger.debug('Failed to read station longitude from metadata')
    mdata.append(d)
    # 'altitude'
    try:
        d=pidMetadata.specificInfo.acquisition.station.location.alt  if (bCoreLib) \
            else  pidMetadata['specificInfo']['acquisition']['station']['location']['alt']
    except:
        d=''
        ndr+=1
        logger.debug('Failed to read station altitude from metadata')
    mdata.append(d)
    # 'samplingHeight'
    try:
        d=pidMetadata.specificInfo.acquisition.samplingHeight  if (bCoreLib) \
            else  pidMetadata['specificInfo']['acquisition']['samplingHeight']
    except:
        d=''
        ndr+=1
        logger.debug('Failed to read station samplingHeight from metadata')
    mdata.append(d)
    # 'size'
    try:
        d=pidMetadata.size  if (bCoreLib) \
            else  pidMetadata['size'] 
    except:
        d=''
        ndr+=1
        logger.debug('Failed to read size from metadata')
    mdata.append(d)
    # 'nRows'
    try:
        d=pidMetadata.specificInfo.nRows  if (bCoreLib) \
            else  pidMetadata['specificInfo']['nRows']
    except:
        d=''
        ndr+=1
        logger.debug('Failed to read nRows from metadata')
    mdata.append(d)
    dL=int(0) # often we may get a dataLevel of 2, 1, or 3 that we can use as a fallBack for retrieving dClass that relies on the data description
    try:
        n=int(d)
        if((n<4) and (n>0)):
            dL=n
    except:
        pass
    # 'dataLevel'
    try:
        d=pidMetadata.specification.dataLevel  if (bCoreLib) \
            else  pidMetadata['specification']['dataLevel']
    except:
        d=''
        ndr+=1
        logger.debug('Failed to read dataLevel from metadata')
    mdata.append(d)
    # 'obsStart'
    try:
        d=pidMetadata.specificInfo.acquisition.interval.start  if (bCoreLib) \
            else  pidMetadata['specificInfo']['acquisition']['interval']['start']
    except:
        d=''
        ndr+=1
        logger.debug('Failed to read acquisition start from metadata')
    mdata.append(d)
    # 'obsStop'
    try:
        d=pidMetadata.specificInfo.acquisition.interval.stop  if (bCoreLib) \
            else  pidMetadata['specificInfo']['acquisition']['interval']['stop']
    except:
        d=''
        ndr+=1
        logger.debug('Failed to read acquisition stop from metadata')
    mdata.append(d)
    # 'productionTime'
    try:
        d=pidMetadata.specificInfo.productionInfo.dateTime  if (bCoreLib) \
            else  pidMetadata['specificInfo']['productionInfo']['dateTime']
    except:
        d=''
        ndr+=1
        logger.debug('Failed to read productionInfo from metadata')
    mdata.append(d)
    # 'accessUrl'
    try:
        d=pidMetadata.accessUrl  if (bCoreLib) else  pidMetadata['accessUrl'] 
    except:
        d=''
        #ndr+=1  # we got here via the url, so no drama if we don't have a value we already know
    mdata.append(d)
    # 'fileName'
    try:
        d=pidMetadata.fileName  if (bCoreLib) else pidMetadata['fileName'] 
    except:
        d=''
        #ndr+=1 # informativ but it is not being used
    mdata.append(d)
    # 'dataSetLabel' and 'dClass'
    d2 = int(0) #  'dClass' initialise unknown data quality
    try:
        d=pidMetadata.specification.self.label  if (bCoreLib) else pidMetadata['specification']['self']['label']  
        d2 = int(4) if (re.search("Obspack", d, re.IGNORECASE)) else d2
        d2 = int(3) if ((d2==0) and (re.search("Release", d,  re.IGNORECASE))) else d2
        d2 = int(2) if ((d2==0) and (re.search("product", d,  re.IGNORECASE))) else d2
        d2 = int(1) if ((d2==0) and (re.search("NRT ",  d))) else d2
    except:
        d=''
        ndr+=1
        logger.debug('Failed to read data label from metadata')
    if(dL>d2):
        d2=dL  # dataLevel, if available, may be more reliable, but does not distinguish ObsPacks
    mdata.append(d2)
    mdata.append(d)

    if(ndr>1):
        bAcceptable=False
    return (mdata, bAcceptable)


# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()
