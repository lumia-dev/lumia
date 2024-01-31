from loguru import logger
import re
from icoscp.cpb import metadata
from icoscp_core.icos import meta as coreMeta


def createIcosStationLut():
    # Make a lookup table to know which stations are considered ICOS stations:
    #from icoscp_core.icos import meta
    #from icoscp_core.icos import meta as coreMeta
    from icoscp_core.sparql import as_string, as_uri
    statClassQuery = 'select * where{?station <http://meta.icos-cp.eu/ontologies/cpmeta/hasStationClass> ?class}'
    # The table lists the ICOS station classes as described here:
    # https://www.icos-cp.eu/about/join-icos/process-stations#toc-station-classes-class-1-class-2-associated-stations
    statClassLookup = {
        as_uri('station', row): as_string('class', row)
        for row in coreMeta.sparql_select(statClassQuery).bindings
        # for row in meta.sparql_select(stat_class_query).bindings
    }
    # is the station an ICOS station? values are {'1','2','A','no'} class 1, 2, or A(ssociated) or else 'no' meaning it is not an ICOS station
    icosStationLut={}
    for key, value in  statClassLookup.items():
        if('/resources/stations/AS_' in key):
            mkey=key[-3:]
            icosStationLut[mkey]= value[0]  # Associated becomes 'A' one-letter-code
    return(icosStationLut)


def  getMetaDataFromPid_via_icosCore(pid, icosStationLut,  bCoreLib=True):
    mdata=[]
    ndr=0
    bAcceptable=True  
    url="https://meta.icos-cp.eu/objects/"+pid
    sid='   '
    try:
        # first try the low-level icscp-core library to query the metadata
        pidMetadata =  coreMeta.get_dobj_meta(url) if (bCoreLib) else metadata.get(url)
        #returns a list of these objects: [ 
        # 'stationID', 'country', 'latitude','longitude','altitude','samplingHeight','is-ICOS','size', 'nRows','dataLevel',
        # 'obsStart','obsStop','productionTime','accessUrl','fileName','dataSetLabel'] 
        '''
        returns a list of these objects: ['stationID', 'country', 'isICOS','latitude','longitude','altitude','samplingHeight','size', 
                'nRows','dataLevel','obsStart','obsStop','productionTime','accessUrl','fileName','dClass','dataSetLabel'] 
        '''
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
        # 'stationID'
        try:
            d=pidMetadata['specificInfo']['acquisition']['station']['id']
            sid=d[:3]
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read stationid from metadata')
        mdata.append(d)
        # 'country'
        try:
            d=pidMetadata['specificInfo']['acquisition']['station']['countryCode']
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
            try:
                value=icosStationLut[sid]
                d=value
            except:
                pass
        except:
            ndr+=1
            logger.debug('Failed to read ICOS flag from metadata')
        mdata.append(d)
        # 'latitude'
        try:
            d=pidMetadata['specificInfo']['acquisition']['station']['location']['lat']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station latitude from metadata')
        mdata.append(d)
        # 'longitude'
        try:
            d=pidMetadata['specificInfo']['acquisition']['station']['location']['lon']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station longitude from metadata')
        mdata.append(d)
        # 'altitude'
        try:
            d=pidMetadata['specificInfo']['acquisition']['station']['location']['alt']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station altitude from metadata')
        mdata.append(d)
        # 'samplingHeight'
        try:
            d=pidMetadata['specificInfo']['acquisition']['samplingHeight']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station samplingHeight from metadata')
        mdata.append(d)
        # 'size'
        try:
            d=pidMetadata['size'] 
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read size from metadata')
        mdata.append(d)
        # 'nRows'
        try:
            d=pidMetadata['specificInfo']['nRows']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read nRows from metadata')
        mdata.append(d)
        # 'dataLevel'
        try:
            d=pidMetadata['specification']['dataLevel']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read dataLevel from metadata')
        mdata.append(d)
        # 'obsStart'
        try:
            d=pidMetadata['specificInfo']['acquisition']['interval']['start']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read acquisition start from metadata')
        mdata.append(d)
        # 'obsStop'
        try:
            d=pidMetadata['specificInfo']['acquisition']['interval']['stop']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read acquisition stop from metadata')
        mdata.append(d)
        # 'productionTime'
        try:
            d=pidMetadata['specificInfo']['productionInfo']['dateTime']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read productionInfo from metadata')
        mdata.append(d)
        # 'accessUrl'
        try:
            d=pidMetadata['accessUrl']
        except:
            d=''
            #ndr+=1  # we got here via the url, so no drama if we don't have a value we already know
        mdata.append(d)
        # 'fileName'
        try:
            d=pidMetadata['fileName']
        except:
            d=''
            #ndr+=1 # informativ but it is not being used
        mdata.append(d)
        # 'dataSetLabel'
        # 'dataSetLabel' and 'dClass'
        d2 = int(0) #  'dClass' initialise unknown data quality
        try:
            d=pidMetadata['specification']['self']['label']
            d2 = int(4) if (d.contains("Obspack", flags=re.IGNORECASE)) else d2
            d2 = int(3) if (d.contains("Release", flags=re.IGNORECASE)) else d2
            d2 = int(2) if (d.contains("product", flags=re.IGNORECASE)) else d2
            d2 = int(1) if (d.contains("NRT ")) else d2
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read data label from metadata')
        mdata.append(d2)
        mdata.append(d)
    except:
        print(f'Failed to get the metadata using icoscp.cpb.metadata.get(url) for url={url}')
        return(None,  False)  # Tell the calling parent routine that things went south...
    if(ndr>1):
        bAcceptable=False
    return (mdata, bAcceptable)

