#!/usr/bin/env python

import os
import sys
import argparse
import time
import platform
import traceback
import _thread
from loguru import logger
from pandas import DataFrame,  read_csv, concat , to_datetime #,  timedelta , concat , read_csv, Timestamp, Timedelta, offsets  #, read_hdf, Series  #, read_hdf, Series
from pandas.api.types import is_float_dtype
import xarray as xr

from icoscp_core.icos import auth as iccAuth
from icoscp.cpauth.authentication import Authentication , init_auth      
from icoscp.cpb import metadata
from icoscp.cpb.dobj import Dobj
from icoscp_core.icos import meta as iccMeta
from icoscp_core.icos import data as iccData
from icoscp_core.icos import bootstrap as iccBootstrap

import myCarbonPortalTools
import housekeeping as hk
#from ipywidgets import widgets

scriptName=sys.argv[0]
import boringStuff as bs


def  getMetaDataFromPid_via_icosCore(pid, icosStationLut):
    mdata=[]
    ndr=0
    bAcceptable=True  
    url="https://meta.icos-cp.eu/objects/"+pid
    sid='   '
    try:
        # first try the low-level icscp-core library to query the metadata
        pidMetadataCore =  iccMeta.get_dobj_meta(url)
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
                #value=icosStationLut[sid]
                #d=value
                d=pidMetadataCore.specificInfo.acquisition.station.specificInfo.stationClass
            except:
                pass  # if the key does not exist, then it is not an ICOS station, hence d='no'
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
        try:
            d=pidMetadataCore.specificInfo.acquisition.station.name
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read full station name from metadata')
        mdata.append(d) # sFullStationName=dob.station['org']['name']
    except:
        print(f'Failed to get the metadata using icoscp_core.icos.iccMeta.get_dobj_meta(url) for url={url}')
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
                pidMetadata['specificInfo']['acquisition']['station']['specificInfo']['stationClass']
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
        try:
            d=pidMetadata['specificInfo']['acquisition']['station']['org']['name']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read full station name from metadata')
        mdata.append(d) # sFullStationName=dob.station['org']['name']
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
    nNoTempCov=0
    # icosStationLut=myCarbonPortalTools.createIcosStationLut()   
    #print(f'statClassLookup={statClassLookup}')
    #station_basic_meta_lookup = {s.uri: s for s in meta.list_stations()}
    #print(f'station_basic_meta_lookup={station_basic_meta_lookup}')
    nLst=len(pidLst)
    step=int((nLst/10.0)+0.5)
    bPrintProgress=False #True
    sStart='2018-01-01 00:00:00'
    sEnd='2018-02-01 23:59:59'
    pdSliceStartTime=bs.getPdTime(sStart) #,  tzUT=True)
    pdSliceEndTime=bs.getPdTime(sEnd) #,  tzUT=True)
    bFirstDf=True
    CrudeErrorEstimate="1.5" # 1.5 ppm
    #errorEstimate=CrudeErrorEstimate
    nDataSets=0
    #printonce=True
    #noTemporalCoverageLst=[]
    for pid in pidLst:
        fileOk='failed'
        badDataSet=False
        fNamePid='data record not found'
        metaDataRetrieved=True
        datafileFound=False
        icosMetaOk=False
        url="https://meta.icos-cp.eu/objects/"+pid
        (mdata, icosMetaOk)=myCarbonPortalTools.getMetaDataFromPid_via_icoscp_core(pid)
        if(mdata is None):
            logger.error(f'Failed: Obtaining the metadata for data object {url} was unsuccessful. Thus this data set cannot be used.')
        if(mdata is None):
            metaDataRetrieved=False
        if(icosMetaOk):
            icoscpMetaOk='   yes'
        else:
            icoscpMetaOk='    no'
        bAllImportantColsArePresent=False
        fNamePid=myCarbonPortalTools.getPidFname(pid)
        fileOk='   yes'
        datafileFound=True
        if(fNamePid is None):
            fileOk='    no'
            datafileFound=False
        data=[pid,icoscpMetaOk,  fileOk, fNamePid]+mdata
        if((datafileFound) and (metaDataRetrieved) and(icosMetaOk)):
            if(nGoodPIDs==0):
                '''
                returns a list of these objects: ['stationID', 'country', 'isICOS','latitude','longitude','altitude','samplingHeight','size', 
                        'nRows','dataLevel','obsStart','obsStop','productionTime','accessUrl','fileName','dClass','dataSetLabel','stationName'] 
                '''
                columnNames=['pid', 'icoscpMeta.ok', 'file.ok',  'fNamePid','stationID', \
                    'country','is-ICOS', 'latitude','longitude','altitude','samplingHeight','size', 'nRows','dataLevel',\
                    'obsStart','obsStop','productionTime','accessUrl','fileName','dClass','dataSetLabel', 'stationName'] 
                dfgood=DataFrame(data=[data], columns=columnNames)
            else:
                dfgood.loc[len(dfgood)] = data
            nGoodPIDs+=1
        else:
            if(nBadies==0): 
                columnNames=['pid', 'icoscpMeta.ok', 'file.ok',  'fNamePid','stationID', \
                    'country','is-ICOS', 'latitude','longitude','altitude','samplingHeight','size', 'nRows','dataLevel',\
                    'obsStart','obsStop','productionTime','accessUrl','fileName','dClass','dataSetLabel', 'stationName'] 
                dfbad=DataFrame(data=[data], columns=columnNames)
                print(f'Data records with some issues:\r\n{columnNames}')
            else:
                dfbad.loc[len(dfbad)] = data
            print(f'{data}')
            nBadies+=1
        nTotal+=1
        if((bPrintProgress) and (nTotal % step ==0)):
            myCarbonPortalTools.printProgressBar(nTotal, nLst, prefix = 'Gathering meta data progress:', suffix = 'Done', length = 50)
        try:
            pidUrl="https://meta.icos-cp.eu/objects/"+pid
            logger.info(f"pidUrl={pidUrl}")
            dobjMeta = iccMeta.get_dobj_meta(pidUrl)
            #logger.debug(f'dobjMeta={dobjMeta}')
            logger.info('dobjMeta data retrieved successfully.')
            dobj_arrays = iccData.get_columns_as_arrays(dobjMeta) 
            logger.info('dobj_arrays data retrieved successfully.')
            logger.debug(f'dobj_arrays={dobj_arrays}')
            obsData1site = DataFrame(dobj_arrays)
            obsData1site.iloc[:512, :].to_csv('_dbg_icc_'+pid+'-obsData1site-obsPortalDbL188.csv', mode='w', sep=',')  

            availColNames = list(obsData1site.columns.values)
            if((any('start_time' in entry for entry in availColNames)) and (any('time' in entry for entry in availColNames))):
                obsData1site.drop(columns=['time'], inplace=True)   #  rename(columns={'time':'halftime'}, inplace=True)
            if(any('start_time' in entry for entry in availColNames)):
                if(any('TIMESTAMP' in entry for entry in availColNames)):
                    obsData1site.drop(columns=['TIMESTAMP'], inplace=True) # rename(columns={'TIMESTAMP':'timeStmp'}, inplace=True)
                obsData1site.rename(columns={'start_time':'time'}, inplace=True)
            if (any('value' in entry for entry in availColNames)) :
                    obsData1site.rename(columns={'value':'obs'}, inplace=True)
            if (any('value_std_dev' in entry for entry in availColNames)) :
                    obsData1site.rename(columns={'value_std_dev':'stddev'}, inplace=True)
            if (any('nvalue' in entry for entry in availColNames)) :
                    obsData1site.rename(columns={'nvalue':'NbPoints'}, inplace=True)
            if (any('TIMESTAMP' in entry for entry in availColNames)) :
                    obsData1site.rename(columns={'TIMESTAMP':'time'}, inplace=True)
            if (any('Flag' in entry for entry in availColNames)):
                obsData1site.rename(columns={'Flag':'icos_flag'}, inplace=True)
            if (any('Stdev' in entry for entry in availColNames)):
                obsData1site.rename(columns={'Stdev':'stddev'}, inplace=True)
            if (any('co2' in entry for entry in availColNames)):
                obsData1site.rename(columns={'co2':'obs'}, inplace=True)
            availColNames = list(obsData1site.columns.values)    
            if ( (any('time' in entry for entry in availColNames))  and (any('obs' in entry for entry in availColNames)) and (any('stddev' in entry for entry in availColNames)) ): #  and (any('icos_flag' in entry for entry in availColNames))
                bAllImportantColsArePresent=True
                for col in [ 'icos_LTR','icos_SMR','icos_STTB','qc_flag','calendar_components','dim_concerns','datetime','time_components','solartime_components','instrument','icos_datalevel','obs_flag','assimilation_concerns']:
                   if (any(col in entry for entry in availColNames)): 
                        obsData1site.drop(columns=[col], inplace=True)
            try:
                dob = Dobj(pidUrl)
                # logger.debug(f'dob={dob}')
                # obsData1site = dob.get()   # much less reliable than icoscp_core routines
                SiteID = dob.station['id'].lower() # site name/code is in capitals, but needs conversion to lower case:
                #fn=dob.info['fileName']
                fLatitude=dob.lat
                fLongitude=dob.lon
                fStationAltitude=dob.alt
                fSamplingHeight=dob.meta['specificInfo']['acquisition']['samplingHeight']
                sAccessUrl=dob.meta['accessUrl'] 
                sFileNameOnCarbonPortal = dob.meta['fileName']
                sFullStationName=dob.station['org']['name']
            except:
                SiteID = mdata[0].lower()
                fLatitude=mdata[3]
                fLongitude=mdata[4]
                fStationAltitude=mdata[5]
                fSamplingHeight=mdata[6]
                sAccessUrl=mdata[13]
                sFileNameOnCarbonPortal = mdata[14]
                sFullStationName=mdata[17]
            bHaveDataForSelectedPeriod=True
            if(bAllImportantColsArePresent==True):
                # grab only the time interval needed. This reduces the amount of data drastically if it is an obspack.
                # Extract data between two dates
                availColNames = list(obsData1site.columns.values)
                #if('T20l411eiXcWzSrTbbit9aCC' in pid):  # has a data gap from 2017-07-05 to 2018-02-20
                #    obsData1site.to_csv('_dbg_icc_'+pid+'-obsData1site-allTimes.csv', mode='w', sep=',')  
                if(any('NbPoints' in entry for entry in availColNames)):
                    obsData1siteTimed =obsData1site.loc[
                                                            (obsData1site['time'] >= pdSliceStartTime) & 
                                                            (obsData1site['time'] < pdSliceEndTime) &  
                                                            (obsData1site['NbPoints'] > 0)]
                else:
                    obsData1siteTimed =obsData1site[
                                                            (obsData1site['time'] >= pdSliceStartTime) & 
                                                            (obsData1site['time'] < pdSliceEndTime) ].copy()
                    #obsData1siteTimed =obsData1site.loc[
                    #                                        (obsData1site['time'] >= pdSliceStartTime) & 
                    #                                        (obsData1site['time'] < pdSliceEndTime) ]
                if(obsData1siteTimed.empty == True):
                    logger.warning(f"ObsData for SiteID={SiteID} pidUrl={pidUrl} has no data during the selected time period.")
                    bHaveDataForSelectedPeriod=False
                    ntData=[pid, pidUrl,  SiteID, sFullStationName, sStart, sEnd]
                    if(nNoTempCov==0): 
                        columnNames=['pid', 'pidUrl','stationID','stationName', 'Requested_StartDate', 'EndDate'] 
                        dfNoTempCov=DataFrame(data=[ntData], columns=columnNames)
                    else:
                        dfNoTempCov.loc[len(dfNoTempCov)] = ntData
                    nNoTempCov+=1
                else:
                    logger.info(f"obsData1siteTimed= {obsData1siteTimed}")
                    
                #obsData1siteTimed.iloc[:786, :].to_csv('_dbg_icc_'+pid+'-obsData1siteTimed-obsPortalDbL310.csv', mode='w', sep=',')  
                
                # obsData1siteTimed.to_csv(sTmpPrfx+'_dbg_obsData1siteTimed-obsPortalDbL310.csv', mode='w', sep=',')  
                
                # TODO: one might argue that 'err' should be named 'err_obs' straight away, but in the case of using a local
                # observations.tar.gz file, that is not the case and while e.g.uncertainties are being set up, the name of 'err' is assumed 
                # for the name of the column  containing the observational error in that dataframe and is only being renamed later.
                # Hence I decided to mimic the behaviour of a local observations.tar.gz file
                # Parameters like site-code, latitude, longitude, elevation, and sampling height are not present as data columns, but can be 
                # extracted from the header, which really is written as a comment. Thus the following columns need to be created: 
                if(bHaveDataForSelectedPeriod):
                    logger.debug('Adding columns for site, lat, lon, alt, sHeight')
                    # obsData1siteTimed.loc[:,'code'] = dob.station['id'].lower()
                    if ('site' not in availColNames):
                        obsData1siteTimed.loc[:,'site'] = SiteID
                    #fn=dob.info['fileName']
                    obsData1siteTimed.loc[:,'lat']=fLatitude
                    obsData1siteTimed.loc[:,'lon']=fLongitude
                    obsData1siteTimed.loc[:,'alt']=fStationAltitude
                    obsData1siteTimed.loc[:,'height']=fSamplingHeight
                    logger.info(f"Observational data for chosen tracer read successfully: PID={pid} station={SiteID},  StationName={sFullStationName}, located at station latitude={fLatitude},  longitude={fLongitude},  stationAltitude={fStationAltitude},  samplingHeight={fSamplingHeight}")
                    # and the Time format has to change from "2018-01-02 15:00:00" to "20180102150000"
                    # Note that the ['TIMESTAMP'] column is a pandas.series at this stage, not a Timestamp nor a string
                    # I tried to pull my hair out converting the series into a timestamp object or similar and format the output,
                    # but that is not necessary. When reading a local tar file with all observations, it is also a pandas series object, 
                    # (not a timestamp type variable) and since I'm reading the data here directly into the dataframe, and not 
                    # elsewhere, no further changes are required.
                    logger.info(f"obsData1siteTimed2= {obsData1siteTimed}")
                    obsData1siteTimed.iloc[:786, :].to_csv('_dbg_icc_'+pid+'-obsData1siteTimed2-obsPortalDbL188.csv', mode='w', sep=',')  
                    logger.debug(f"columns present:{obsData1siteTimed.columns}")
                    availColNames = list(obsData1siteTimed.columns.values)
                    if ('obs' in availColNames):
                        if(bFirstDf):
                            allObsDfs= obsData1siteTimed.copy()
                        else:
                            allObsDfs= concat([allObsDfs, obsData1siteTimed])
                        # Now let's create the list of sites and store it in self....
                        # The example I have from the observations.tar.gz files looks like this:
                        # site,code,name,lat,lon,alt,height,mobile,file,sitecode_CSR,err
                        # trn,trn,Trainou,47.9647,2.1125,131.0,180.0,,/proj/inversion/LUMIA/observations/eurocom2018/rona/TRN_180m_air.hdf.all.COMBI_Drought2018_20190522.co2,dtTR4i,1.5
                        # sFileNameOnCarbonPortal = ICOS_ATC_L2_L2-2022.1_TOH_147.0_CTS_CO2.zip
                        logger.info(f"station ID      : {SiteID}")
                        #logger.info(f"PID             : {pid}")
                        logger.info(f"file name       : {sFileNameOnCarbonPortal}")
                        logger.info(f"access url      : {sAccessUrl}")
                        logger.info(f"filePath        : {fNamePid}")
                        mobileFlag=None
                        #scCSR=getSitecodeCsr(dob.station['id'].lower())
                        #logger.info(f"sitecode_CSR: {scCSR}")
                        # 'optimize.observations.uncertainty.type' key to 'dyn' (setup_uncertainties in ui/main_functions.py, ~l174)                    
                        data =( {
                          "site":SiteID.lower() ,
                          "name": sFullStationName,
                          "lat": fLatitude,
                          "lon":fLongitude ,
                          "alt": fStationAltitude,
                          "height": fSamplingHeight,
                          "mobile": mobileFlag,
                          "pid":pid, 
                          "dataObject": pidUrl ,
                          "fnameUrl": sAccessUrl,
                          "fileName":sFileNameOnCarbonPortal, 
                          "filePath": fNamePid
                        })
                        #  "sitecode_CSR": scCSR,
                        df = DataFrame([data])                    
                        #if(bFirstDf):
                        #    df.to_csv('mySites.csv', encoding='utf-8', sep=',', mode='w')
                        #else:
                        #    df.to_csv('mySites.csv', encoding='utf-8', sep=',', mode='a', header=False)
                        if(bFirstDf):
                            allSitesDfs = df.copy()
                            bFirstDf=False
                        else:
                            allSitesDfs = concat([allSitesDfs, df])
                        nDataSets+=1
                    else:
                        logger.error("Houston we have a problem. This datafame has no column by the name of >>obs<<. Please check (all) your dry mole fraction observational files for the selected tracer.")
                        logger.info(f"Available columns in the file with pid= {pid} are: {availColNames}")
                        logger.error(f"Data from pid={pid} is discarded.")
                else:
                    badDataSet=True
        except:
            logger.error(f"Error: reading of the Dobj failed for pidUrl={pidUrl}.")
            traceback.print_exc()
            badDataSet=True
    if(badDataSet):
        nBadies+=1
        #badiesLst.append(pidUrl)
    if(nNoTempCov > 0):
        # noTemporalCoverageLst
        logger.warning(f"A total of {nNoTempCov} PIDs out of {nTotal} data objects had no temporal overlap with the time period requested.")
        dfNoTempCov.to_csv(sOutputPrfx+'noTempCov-obsDataSetsWithInsufficientTemporalCoverage.csv', encoding='utf-8', mode='w', sep=',')
        logger.info(f'PIDs with insufficient temporal coverage (no data) have been written to {sOutputPrfx}noTimCov-obsDataSetsWithInsufficientTemporalCoverage.csv')
    if(nBadies > 0):
        logger.warning(f"A total of {nBadies} PIDs out of {nTotal} data objects had some issues,  thereof {nBadMeta} had bad dobjMetaData and {nBadIcoscpMeta} had bad icoscMetaData.")
        dfbad.to_csv(sOutputPrfx+'bad-PIDs-testresult.csv', encoding='utf-8', mode='w', sep=',')
        #badiesLst.to_csv(sOutputPrfx+'badiesLst-obsDataSetsThatCouldNotBeRead.csv', encoding='utf-8', mode='w', sep=',')
        logger.info(f'Bad PIDs with some issues have been written to {sOutputPrfx}bad-PIDs-testresult.csv')
    if(nGoodPIDs > 0):
        allSitesDfs.to_csv('_dbg_icc_successfullyReadObsDataSets.csv', mode='w', sep=',')  
        dfgood.to_csv(sOutputPrfx+'good-PIDs-testresult.csv', encoding='utf-8', mode='w', sep=',')
        logger.info(f'Good PIDs ()with all queried properties found) have been written to {sOutputPrfx}good-PIDs-testresult.csv')
        # co2 dry mole frac obs data is reported at the middle of the time step, while Lumia expects the start
        # of the time step to be listed. Hence we need to shift the time axis by 30 minutes
        allObsDfs.rename(columns={'time':'timeMOTS'}, inplace=True)  # time@middle of time step
        allObsDfs['time']=allObsDfs['timeMOTS'].transform(lambda x: x.replace(minute=0, second=0))
        allObsDfs.to_csv('_dbg_icc_-allSitesTimedObsDfs.csv', mode='w', sep=',')  
        allObsDfs.drop(columns=['timeMOTS'], inplace=True) 

def readLstOfPids(pidFile):
    with open(pidFile) as file:
       selectedDobjLst = [line.rstrip() for line in file]
    # read the observational data from all the files in the dobjLst. These are of type ICOS ATC time series
    if((selectedDobjLst is None) or (len(selectedDobjLst)<1)):
        logger.error(f"Fatal Error! User specified pidFile {pidFile} is empty.")
        sys.exit(-1)
    return(selectedDobjLst)


def verifyCpauth():
    # cpauth
    # https://icos-carbon-portal.github.io/pylib/modules/#authentication
    from icoscp.cpauth.authentication import Authentication , init_auth      
    from icoscp.cpb.dobj import Dobj    
    needNewCpauth=False
    try:
        dobj = Dobj('11676/pDBSKn2D8ic5ttUCpxyaf2pj')
    except:
        needNewCpauth=True   
        logger.debug('We need a new authentication token for cpauth.icos-cp.eu (1)') 
    if(dobj is None) :
        needNewCpauth=True
        logger.debug('We need a new authentication token for cpauth.icos-cp.eu (2)') 
    else:
        rtnValue=init_auth()
        logger.debug(f'icoscp.cpauth.authentication.init_auth() returned {rtnValue}')
        logger.debug(f'Successfully read test data object from carbn portal dobj.data.head={dobj.data.head()}')
        return(True)
    if(needNewCpauth):
        cp_auth = Authentication()
        logger.debug(f'icoscp.cpauth.authentication.Authentication returned {cp_auth}')
    try:
        dobj = Dobj('11676/pDBSKn2D8ic5ttUCpxyaf2pj')
    except:
        logger.debug('We still need a new authentication token for cpauth.icos-cp.eu. Trying init_auth() instead') 
        rtnValue=init_auth()
        logger.debug(f'icoscp.cpauth.authentication.init_auth() returned {rtnValue}')
    if(dobj is None) :
        logger.debug('We still need a new authentication token for cpauth.icos-cp.eu. Trying init_auth() instead (2)') 
        rtnValue=init_auth()
        logger.debug(f'icoscp.cpauth.authentication.init_auth() returned {rtnValue}')
    else:
        logger.debug(f'Successfully read test data object from carbn portal dobj.data.head={dobj.data.head()}')
        return(True)
    logger.error('Abort. Unable to obtain a valid user authentication against the carbon portal. Please visit https://icos-carbon-portal.github.io/pylib/modules/#authentication for more information.')
    return(False)
 

def verifyCpCoreAuth():
    # cpauth
    # https://github.com/ICOS-Carbon-Portal/data/tree/master/src/main/python/icoscp_core#authentication
    
    needNewCpauth=False
    try:
        dobj = Dobj('11676/pDBSKn2D8ic5ttUCpxyaf2pj')
    except:
        needNewCpauth=True   
        logger.debug('We need a new authentication token for cpauth.icos-cp.eu (1)') 
    if(dobj is None) :
        needNewCpauth=True
        logger.debug('We need a new authentication token for cpauth.icos-cp.eu (2)') 
    else:
        rtnValue=init_auth()
        logger.debug(f'icoscp.cpauth.authentication.init_auth() returned {rtnValue}')
        logger.debug(f'Successfully read test data object from carbn portal dobj.data.head={dobj.data.head()}')
        return(True)
    if(needNewCpauth):
        cp_auth = iccAuth.init_config_file()
        logger.debug(f'icoscp.cpauth.authentication.Authentication returned {cp_auth}')
    try:
        dobj = Dobj('11676/pDBSKn2D8ic5ttUCpxyaf2pj')
    except:
        logger.debug('We still need a new authentication token for cpauth.icos-cp.eu. Trying init_auth() instead') 
        rtnValue=init_auth()
        logger.debug(f'icoscp.cpauth.authentication.init_auth() returned {rtnValue}')
    if(dobj is None) :
        logger.debug('We still need a new authentication token for cpauth.icos-cp.eu. Trying init_auth() instead (2)') 
        rtnValue=init_auth()
        logger.debug(f'icoscp.cpauth.authentication.init_auth() returned {rtnValue}')
    else:
        logger.debug(f'Successfully read test data object from carbn portal dobj.data.head={dobj.data.head()}')
        return(True)
    logger.error('Abort. Unable to obtain a valid user authentication against the carbon portal. Please visit https://icos-carbon-portal.github.io/pylib/modules/#authentication for more information.')
    return(False)
  
    
def runPidTest(pidFile, sOutputPrfx):
    #if (not verifyCpauth()):
    #    sys.exit(-1)
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

'''
2024-03-28 12:32:28.520 | ERROR    | lumia.obsdb.obsCPortalDb:gatherObs_fromCPortal:407 - Error: reading of the Dobj failed for pidUrl=https://meta.icos-cp.eu/objects/QeqT9ATwpxrCvd159Djt2eFr. 
2024-03-28 12:32:40.182 | ERROR    | lumia.obsdb.obsCPortalDb:gatherObs_fromCPortal:407 - Error: reading of the Dobj failed for pidUrl=https://meta.icos-cp.eu/objects/FPidg1GxfRN-xBbP-BBRGSqf.
2024-03-28 12:32:41.909 | ERROR    | lumia.obsdb.obsCPortalDb:gatherObs_fromCPortal:407 - Error: reading of the Dobj failed for pidUrl=https://meta.icos-cp.eu/objects/WqyL0AmwwqhaRLYCZuDHH1Ln.

'''

p = argparse.ArgumentParser()
p.add_argument('--pidLst', default=None,  help='Name of a text file listing the carbon portal PIDs (as a single column) that you wish to test.')
#p.add_argument('--fDiscoveredObs', dest='fDiscoveredObservations', default=None,  help="If step2 is set you must specify the .csv file that lists the observations discovered by LUMIA, typically named DiscoveredObservations.csv")   # yaml configuration file where the user plans his or her Lumia run: parameters, input files etc.
p.add_argument('--verbosity', '-v', dest='verbosity', default='INFO')
p.add_argument('--sOutputPrfx', '-o', default='', help='Output file names will be prefixed with your string -- your string may contain a path or a partial name or both')
args, unknown = p.parse_known_args(sys.argv[1:])

# Set the verbosity in the logger (loguru quirks ...)
logger.remove()
logger.add(sys.stderr, level=args.verbosity)

myMachine=platform.node()
myPlatformCore=platform.platform()  # Linux-5.15.0-89-generic-x86_64-with-glibc2.35
myPlatformFlavour=platform.version() #99-Ubuntu SMP Mon Oct 30 20:42:41 UTC 2023
print(f'node={myMachine}')
print(f'platform={myPlatformCore}')
print(f'platformFlavour={myPlatformFlavour}')
#testGui()
#sys.exit(0)

if(args.pidLst is None):
    #logger.error("testPID: Fatal error: no user configuration (yaml) file provided.")
    logger.error("testPID: Fatal error: no csv /PID file provided.")
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
