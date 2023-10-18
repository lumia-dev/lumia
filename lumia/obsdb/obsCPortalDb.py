#!/usr/bin/env python

import os
import sys
from datetime import timedelta
from numpy import zeros, sqrt
import rctools
import tarfile
import xarray as xr
from lumia.obsdb import obsdb
from lumia.formatters.cdoWrapper import ensureReportedTimeIsStartOfMeasurmentInterval
from multiprocessing import Pool
from loguru import logger
from typing import Union
import xarray as xr
from pandas import DataFrame, to_datetime, concat , read_csv, Timestamp, offsets  #, read_hdf, Series  #, read_hdf, Series
from pandas.api.types import is_float_dtype
# from rctools import RcFile
# from icoscp.cpb import metadata as meta
from icoscp.cpb.dobj import Dobj
from icosPortalAccess.readObservationsFromCarbonPortal import readObservationsFromCarbonPortal,  getSitecodeCsr


def _calc_weekly_uncertainty(site, times, err):
    err = zeros(len(times)) + err
    nobs = zeros((len(times)))
    for it, tt in enumerate(times):
        nobs[it] = sum((times >= tt-timedelta(days=3.5)) & (times < tt+timedelta(days=3.5)))
    err *= sqrt(nobs)
    return site, err


class obsdb(obsdb):

    @classmethod
    def from_CPortal(cls, rcf: Union[dict, rctools.RcFile], setup_uncertainties: bool = True, filekey : str = 'file') -> "obsdb":
        """
        Construct an observation database based on a rc-file and the carbon portal. The class does the following:
        - loads all tracer observation files from the carbon portal
        - rename fields as required
        - calculate uncertainties (according to how specified in the rc-file)

        Arguments:
            - rcf: rctools.RcFile object (or dict)
            - setupUncertainties (optional): determine if the uncertainties need to be computed
            - filename (optional): path of the file containing the obs (overrides the one given by filekey)
            - filekey (optional): name of the section containing the file path and relevant keys

        """
        db = cls(
            rcf['observations'][filekey]['path'], 
            start=rcf['observations'].get('start', None), 
            end=rcf['observations'].get('end', None), 
            bFromCPortal=True,  rcFile=rcf)
        # db.rcf = rcf

        # if (1>2):  # TODO: check. should not be needed
        # Rename fields, if required by the config file or dict:
        # the config file can have a key "observations.file.rename: col1:col2". In this case, the column "col1" will be renamed in "col2".
        # this can also be a list of columns: "observations.file.rename: [col1:col2, colX:colY]"
        
        # if not read from carbon portal, then this renaming happens later and 
        # thus should not be done here: lumia.obsdb.InversionDb:from_rc:51

        # If no "tracer" column in the observations file, it can also be provided through the rc-file (observations.file.tracer key)
        if "tracer" not in db.observations.columns:
            tracer = rcf['observations'][filekey]['tracer']
            logger.warning(f'No "tracer" column provided. Attributing all observations to tracer {tracer}')
            db.observations.loc[:, 'tracer'] = tracer
        return db
 
 
    def load_fromCPortal(self, rcf=None) -> "obsdb":
        """
        Public method  load_fromCPortal

        @param rcf name of the rc or yaml resource file. Usually taken from self (defaults to None)
        @type RCF (optional)
        
        Description: 1st step: Read all dry mole fraction files from all available records on the carbon portal
         - these obviously have no background co2 concentration values (which need to be provided by other means)
        """
        (self, obsDf,  obsFile)=self.gatherObs_fromCPortal(rcf)
        # all matching data was written to the obsFile
        
        # Read the file that has the background co2 concentrations
        bgFname=self.rcf['background']['concentrations']['co2']['backgroundCo2File']
        (bgDf, bInterpolationRequired)=self.load_background(bgFname)
        # Now we have to merge the background CO2 concentrations into the observational data...
        (mergedDf)=self.combineObsAndBgnd(obsDf,  bgDf, bInterpolationRequired)
        setattr(self,'observations', mergedDf)
        # filename="observations.tar.gz"
        self.filename = bgFname # filename
        logger.info("Observed and background concentrations of CO2 have been read and merged successfully.")
        # Then we should be able to continue as usual...i.e. like in the case of reading a prepared local obs+background data set.
        return(self)
        
    def gatherObs_fromCPortal(self, rcf=None,  errorEstimate=None) -> "obsdb":
        # TODO: ·errorEstimate: it may be better to set this to be calculated dynamically in the yml file by setting the
       # 'optimize.observations.uncertainty.type' key to 'dyn' (setup_uncertainties in ui/main_functions.py, ~L.174) 
       # The value actually matters, in some cases: it can be used as a value for the weekly uncertainty, or for the default 
       # single-obs uncertainty (in the "lumia.obsdb.InversionDb.obsdb.setup_uncertainties_cst" and 
       # "lumia.obsdb.InversionDb.obsdb.setup_uncertainties_weekly" methods). But it's not something you can retrieve 
       # from the observations themselves (it accounts for model uncertainty as well, to a degree), so it's more of a user settings. 
        self.rcf = rcf
        CrudeErrorEstimate="1.5" # 1.5 ppm
        timeStep=self.rcf['run']['timestep']
        # sLocation=rcf['observations']['file']['location']
        # if ('CARBONPORTAL' in sLocation):
        # We need to provide the contents of "observations.csv" and "sites.csv". "files.csv" is (always) empty - thus unimportant - and so is the data frame resulting from it.
        # bFromCarbonportal=True
        bFirstDf=True
        nDataSets=0
        # we attempt to locate and read the tracer observations directly from the carbon portal - given that this code is executed on the carbon portal itself
        # readObservationsFromCarbonPortal(sKeyword=None, tracer='CO2', pdTimeStart=None, pdTimeEnd=None, year=0,  sDataType=None,  iVerbosityLv=1)
        cpDir=rcf['observations']['file']['cpDir']
        #remapObsDict=rcf['observations']['file']['renameCpObs']
        #if self.start is None:
        sStart=self.rcf['observations']['start']    # should be a string like start: '2018-01-01 00:00:00'
        sEnd=self.rcf['observations']['end']
        # self.start = self.observations.time.min()
        # if self.end is None:
        # self.end = self.observations.time.max()
        pdTimeStart = to_datetime(sStart, format="%Y-%m-%d %H:%M:%S")
        pdTimeStart=pdTimeStart.tz_localize('UTC')
        pdTimeEnd = to_datetime(sEnd, format="%Y-%m-%d %H:%M:%S")
        pdTimeEnd=pdTimeEnd.tz_localize('UTC')
        # create a datetime64 version of these so we can extract the time interval needed from the pandas data frame
        pdSliceStartTime=pdTimeStart.to_datetime64()
        pdSliceEndTime=pdTimeEnd.to_datetime64()
        (dobjLst, selectedDobjLst, dfObsDataInfo,  cpDir)=readObservationsFromCarbonPortal(tracer='CO2',  cpDir=cpDir,  pdTimeStart=pdTimeStart, pdTimeEnd=pdTimeEnd, timeStep=timeStep,  sDataType=None,  iVerbosityLv=1)
        # read the observational data from all the files in the dobjLst. These are of type ICOS ATC time series
        if((selectedDobjLst is None) or (len(selectedDobjLst)<1)):
            logger.error("Fatal Error! ABORT! dobjLst is empty. We did not find any dry-mole-fraction tracer observations on the carbon portal. We need a human to fix this...")
            sys.exit(-1)
        nBadies=0
        # bWriteCsv=True
        bAllImportantColsArePresent=False
        for pid in selectedDobjLst:
            # sFileNameOnCarbonPortal = cpDir+pid+'.cpb'
            # meta.get('https://meta.icos-cp.eu/objects/Igzec8qneVWBDV1qFrlvaxJI')

            # TODO: remove next line· - for testing only
            # pid="6k8ll2WBSqYqznUbTaVLsJy9" # TRN 180m - same as in observations.tar.gz - for testing
            # mdata=meta.get("https://meta.icos-cp.eu/objects/"+pid)  # mdata is available as part of dob (dob.meta)
            pidUrl="https://meta.icos-cp.eu/objects/"+pid
            logger.info(f"pidUrl={pidUrl}")
            dob = Dobj(pidUrl)
            # logger.info(f"dobj: {dob}")
            logger.info(f"Reading observed co2 data from: PID={pid} station={dob.station['org']['name']}, located at station latitude={dob.lat},  longitude={dob.lon},  altitude={dob.alt},  elevation={dob.elevation}")
            obsData1site = dob.get()
            if('PXBNgmAH-PG5_AYXDl4fu2se'in pid):
                print('.')
            # logger.info(f"samplingHeight={dob.meta['specificInfo']['acquisition']['samplingHeight']}")
            availColNames = list(obsData1site.columns.values)
            if ('Site' not in availColNames):
                obsData1site.loc[:,'Site'] = dob.station['id'].lower()
            if ( (any('value' in entry for entry in availColNames))  and (any('value_std_dev' in entry for entry in availColNames)) and (any('time' in entry for entry in availColNames)) ):
                # There is an issue with the icoscp package when reading ObsPack data. Although the netcdf files stores seconds since 1970-01-01T00:00:00, 
                # what ends upin the pandas dataframe is only the hour of the day without date....
                # Let's read the netcdf file ourselves then... for each PID there are 2 files, one without extension (the netcdf file) and one with .cpb extension
                # example for an ObsPack Lv 2 data set: fn='/data/dataAppStorage/netcdfTimeSeries/bALaWpparMpY6_N-zW2Cia9a'
                datafileFound=False
                fn='/data/dataAppStorage/netcdfTimeSeries/'+pid
                if(os.path.exists(fn)):
                    datafileFound=True
                else:
                    fn='/data/dataAppStorage/asciiAtcProductTimeSer/'+pid
                    if(os.path.exists(fn)):
                        datafileFound=True
                    else:
                        fn='/data/dataAppStorage/asciiAtcTimeSer/'+pid
                        if(os.path.exists(fn)):
                            datafileFound=True
                if(datafileFound==False):
                    logger.warning(f"Suspicious data object {pidUrl}  :")
                    logger.warning("This data set might be an ObsPack, but I cannot locate the file on the server.")
                    # Remove this pid from the list of used data sets: pid in selectedDobjLst                
                    selectedDobjLst[:] = [x for x in selectedDobjLst if pid not in x]
                    nBadies+=1 
                else:
                    ds = xr.open_dataset(fn)
                    df = ds.to_dataframe()
                    df = df.reset_index()
                    #if(bWriteCsv==True):
                    #    obsData1site.to_csv('obsData1site.csv',index=True, header=True)
                    #    bWriteCsv=False
                    obsData1site=df
                    obsData1site.rename(columns={'value':'obs','value_std_dev':'stddev', 'nvalue':'NbPoints'}, inplace=True)
                    if(is_float_dtype(obsData1site['obs'])==False):
                        obsData1site['obs']=obsData1site['obs'].astype(float)
                    if(is_float_dtype(obsData1site['stddev'])==False):
                        obsData1site['stddev']=obsData1site['stddev'].astype(float)
                    # this pid is probably an ObsPack in netcdf format. co2 values are then in mol/mol and must be multiplied by 1e6 to get ppm
                    obsData1site['obs']= 1.0e6*obsData1site['obs']  # from mol/mol to micromol/mol
                    obsData1site['stddev']= 1.0e6*obsData1site['stddev']
                    if ( 'qc_flag'  in entry for entry in availColNames):
                        obsData1site.rename(columns={'qc_flag':'icos_flag'}, inplace=True)
                    else:
                        obsData1site['icos_flag']= 'O'
                    bAllImportantColsArePresent=True
            #elif ( ('TIMESTAMP' not in entry for entry in availColNames)  or ('co2' not in entry for entry in availColNames) or ('Stdev' not in entry for entry in availColNames) or ('Flag' not in entry for entry in availColNames)):
            elif ( (any('TIMESTAMP' in entry for entry in availColNames))  and (any('co2' in entry for entry in availColNames)) and (any('Stdev' in entry for entry in availColNames))   and (any('Flag' in entry for entry in availColNames))):
                # We rename first and then replace the values AFTER extracting the time slice - should be faster. Often the object is much smaller
                obsData1site.rename(columns={'TIMESTAMP':'time','Site':'code','co2':'obs','Stdev':'stddev','Flag':'icos_flag'}, inplace=True)
                bAllImportantColsArePresent=True
            else:
                logger.warning(f"Suspicious data object {pidUrl}  :")
                logger.warning("This data set is missing at least one of the expected columns >> time/TIMESTAMP, obs/co2/value, stdev/value_std_dev or Flag <<. This data object will be ignored.")
                # Remove this pid from the list of used data sets: pid in selectedDobjLst                
                selectedDobjLst[:] = [x for x in selectedDobjLst if pid not in x]
                # TODO should also remove entry from dataframe dfObsDataInfo and write an updated copy to csv file.
                nBadies+=1  # Have: icos_LTR, icos_SMR, icos_STTB, icos_datalevel, qc_flag, time, value, value_std_dev, Site
            if(bAllImportantColsArePresent==True):
                # grab only the time interval needed. This reduces the amount of data drastically if it is an obspack.
                obsData1siteTimed = obsData1site.loc[(
                    (obsData1site.time >= pdSliceStartTime) &
                    (obsData1site.time <= pdSliceEndTime) &
                    (obsData1site['NbPoints'] > 0)
                )]  
                # TODO: one might argue that 'err' should be named 'err_obs' straight away, but in the case of using a local
                # observations.tar.gz file, that is not the case and while e.g.uncertainties are being set up, the name of 'err' is assumed 
                # for the name of the column  containing the observational error in that dataframe and is only being renamed later.
                # Hence I decided to mimic the behaviour of a local observations.tar.gz file
                # However, that said, invdb.setupUncertainties() later in the game replaces the contents of 'err' with the total error from all sources 
                # of uncertainties. Therefore perhaps it is best to have that column twice with both names - at least until I fully understand what is actually going on.
                absErrEst=self.rcf['observations']['uncertainty']['systematicErrEstim']
                logger.info(f"User provided estimate of the absolute uncertainty of the observations including systematic errors is {absErrEst} percent.")
                # To avoid really bad things from happening where we have an observed CO2 concentration but no value for stddev: estimate a conservative value
                obsData1siteTimed['stddev'] = obsData1siteTimed['stddev'].fillna(1.0) # 1ppm should get me in the ballpark. Stddev is understood to be an absolute error in ppm not percent
                obsData1siteTimed.loc[:,'err_obs']=obsData1siteTimed.loc[:,'stddev']+(self.rcf['observations']['uncertainty']['systematicErrEstim'])*obsData1siteTimed.loc[:,'obs']*0.01 
                obsData1siteTimed.loc[:,'err']=obsData1siteTimed.loc[:,'err_obs'] 
                # Parameters like site-code, latitude, longitude, elevation, and sampling height are not present as data columns, but can be 
                # extracted from the header, which really is written as a comment. Thus the following columns need to be created: 
                fn=dob.info['fileName']
                # 'SamplingHeight':'height' (taken from metadata)
                obsData1siteTimed.loc[:,'site'] = dob.station['id'].lower()
                obsData1siteTimed.loc[:,'lat']=dob.lat
                obsData1siteTimed.loc[:,'lon']=dob.lon
                obsData1siteTimed.loc[:,'alt']=dob.alt
                obsData1siteTimed.loc[:,'height']=dob.meta['specificInfo']['acquisition']['samplingHeight']
                # site name/code is in capitals, but needs conversion to lower case:
                obsData1siteTimed.loc[:,'code'] = dob.station['id'].lower()
                if ('Site' not in availColNames):
                    obsData1siteTimed.loc[:,'Site'] = dob.station['id'].lower()
                # and the Time format has to change from "2018-01-02 15:00:00" to "20180102150000"
                # Note that the ['TIMESTAMP'] column is a pandas.series at this stage, not a Timestamp nor a string
                # I tried to pull my hair out converting the series into a timestamp object or similar and format the output,
                # but that is not necessary. When reading a local tar file with all observations, it is also a pandas series object, 
                # (not a timestamp type variable) and since I'm reading the data here directly into the dataframe, and not 
                # elsewhere, no further changes are required.
                logger.info(f"obsData1siteTimed= {obsData1siteTimed}")
                logger.debug(f"columns present:{obsData1siteTimed.columns}")
                #if(bFirstDf):
                #    obsData1siteTimed.to_csv('obsData1siteTimed.csv', encoding='utf-8', mode='w', sep=',')
                #else:
                #    obsData1siteTimed.to_csv('obsData1siteTimed.csv', encoding='utf-8', mode='a', sep=',', header=False)
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
                    sFileNameOnCarbonPortal = dob.meta['fileName']
                    logger.info(f"station name    : {dob.station['org']['name']}")
                    logger.info(f"PID             : {pid}")
                    logger.info(f"file name (csv) : {dob.meta['fileName']}")
                    logger.info(f"access url      : {dob.meta['accessUrl']}")
                    # logger.info(f"mobile flag: {}")
                    mobileFlag=None
                    scCSR=getSitecodeCsr(dob.station['id'].lower())
                    logger.info(f"sitecode_CSR: {scCSR}")
                    # 'optimize.observations.uncertainty.type' key to 'dyn' (setup_uncertainties in ui/main_functions.py, ~l174)                    
                    if(errorEstimate is None):
                        errorEstimate=CrudeErrorEstimate
                        logger.warning(f"A crude fall-back estimate of {CrudeErrorEstimate} ppm for overall uncertainties in the observations of CO2 has been used. Consider doing something smarter like changing the 'optimize.observations.uncertainty.type' key to 'dyn' in the .yml config file.")
                    else:
                        logger.info(f"errorEstimate (observations): {errorEstimate}")
                    data =( {
                      "site":dob.station['id'].lower() ,
                      "code": dob.station['id'].lower(),
                      "name": dob.station['org']['name'] ,
                      "dataObject": pidUrl ,
                      "fnameUrl": dob.meta['accessUrl'] ,
                      "lat": dob.lat,
                      "lon":dob.lon ,
                      "alt": dob.alt,
                      "height": dob.meta['specificInfo']['acquisition']['samplingHeight'],
                      "mobile": mobileFlag,
                      "file": sFileNameOnCarbonPortal,
                      "sitecode_CSR": scCSR,
                      "err": errorEstimate
                    })
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
        
        if(nBadies>0):
            logger.warning(f"A total of {nBadies} out of {len(selectedDobjLst)} data objects were rejected.")
        if(nDataSets==0):
                logger.error("Houston we have a big problem. No valid input files were found or the data within did not comply with what I was programmed to handle... ")
                logger.error("Mission Abort!")
                sys.exit(-1)
        else:
            logger.info(f"Read observational dry mole fraction data from {nDataSets} input files into a common data frame object.")
        # TODO: The observational data does not provide the background concentration. For testing we can brutally estimate a background. 
        # However, we expect to read the background from a separate external file, read it into a DF, and only if points are missing could we either 
        # abort LUMIA or use the crude estimate - perhaps with a threshold if no more than NmaxMissing points are missing, then use the crude 
        # estimate -- and ample uncertainties(!) for those points.
        # bruteForceObsBgBias=float(2.739) # ppm  Average over 65553 observations (drought 2018 data set), on average this estimate is wrong by 7.487ppm            
        # allObsDfs.loc[:,'crudeBgEstimate']=(allObsDfs.loc[:,'obs'] - bruteForceObsBgBias) 
        # allObsDfs.loc[:,'background']=(allObsDfs.loc[:,'obs'] - bruteForceObsBgBias) 

        #allObsDfs.loc[:,'background']=None
        #allObsDfs.loc[allObsDfs['background']  is None, 'background'] = allObsDfs.loc['crudeBgEstimate']

        # instead of replacing all, replace only those background points we are missing   allSitesDfs.loc[:,'background']=allSitesDfs.loc[:,'crudeBgEstimate']
        # TODO bgDf=readBackground(self.rcf,allSitesDfs)
        # allSitesDfs.loc[:,'background']=bgDf.loc[:,'background']
        # allSitesDfs['background'] = np.where(allSitesDfs['background'] is None, allSitesDfs['crudeBgEstimate'], allSitesDfs['background'])
        # df.loc[df['Fee'] > 22000, 'Fee'] = 15000
        # TODO: add ample background error for crude estimates.
        # setattr(self, 'observations', allObsDfs)
        setattr(self, 'sites', allSitesDfs)
        allObsDfs.to_csv('obsData-NoBkgnd.csv', encoding='utf-8', mode='w', sep=',')
        allSitesDfs.to_csv('mySitesAll.csv', encoding='utf-8', sep=',', mode='w')
        # self.load_tar(filename)
        # logger.info(f"{allObsDfs.shape[0]} observation read from {filename}")
        return(self,  allObsDfs,  'obsData-NoBkgnd.csv')
    
    def obsoleteEnsureReportedTimeIsStartOfMeasurmentInterval(sExistingFile,  tim0) -> "obsdb":
        '''  
        ensureReportedTimeIsStartOfMeasurmentInterval() checks whether the first time dimension value is one (undesired)
                                 or zero (as it should). Normally the netcdf header should provide whether the times reported
                                 refer to the start of the time step, the middle or the end. VPRM files do, EDGAR4.3 files don't.
                                 Hence we cannot rely on the header info and we use a primitive and brutal approach with zero
                                 elegance: if the first time step starts at one, then measurments are assumed to be reported at the
                                 end of each time step, while Lumia expect this time to represent the beginning of the time interval.
                                 Therefore we shift the time axis back in time by one timestep which is then equivalent to having
                                 times representing the beginning of an observation period.
                                 I had to add a fix for background co2 concentration netcdf files that are reported at the middle of
                                the time step and that need to be shifted by 30 minutes
            # TODO: If Time starts with one rather than zero hours, then the time recorded refers to the end of the 1h measurement interval
            #             as opposed to Lumia, which expects that time to represent the start of the measurement time interval.
            # We can fix this by shifting the time axis by one hour (with cdo):
            # cdo shifttime,-1hour xLjxG3d9euFZ9SOUj69okhaU.dLat250dLon250.eots xLjxG3d9euFZ9SOUj69okhaU.dLat250dLon250
            # TODO: This needs to be made smarter so we can call CDO and fix the time axis no matter what.....
        '''
        if (sExistingFile is None) :
            logger.error("Fatal error in cdoWrapper:ensureReportedTimeIsStartOfMeasurmentInterval(): no input file provided.")
            sys.exit(1)
        if((tim0 is not None) and (tim0==0)):
                # we are good. No need to shift. Time dimension starts at zero.
                return(sExistingFile,  True)
        
        # Do we have to shift the time axis or not? 
        # read the first value from the time dimension and see if it is zero.
        xrExisting = xr.open_dataset(sExistingFile, drop_variables='NEE') # only read the dimensions
        dTime=xrExisting.time.data
        t1 = Timestamp(dTime[0])
        tim0=t1.hour
        tim0m=t1.minute
        # print('tim0=%d'%tim0, flush=True)
        if((tim0==0) and (tim0m==0)):
                # we are good. No need to shift. Time dimension starts at zero.
                return(sExistingFile,  True)
        if(os.path.exists(sExistingFile[:-3]+'_0hours.nc')): # Perhaps we already have a fixed file?
            try:
                xrExisting = xr.open_dataset(sExistingFile[:-3]+'_0hours.nc', drop_variables='NEE') # only read the dimensions
                dTime=xrExisting.time.data
                t1 = Timestamp(dTime[0])
                tim0=t1.hour
                tim0m=t1.minute
                # print('tim0=%d'%tim0, flush=True)
                if((tim0==0) and (tim0m==0)):
                        # we are good. No need to shift. Time dimension starts at zero.
                        return(sExistingFile[:-3]+'_0hours.nc',  True)
            except:
                print("No worries")
        tStep=dTime[1] - dTime[0]
        tStep*=1e-9  # from nanoseconds to seconds - we expect 3600s=1h
        timeStep=int(tStep/60) # time step in minutes
        # print('time step= %dh'%timeStep, flush=True)
        # We expect tim0==tStep or half a time step at this stage, which is the reason for shifting the time dimension by one time unit.
        dtim=(tim0*60)+tim0m
        if((dtim!=timeStep) and (2*dtim!=timeStep)): 
            logger.warning(f"First time axis value in input file {sExistingFile} is not zero(=midnight) nor zero plus half nor zero plus one time step.")
        fnameOutput=sExistingFile[:-3]+'_0hours.nc'
        cdoCmd='cdo shifttime,-%dminute %s  %s'%(dtim, sExistingFile, fnameOutput)
        print(cdoCmd, flush=True)
        rvalue=os.system(cdoCmd) 
        if(rvalue!=0):
            # cdo has issues with the netcdf co2 background concentration files that Guillaume created. Try one last alternative approach:
            # dump the netcdf file to a text file and modify the start time, then use ncgen to reassemble.
            os.system("rm "+fnameOutput)
            sCmd='ncdump '+sExistingFile+' >'+sExistingFile+'.dump'
            rvalue=os.system(sCmd)
            with open(sExistingFile+'.dump') as f:
                lines = f.readlines()
            f.close()
            # outLines=[]
            with open(sExistingFile+'.dump2', 'w') as file:
                for line in lines:
                    newline=''
                    bReplaceLine=False
                    if 'time:units = \"hours since ' in line:
                        # time:units = "hours since 2018-01-01 00:30:00" ;  => time:units = "hours since 2018-01-01 00:00:00" ;
                        bNextChunkIsHours=False
                        chunks=line.split(' ')
                        for chunk in chunks:
                            if any(chars.isdigit() for chars in chunk):  # must be the date chunk 2018-01-01
                                bNextChunkIsHours=True
                            newline=newline+chunk+' '
                            if(bNextChunkIsHours):
                                newline=newline+'00:00:00\" ;\r\n'
                                bReplaceLine=True
                                break
                    if(bReplaceLine==True):
                        # outLines.append(newline)
                        file.write(newline)
                    else:
                        # outLines.append(line)                
                        file.write(line)
                    bReplaceLine=False
            file.close()
            # Now outLines should have all the data as before, but with the start time set to 00:00:00 as expected by Lumia
            # with open(sExistingFile+'.dump2', 'w') as file:
            #    file.write(jsondumps(outLines))
            sCmd='ncgen -o '+sExistingFile[:-3]+'_0hours.nc '+sExistingFile+'.dump2 '
            rvalue=os.system(sCmd)
            sCmd='rm '+sExistingFile+'.dump2'
            os.system(sCmd)
            sCmd='rm '+sExistingFile+'.dump'
            os.system(sCmd)
            if(rvalue==0):
                return(sExistingFile[:-3]+'_0hours.nc',  True)
            else:
                return(sExistingFile,  False)
        return(sExistingFile,  True)
     
    
    def  combineObsAndBgnd(self, obsDf, bgDf, bInterpolationRequired=False) -> "obsdb":
        """
        Function combineObsAndBgnd  Merges the background CO2 concentrations into the obsDf
    
        @param obsDf  the dataframe that holds all the observed dry mole fraction CO2 concentrations that were collected from the carbon portal
        @type df Pandas dataframe
        @param bgDf the dataframe that holds the background CO2 concentrations - must also contain 'time' and 'code' (=station name) columns
                                matching names and conventions from the obsDf.
        @type  df Pandas dataframe
        @returns The merged obsDf+background dataframe. The background may have been interpolated to provide meaningful values in all rows.
        @type  df Pandas dataframe
        
        # Merging of the 2 dataframes is a little more complicated as matches are not guaranteed....
        # we need to match the
        #   1. 3-letter site code
        #   2. the height (however, we have to ignore height, because Guillaume's co2 background concentrations are only stored for one height per observation site)
        #   3. the time
        # For ideas, look here:
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.merge.html
        # https://stackoverflow.com/questions/53549492/joining-two-pandas-dataframes-based-on-multiple-conditions
        # https://stackoverflow.com/questions/41815079/pandas-merge-join-two-data-frames-on-multiple-columns
        # https://stackoverflow.com/questions/74550482/pandas-interpolate-within-a-groupby-for-one-column
        # we do want 'left' here as opposed to 'inner'
        """
        print("bgDf=",  flush=True)
        print(bgDf,  flush=True)
        print("bgDf.info()=",  flush=True)
        print("obsDf.columns=",  flush=True)
        print(obsDf.columns,  flush=True)
        print("obsDf.index=",  flush=True)
        print(obsDf.index,  flush=True)
        obsDf.info() # is a timezone-naive datetime[64] data type
        obsDf.reset_index(inplace=True)  # .set_index('level_1') # .drop('time',axis=1)   
        print(obsDf.columns,  flush=True)
        print("obsDf.index=",  flush=True)
        print(obsDf.index,  flush=True)
        obsDf.info() # is a timezone-naive datetime[64] data type

        xBgDf = bgDf[['code', 'time', 'background']].copy()  # turns time into an integer it seems....
        xBgDf.to_csv('xBgDf.csv', encoding='utf-8', mode='w', sep=',')
        # # obsDf['time'] = to_datetime(obsDf['time'], format='%Y%m%d%H%M%S', utc = True)  # confirmed: 'time' is Timestamp type after this operation
        # obsDf['time'] = to_datetime(obsDf['time'], yearfirst=True, utc = True)  # confirmed: 'time' is Timestamp type after this operation
        # xBgDf['time'] = to_datetime(xBgDf['time'], yearfirst=True, utc = True)  # confirmed: 'time' is Timestamp type after this operation
        # # xBgDf['time'] = to_datetime(xBgDf['time'], format='%Y%m%d%H%M%S', utc = True)  # confirmed: 'time' is Timestamp type after this operation

        # xBgDf = bgDf[['code', 'time', 'background']].copy()  # turns time into an integer it seems....
        print("xBgDf.info()=",  flush=True)
        xBgDf.info() # # is a timezone aware datetime[64,UTC] data type
        print("bgDf.info()=",  flush=True)
        obsDf.info() # is a timezone-naive datetime[64] data type
        # if we don't fix this before calling merge, then no times will match ever....
        xBgDf['time'] = xBgDf['time'].astype("datetime64[ns, UTC]")
        obsDf['time'] = to_datetime(obsDf['time'], utc=True) # .dt.tz_convert('UTC')        
        obsDf['time'] = obsDf['time'].astype("datetime64[ns, UTC]")
        xBgDf.info()
        obsDf.info()
        xBgDf.to_csv('./xBgDf_utc.csv', encoding='utf-8', mode='w', sep=',')
        obsDf.to_csv('./obsDf_utc.csv', encoding='utf-8', mode='w', sep=',')


        # Merging only seems to work if background concentrations are provided at the same times as the observations. 
        obsDfWthBg = obsDf.merge(xBgDf,  how='left', on=['code','time'], indicator=True)
        # obsDfWthBg['time'] = to_datetime(obsDfWthBg['time'], utc = True)  # confirmed: 'time' is Timestamp type after this operation
        obsDfWthBg.to_csv('./obsDfWthBgContainingNaNs.csv', encoding='utf-8', mode='w', sep=',')
        # This has merged the 'background' column from the external file into the obsDf that holds all the observations from the carbon portal
        # However, we may not have background values for all sites and date-times....hence we interpolate in the background column 
        # linearly to cope with this in a reasonable way
        # Simple, first step: Works, but ignores the change of observation location: dfLin=newDf.[['background']].interpolate('linear')
        # The following trick was taken from here: https://stackoverflow.com/questions/39384749/groupby-pandas-incompatible-index-of-inserted-column-with-frame-index
        # limit : int, optional: Maximum number of consecutive NaNs to fill. Must be greater than 0. 7 days would be 7*24=168
        obsDfWthBg["intpBackground"]=obsDfWthBg[['code','background']].groupby(['code']).apply(lambda group: group.interpolate(method='linear', limit=168, limit_direction='both'))["background"].reset_index().set_index('level_1').drop('code',axis=1)   
        obsDfWthBg.to_csv('obsDfLinGrpInterpolatedBg.csv', encoding='utf-8', mode='w', sep=',')
        
        # Trying to interpolate over the actual corresponding date-time values results in some strange interpolation results
        # with resulting background values well below any supporting values...
        # However, given that we use equal time steps, linear interpolation really does the same thing and we need not bother
        # with a more complicated time axis.
        # newDf.set_index('time',inplace=True)
        # df2=newDf[['background']].interpolate('time')
        # df2.to_csv('/home/cec-ami/nateko/data/icos/DICE/mergeBackgroundCO2Test/newDfinterpolatedBg-time.csv', encoding='utf-8', mode='w', sep=',')
        obsDfWthBg.drop(columns=['background','_merge'], inplace=True)
        obsDfWthBg.rename(columns={'intpBackground':'background'}, inplace=True)
        obsDfWthBg.to_csv('ObsDfLinGrpInterpolatedBgShort.csv', encoding='utf-8', mode='w', sep=',')

        nTotal=len(obsDfWthBg)
        obsDfWthBg.dropna(subset=['obs'], inplace=True)
        nDroppedObsRows=nTotal - len(obsDfWthBg)
        nRemaining=len(obsDfWthBg)
        if(nDroppedObsRows>0):
            logger.info(f"Dropped {nDroppedObsRows} rows with no valid co2 observations. {nRemaining} good rows are remaining")
        obsDfWthBg.to_csv('ObsDfWthBg-dropnaObs.csv', encoding='utf-8', mode='w', sep=',')

        #  How do we handle any missing information on background concentrations where interpolation was not a sensible option:
        #  background :
        #     concentrations :
        #       stationWithoutBackgroundConcentration : DROP   # DROP or exclude station, user-provided FIXED estimate, DAILYMEAN across all other stations
        #       userProvidedBackgroundConcentration : 410 # ppm - only used if stationWithoutBackgroundConcentration==FIXED
        #
        # How many lines are affected?
        dfGoodBgValuesOnly=obsDfWthBg.dropna(subset=['background'])
        nRemaining=len(dfGoodBgValuesOnly)
        nDroppedBgRows=nTotal - nRemaining - nDroppedObsRows
        if(nDroppedBgRows > 0):
            whatToDo= 'DAILYMEAN' # 'DAILYMEAN' # self.rcf.get('background.concentrations.co2.stationWithoutBackgroundConcentration')
            if(whatToDo=='DAILYMEAN'):
                dfDailyMeans = dfGoodBgValuesOnly[['time','background']].resample('D', on='time').mean()
                # dfDailyMeans.rename(columns={'background':'dMeanBackground'}, inplace=True)
                dfDailyMeans.to_csv('./dMeans.csv', encoding='utf-8', mode='w', sep=',')
                # dfDailyMeans contains one mean background concentration across all good background values per day.
                dfDailyMeans['time2'] = to_datetime(dfDailyMeans.index)
                dfDailyMeans['date'] = to_datetime(dfDailyMeans['time2']).dt.date
                obsDfWthBg['date'] = to_datetime(obsDfWthBg['time']).dt.date
                # now we are sure we are dealing with datetime objects as opposed to int64 time values in seconds since a reference date.
                df = obsDfWthBg.merge(dfDailyMeans,  how='left', on=['date'], indicator=True)
                # df.to_csv('/home/cec-ami/nateko/data/icos/DICE/mergeBackgroundCO2Test/dfFinalObsDfWthBg.csv', encoding='utf-8', mode='w', sep=',')
                # Now we have a termporal df that contains in an additional column the daily mean background values for all observations
                
                # obsDfWthBg["dMeanBg"]=obsDfWthBg[['date','background']].groupby(['date']).merge(dfDailyMeans,  how='left', on=['date', 'dMeanBg'])
    
                # it would be more elegant to do it this way, but in practice it does not fill NaN values.... obsDfWthBg.fillna(obsDfWthBg.groupby('date')['background'].transform('mean'), inplace = True)
                # Now we replace all NaN values in the background column with estimates from the daily mean background value column
                # df['col1'] = df['col1'].fillna(df['col2'])
                obsDfWthBg['background']=obsDfWthBg['background'].fillna(df['background_y'])
                logger.info(f"Filled {nDroppedBgRows} rows with daily-mean values of the background co2 concentration across all valid sites. {nRemaining} good rows are remaining")
            elif(whatToDo=='FIXED'):
                # replace background CO2 concentration values that are missing with the fixed user-provided estimate of the background co2 concentrations
                EstimatedBgConcentration=410.0 # float(self.rcf.get('background.concentrations.co2.userProvidedBackgroundConcentration'))
                if(EstimatedBgConcentration<330.0):
                    logger.error(f'The user provided value for background.concentrations.co2.userProvidedBackgroundConcentration={EstimatedBgConcentration} is <330ppm,  which seems unreasonable. Please review you yml configuration file.')
                if(EstimatedBgConcentration>500.0):
                    logger.error(f'The user provided value for background.concentrations.co2.userProvidedBackgroundConcentration={EstimatedBgConcentration} is >500ppm,  which seems unreasonable. Please review you yml configuration file.')
                obsDfWthBg.fillna({'background':EstimatedBgConcentration},inplace=True)
                logger.info(f"Filled {nDroppedBgRows} rows with user-provided estimate of the background co2 concentration of {EstimatedBgConcentration}ppm. {nRemaining} good rows did not require intervention")
            else : #(whatToDo=='DROP')
                obsDfWthBg.dropna(subset=['background'], inplace=True)
                logger.info(f"Dropped {nDroppedBgRows} rows with missing background co2 concentrations. {nRemaining} good rows are remaining")
    
        # ..some prettification for easier human reading. However, I did not bother to format the index column. You could look
        # at https://stackoverflow.com/questions/16490261/python-pandas-write-dataframe-to-fixed-width-file-to-fwf   for ideas....
        if 'date' in obsDfWthBg.columns:
            obsDfWthBg.drop(columns=['date'], inplace=True)
        # obsDfWthBg.to_csv('finalObsDfWthBgUnformatted.csv', encoding='utf-8', mode='w', sep=',')
        obsDfWthBg.to_csv('./finalObsDfWthBgBeforeCleaning.csv', encoding='utf-8', mode='w', sep=',', float_format="%.5f")
        column_names = obsDfWthBg.columns
        print(column_names,  flush=True)
        # obsDfWthBg['Unnamed: 0'] = obsDfWthBg['Unnamed: 0'].map(lambda x: '%6d' % x)
        # the above line works n the test code, but not here, as the index is not named or reset here. Just leave it unformatted for now.

        obsDfWthBg['NbPoints'] = obsDfWthBg['NbPoints'].map(lambda x: '%3d' % x)
        obsDfWthBg['background'] = obsDfWthBg['background'].map(lambda x: '%10.5f' % x)
        obsDfWthBg['obs'] = obsDfWthBg['obs'].map(lambda x: '%10.5f' % x)
        obsDfWthBg['err'] = obsDfWthBg['err'].map(lambda x: '%8.5f' % x)
        obsDfWthBg['err_obs'] = obsDfWthBg['err_obs'].map(lambda x: '%8.5f' % x)
        obsDfWthBg['stddev'] = obsDfWthBg['stddev'].map(lambda x: '%10.7f' % x)
        obsDfWthBg['lat'] = obsDfWthBg['lat'].map(lambda x: '%8.4f' % x)
        obsDfWthBg['lon'] = obsDfWthBg['lon'].map(lambda x: '%8.4f' % x)
        obsDfWthBg['alt'] = obsDfWthBg['alt'].map(lambda x: '%6.1f' % x)
        obsDfWthBg['height'] = obsDfWthBg['height'].map(lambda x: '%6.1f' % x)
        obsDfWthBg.to_csv('./finalObsDfWthBg.csv', encoding='utf-8', mode='w', sep=',', float_format="%.5f")
        # setattr('observations', newDf)
        return(obsDfWthBg)
    """
        #  How do we handle any missing information on background concentrations where interpolation was not a sensible option:
        #  background :
        #     concentrations :
        #       stationWithoutBackgroundConcentration : DROP   # DROP or exclude station, user-provided FIXED estimate, DAILYMEAN across all other stations
        #       userProvidedBackgroundConcentration : 410 # ppm - only used if stationWithoutBackgroundConcentration==FIXED
        whatToDo=self.rcf.get('background.concentrations.co2.stationWithoutBackgroundConcentration')
        if(whatToDo=='DAILYMEAN'):
            meanValue=411.0
            obsDfWthBg.fillna(meanValue)
        if(whatToDo=='FIXED'):
            # replace background CO2 concentration values that are missing with the fixed user-provided estimate of the background co2 concentrations
            EstimatedBgConcentration=float(self.rcf.get('background.concentrations.co2.userProvidedBackgroundConcentration'))
            if(EstimatedBgConcentration<330.0):
                logger.error(f'The user provided value for background.concentrations.co2.userProvidedBackgroundConcentration={EstimatedBgConcentration} is <330ppm,  which seems unreasonable. Please review you yml configuration file.')
            if(EstimatedBgConcentration>500.0):
                logger.error(f'The user provided value for background.concentrations.co2.userProvidedBackgroundConcentration={EstimatedBgConcentration} is >500ppm,  which seems unreasonable. Please review you yml configuration file.')
            obsDfWthBg.fillna(EstimatedBgConcentration)
        else : #(whatToDo=='DROP'
            obsDfWthBg.dropna(subset=['background'], inplace=True)
            nRemaining=len(obsDfWthBg)
            nDroppedBgRows=nTotal - nRemaining - nDroppedObsRows
            if(nDroppedBgRows>0):
                logger.info(f"Dropped {nDroppedBgRows} rows with bad background co2 concentrations. {nRemaining} good rows are remaining")

    """

    def load_background(self,  bgFname) -> "obsdb":
        logger.info(f"Reading co2 background concentrations from file {bgFname}...")
        bInterpolationRequired=False
        try:
            # chop = bgFname[-3:]
            if('.tar.gz' in bgFname[-7:]):
                with tarfile.open(bgFname, 'r:gz') as tar:
                    with tar.extractfile("observations.csv") as extracted:
                        bgDf = read_csv(extracted)
            elif('.tar' in bgFname[-4:]):
                with tarfile.open(bgFname, 'r:') as tar:
                    with tar.extractfile("observations.csv") as extracted:
                        bgDf = read_csv(extracted)
            elif('.csv' in bgFname[-4:]):
                bgDf = read_csv(bgFname)
            elif('.nc' in bgFname[-3:]):
                fnametrunk=bgFname[:-7]
                startyr=self.start.year
                endyr=self.end.year
                bgFnames=[]
                for yr in range(startyr,  endyr+1,  1):
                    ncFname=fnametrunk+str(yr)+'.nc'
                    (ncFname,  bSuccess)=ensureReportedTimeIsStartOfMeasurmentInterval(ncFname,  tim0=30,  checkGrid =False)
                    if(bSuccess):
                        bgFnames.append(ncFname)
                    else:
                        bgFnames.append(ncFname)
                        bInterpolationRequired=True
                ds = xr.open_mfdataset(bgFnames)
                bgDf = ds.to_dataframe()
                print("bgDf.columns=",  flush=True)
                print(bgDf.columns,  flush=True)
                print("bgDf.index=",  flush=True)
                print(bgDf.index,  flush=True)
                bgDf = bgDf.reset_index()
                print("bgDf.columns=",  flush=True)
                print(bgDf.columns,  flush=True)
                print("bgDf.index=",  flush=True)
                print(bgDf.index,  flush=True)
                # Guillaume's files are organised completely differently. The table needs to be transposed in a somewhat
                # complicated manner. We do need a column for mix_background and the station codes also in a single 
                # column after the time column. Presently the netcdf file is organised as:
                # 	time	                            field	                    bik	            bir	            bsd	            cbw	            cgr	            cmn	            dec	            dig	gat	hei	hel	hpb	htm	hun	ipr	jfj	kas	kit	kre	lin	lmp	lmp_wdcgg	lmu	lut	nor	ope	oxk	pal	prs	puy	rgl	sac	smr	snb	ssl	ste	svb	tac	toh	trn	uto	wao	zsf
                # 0	2018-01-01 00:30:00	mix	                    423.31825	414.54709	411.99981	413.71381	411.15119	410.22423	410.92848	422.58164	415.18681	416.34113	413.86561	414.02218	417.64854	418.09004	418.42125	410.04927	409.77242	416.37753	415.57684	415.15033	410.23125	410.23125	411.20165	413.71363	421.84517	414.7046	412.32581	420.09796	409.99839	411.61891	412.29216	412.87999	424.33518	409.44515	412.71231	414.41855	423.48789	412.78749	412.50768	412.84079	422.34918	412.70306	410.57669
                # 1	2018-01-01 00:30:00	mix_background	410.99988	410.38451	411.66074	410.82046	409.35623	408.91705	409.44143	410.79327	409.39662	409.35538	410.23275	409.33394	409.46179	410.58845	409.18821	409.48239	408.79192	409.38533	409.53914	408.85297	409.46583	409.46583	410.09292	410.52195	411.03353	410.21645	409.42688	412.00559	409.43838	411.02189	411.84737	411.12235	413.99581	408.70579	409.83367	409.93449	411.84127	411.52936	409.80963	411.04295	411.56516	411.51942	409.2546
                # 2	2018-01-01 01:30:00	mix	                    423.37996	414.43735	412.02914	413.83583	411.25651	410.29187	411.0536	422.74757	415.2549	416.56408	413.91888	414.70403	416.94752	418.24933	418.22997	410.30616	409.78407	416.59712	415.95695	415.41202	410.18699	410.18699	411.2712	413.80076	421.8801	414.82928	412.55697	420.37188	410.23973	411.72636	412.23861	412.90815	424.47821	409.68731	413.06432	414.53945	423.70908	412.82136	412.54559	412.85981	422.39996	412.73577	410.82798
                # 3	2018-01-01 01:30:00	mix_background	410.8847	    410.45984	411.69677	410.98005	409.34674	408.9595	409.58232	410.64421	409.51679	409.52997	410.40376	409.45563	409.49234	410.57717	409.25395	409.69497	408.7619	409.55768	409.57349	408.90673	409.46272	409.46272	410.21754	410.6795	410.93197	410.38774	409.58552	412.00482	409.63514	411.20169	411.84318	411.26573	414.17626	408.82313	410.05339	410.11872	411.80154	411.6195	410.02377	411.19102	411.47584	411.60547	409.43357
                # 1) drop all lines we are not interested in...
                bgDf = bgDf[bgDf['field'] == 'mix_background']
                if (len(bgDf.index) < 99) :
                    try:
                        colRename=self.rcf['background']['concentrations']['co2']['rename']
                        bgDf = bgDf[bgDf['field'] == colRename]
                    except:
                        logger.error(f"Abort! The co2 background concentrations file {bgFname} seems to have no valid background concentrations or uses a non-recognised name.")
                        sys.exit(-1)
                bgDf.to_csv('./backgroundCo2Concentrations/background_2018-1.csv', encoding='utf-8', mode='w', sep=',', float_format="%.5f")
                # 	time	                            field	                    bik	            bir	            bsd	            cbw	            cgr	            cmn	            dec	            dig	gat	hei	hel	hpb	htm	hun	ipr	jfj	kas	kit	kre	lin	lmp	lmp_wdcgg	lmu	lut	nor	ope	oxk	pal	prs	puy	rgl	sac	smr	snb	ssl	ste	svb	tac	toh	trn	uto	wao	zsf
                # 1, 2018-01-01 00:30:00,mix_background,410.99988,410.38451,411.66074,410.82046,409.35623,408.91705,409.44143,410.79327,409.39662,409.35538,410.23275,409.33394,409.46179,410.58845,409.18821,409.48239,408.79192,409.38533,409.53914,408.85297,409.46583,409.46583,410.09292,410.52195,411.03353,410.21645,409.42688,412.00559,409.43838,411.02189,411.84737,411.12235,413.99581,408.70579,409.83367,409.93449,411.84127,411.52936,409.80963,411.04295,411.56516,411.51942,409.25460
                # 3, 2018-01-01 01:30:00,mix_background,410.88470,410.45984,411.69677,410.98005,409.34674,408.95950,409.58232,410.64421,409.51679,409.52997,410.40376,409.45563,409.49234,410.57717,409.25395,409.69497,408.76190,409.55768,409.57349,408.90673,409.46272,409.46272,410.21754,410.67950,410.93197,410.38774,409.58552,412.00482,409.63514,411.20169,411.84318,411.26573,414.17626,408.82313,410.05339,410.11872,411.80154,411.61950,410.02377,411.19102,411.47584,411.60547,409.43357
                # 5, 2018-01-01 02:30:00,mix_background,410.76600,410.53569,411.72298,411.13424,409.33660,409.02065,409.73427,410.50218,409.65776,409.73303,410.57675,409.59576,409.54582,410.55909,409.34219,409.91442,408.74663,409.75560,409.60251,408.97831,409.45644,409.45644,410.33926,410.83786,410.82834,410.55535,409.76107,412.00653,409.83817,411.35131,411.82704,411.38360,414.34691,408.95472,410.28572,410.32049,411.76479,411.69092,410.25045,411.31323,411.37745,411.67425,409.62380
                counter=0
                for col in bgDf.columns:
                    if (('time' not in col) and ('field' not in col)):
                        tmpBgDf=bgDf[['time', 'field', col]].copy()  # turns time into an integer it seems....
                        tmpBgDf['field']=col
                        tmpBgDf.rename(columns={col:'background'}, inplace=True)
                        if(counter==0):
                            trspBgDf=tmpBgDf[['time', 'field', 'background']].copy()
                        else:
                            trspBgDf=concat([trspBgDf, tmpBgDf], ignore_index=True)
                        counter+=1
                if(counter==0):
                    logger.error(f"Abort! The co2 background concentrations file {bgFname} seems to have no valid data or site codes.")
                    sys.exit(-1)
                trspBgDf.rename(columns={'field':'code'}, inplace=True)
                # obsDf['time'] = to_datetime(obsDf['time'], format='%Y%m%d%H%M%S', utc = True)  # confirmed: 'time' is Timestamp type after this operation
                # Using 'yearfirst=True' is the better option in case the time column already contains a series or time object and not an integer as assumed above.
                trspBgDf['time'] = to_datetime(trspBgDf['time'], yearfirst=True, utc = True)  # confirmed: 'time' is Timestamp type after this operation
                try:
                    trspBgDf['time'] = to_datetime(trspBgDf['time'], format='%Y%m%d%H%M%S', utc = True)  # confirmed: 'time' is Timestamp type after this operation
                except:
                    trspBgDf['time'] = to_datetime(trspBgDf['time'], yearfirst=True, utc = True)  # Did not work for me....
                trspBgDf.to_csv('./backgroundCo2Concentrations/background_2018-f.csv', encoding='utf-8', mode='w', sep=',', float_format="%.5f")
                if(bInterpolationRequired):
                    trspBgDf['time'] -= offsets.Minute(30)
                    trspBgDf.to_csv('./backgroundCo2Concentrations/background_2018-hourly.csv', encoding='utf-8', mode='w', sep=',', float_format="%.5f")
                    bInterpolationRequired=False
                    return(trspBgDf, bInterpolationRequired)
                bgDf=trspBgDf[['time', 'code', 'background']].copy()
                bgDf.to_csv('./backgroundCo2Concentrations/background_2018a.csv', encoding='utf-8', mode='w', sep=',', float_format="%.5f")
                colRename=""
                try:
                    colRename=self.rcf['background']['concentrations']['co2']['rename']
                    if colRename in bgDf.columns :
                        bgDf.rename(columns={colRename:'background'}, inplace=True)
                except:
                    if (('mix_background' not in bgDf.columns) and ('background' not in bgDf.columns)) :
                        logger.error(f"Abort! The user provided co2 background concentrations file {bgFname} does not contain a column named {colRename} or background. Please review also the value of the >>background:concentrations:co2:rename<< key in your yaml file.")
                        sys.exit(-1)
            else:
                logger.error(f"Abort! Unsupported data format for co2 background concentrations (file {bgFname}). That file should be in .csv format (which may be inside a .tar or .tar.gz archive) or a netcdf file terminating in YYYY.nc and contain a column named background")
                sys.exit(-1)
        except:
            logger.error(f"Abort! Unable to read co2 background concentrations from file {bgFname}. That file should be in .csv format (which may be inside a .tar or .tar.gz archive) or a netcdf file terminating in YYYY.nc")
            sys.exit(-1)
        if ('background' not in bgDf.columns) :
            logger.error("Abort! The user provided co2 background concentrations file {bgFname} does not contain a column named background. Please review also the value of the >>background:concentrations:co2:rename<< key in your yaml file.")
            sys.exit(-1)
        return(bgDf, bInterpolationRequired)


    def setup_uncertainties(self, *args, **kwargs):
        errtype = self.rcf.get('observations.uncertainty.frequency')
        if errtype == 'weekly':
            self.setup_uncertainties_weekly()
        elif errtype == 'cst':
            self.setup_uncertainties_cst()
        elif errtype == 'dyn':
            self.setup_uncerainties_dynamic(*args, **kwargs)
        else :
            logger.error(f'The rc-key "obs.uncertainty" has an invalid value: "{errtype}"')
            raise NotImplementedError


    def setup_uncertainties_weekly(self):
        res = []
        # if 'observations.uncertainty.default_weekly_error' in self.rcf.keys :  TODO: TypeError: self.rcf.keys  is not iterable
        if (1==1):
            default_error = self.rcf.get('observations.uncertainty.default_weekly_error')
            if 'err' not in self.sites.columns :
                self.sites.loc[:, 'err'] = default_error
                
        with Pool() as pp :
            for site in self.sites.itertuples():
                dbs = self.observations.loc[self.observations.site == site.Index]
                if dbs.shape[0] > 0 :
                    res.append(pp.apply_async(_calc_weekly_uncertainty, args=(site.code, dbs.time, site.err)))
        
            for r in res :
                s, e = r.get()
                self.observations.loc[self.observations.site == s, 'err'] = e
                logger.info(f"Error for site {s:^5s} set to an averge of {e.mean():^8.2f} ppm")

    def setup_uncertainties_cst(self):
        for site in self.sites.itertuples():
            self.observations.loc[self.observations.site == site.Index, 'err'] = site.err

    def setup_uncertainties_dynamic(self, model_var: str = 'mix_apri', freq: str = '7D', err_obs: str = 'err_obs', err_min : float = 3):
        """
        Estimate the uncertainties based on the standard deviation of the (prior) detrended model-data mismatches. Done in three steps:
        1. Calculate weekly moving average of the concentrations, both for the model and the observed data.
        2. Subtract these weekly averages from the original concentration time series.
        3. Calculate the standard deviation of the difference of residuals

        The original uncertainties are then scaled to reach the values calculated above:
        - the target uncertainty (s_target) corresponds to the standard deviation in step 3 above
        - the obs uncertainty s_obs are inflated by a constant (site-specific) model uncertainty s_mod, computed from s_target^2 = sum(s_obs^2 + s_mod^2)/n (with n the number of observations).

        The calculation is done for each site code, so if two sites share the same code (e.g. sampling at the same location by two data providers), they will have the same observation uncertainty.
        """

        # Ensure that all observations have a measurement error:
        for col in {err_obs, 'obs'}:  # make sure float values have not been stored as strings - Python 3.10 wants to get smart on you compared to 3.9, where this goof-up did not happen....
            if(is_float_dtype(self.observations[col])==False):
                self.observations[col]=self.observations[col].astype(float)
        sel = self.observations.loc[:, err_obs] <= 0
        self.observations.loc[sel, err_obs] = self.observations.loc[sel, 'obs'] * err_min / 100.

        for code in self.observations.code.drop_duplicates():
            # 1) select the data
            mix = self.observations.loc[self.observations.code == code].loc[:, ['time', 'obs',model_var, err_obs]].set_index('time').sort_index()

            # 2) Calculate weekly moving average and residuals from it
            #trend = mix.rolling(freq).mean()
            #resid = mix - trend

            # Use a weighted rolling average, to avoid giving too much weight to the uncertain obs:
            weights = 1./mix.loc[:, err_obs]**2
            total_weight = weights.rolling(freq).sum()   # sum of weights in a week (for normalization)
            obs_weighted = mix.obs * weights
            mod_weighted = mix.loc[:, model_var] * weights
            obs_averaged = obs_weighted.rolling(freq).sum() / total_weight
            mod_averaged = mod_weighted.rolling(freq).sum() / total_weight
            resid_obs = mix.obs - obs_averaged
            resid_mod = mix.loc[:, model_var] - mod_averaged

            # 3) Calculate the standard deviation of the residuals model-data mismatches. Store it in sites dataframe for info.
            sigma = (resid_obs - resid_mod).values.std()
            self.sites.loc[self.sites.code == code, 'err'] = sigma
            logger.info(f'Model uncertainty for site {code} set to {sigma:.2f}')

            # 4) Get the measurement uncertainties and calculate the error inflation
#            s_obs = self.observations.loc[:, err_obs].values
#            nobs = len(s_obs)
#            s_mod = sqrt((nobs * sigma**2 - (s_obs**2).sum()) / nobs)

            # 5) Store the inflated errors:
            self.observations.loc[self.observations.code == code, 'err'] = sqrt(self.observations.loc[self.observations.code == code, err_obs]**2 + sigma**2).values
            self.observations.loc[self.observations.code == code, 'resid_obs'] = resid_obs.values
            self.observations.loc[self.observations.code == code, 'resid_mod'] = resid_mod.values
            self.observations.loc[self.observations.code == code, 'obs_detrended'] = obs_averaged.values
            self.observations.loc[self.observations.code == code, 'mod_detrended'] = mod_averaged.values