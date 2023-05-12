#!/usr/bin/env python

from datetime import timedelta
from numpy import zeros, sqrt
import rctools
from lumia.obsdb import obsdb
from multiprocessing import Pool
from loguru import logger
from typing import Union
from pandas import DataFrame, to_datetime, concat  # , read_csv, read_hdf, Series, Timestamp
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
        renameLst=[] # lumia/obsdb/_init_.py expects a List/Dict in map_fields(), not a string
        renameLst.append(rcf['observations'][filekey].get('rename', []))
        logger.info('Renaming the following columns in the observations data:')
        logger.info(renameLst)
        db.map_fields(renameLst)
        #db.map_fields(rcf['observations'][filekey].get('rename', []))

        # If no "tracer" column in the observations file, it can also be provided through the rc-file (observations.file.tracer key)
        if "tracer" not in db.observations.columns:
            tracer = rcf['observations'][filekey]['tracer']
            logger.warning(f'No "tracer" column provided. Attributing all observations to tracer {tracer}')
            db.observations.loc[:, 'tracer'] = tracer
        return db
 
    def load_fromCPortal(self, filename,  rcf=None,  errorEstimate=None) -> "obsdb":
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
        # We need to provide the contents of "observations.csv" and "sites.csv". "files.csv" is empty and so is the data frame resulting from it.
        # bFromCarbonportal=True
        bFirstDf=True
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
        (dobjLst, cpDir)=readObservationsFromCarbonPortal(tracer='CO2',  cpDir=cpDir,  pdTimeStart=pdTimeStart, pdTimeEnd=pdTimeEnd, timeStep=timeStep,  sDataType=None,  iVerbosityLv=1)
        # read the observational data from all the files in the dobjLst. These are of type ICOS ATC time series
        for pid in dobjLst:
            # sFileNameOnCarbonPortal = cpDir+pid+'.cpb'
            # meta.get('https://meta.icos-cp.eu/objects/Igzec8qneVWBDV1qFrlvaxJI')

            # TODO: remove next line· - for testing only
            # pid="6k8ll2WBSqYqznUbTaVLsJy9" # TRN 180m - same as in observations.tar.gz - for testing
            # mdata=meta.get("https://meta.icos-cp.eu/objects/"+pid)  # mdata is available as part of dob (dob.meta)
            dob = Dobj("https://meta.icos-cp.eu/objects/"+pid)
            logger.info(f"dobj: {dob}")
            logger.info(f"Reading observed co2 data from: station={dob.station['org']['name']}, located at station latitude={dob.lat},  longitude={dob.lon},  altitude={dob.alt},  elevation={dob.elevation}")
            obsData1site = dob.get()
            logger.info(f"samplingHeight={dob.meta['specificInfo']['acquisition']['samplingHeight']}")
            # We rename first and then replace the values AFTER extracting the time slice - should be faster. Often the object is much smaller
            obsData1site.rename(columns={'TIMESTAMP':'time','Site':'code','co2':'obs','Stdev':'stddev','Flag':'icos_flag'}, inplace=True)
            # TODO: one might argue that 'err' should be named 'err_obs' straight away, but in the case of using a local
            # observations.tar.gz file, that is not the case and while e.g.uncertainties are being set up, the name of 'err' is assumed 
            # for the name of the column  containing the observational error in that dataframe and is only being renamed later.
            # Hence I decided to mimic the behaviour of a local observations.tar.gz file
            # However, that said, invdb.setupUncertainties() later in the game replaces the contents of 'err' with the total error from all sources 
            # of uncertainties. Therefore perhaps it is best to have that column twice with both names - at least until I fully understand what is actually going on.
            absErrEst=self.rcf['observations']['uncertainty']['systematicErrEstim']
            logger.info(f"User provided estimate of the absolute uncertainty of the observations including systematic errors is {absErrEst} percent.")
            obsData1site.loc[:,'err_obs']=(obsData1site.loc[:,'stddev']+self.rcf['observations']['uncertainty']['systematicErrEstim'])*obsData1site.loc[:,'obs']*0.01 
            obsData1site.loc[:,'err']=obsData1site.loc[:,'err_obs'] 
            # Parameters like site-code, latitude, longitude, elevation, and sampling height are not present as data columns, but can be 
            # extracted from the header, which really is written as a comment. Thus the following columns need to be created: 
            # 'SamplingHeight':'height' (taken from metadata)
            obsData1site.loc[:,'site'] = dob.station['id'].lower()
            obsData1site.loc[:,'lat']=dob.lat
            obsData1site.loc[:,'lon']=dob.lon
            obsData1site.loc[:,'alt']=dob.alt
            obsData1site.loc[:,'height']=dob.meta['specificInfo']['acquisition']['samplingHeight']
            # site name/code is in capitals, but needs conversion to lower case:
            obsData1site.loc[:,'code'] = dob.station['id'].lower()
            obsData1siteTimed = obsData1site.loc[(
                (obsData1site.time >= pdSliceStartTime) &
                (obsData1site.time <= pdSliceEndTime) &
                (obsData1site['NbPoints'] > 0)
            )]  
            # and the Time format has to change from "2018-01-02 15:00:00" to "20180102150000"
            # Note that the ['TIMESTAMP'] column is a pandas.series at this stage, not a Timestamp nor a string
            # I tried to pull my hair out converting the series into a timestamp object or similar and format the output,
            # but that is not necessary. When reading a local tar file with all observations, it is also a pandas series object, 
            # (not a timestamp type variable) and since I'm reading the data here directly into the dataframe, and not 
            # elsewhere, no further changes are required.
            logger.info(f"obsData1siteTimed= {obsData1siteTimed}")
            #if(bFirstDf):
            #    obsData1siteTimed.to_csv('obsData1siteTimed.csv', encoding='utf-8', mode='w', sep=',')
            #else:
            #    obsData1siteTimed.to_csv('obsData1siteTimed.csv', encoding='utf-8', mode='a', sep=',', header=False)
            if(bFirstDf):
                allObsDfs= obsData1siteTimed.copy()
            else:
                allObsDfs= concat([allObsDfs, obsData1siteTimed])
            # Now let's create the list of sites and store it in self....
            # The example I have from the observations.tar.gz files looks like this:
            # site,code,name,lat,lon,alt,height,mobile,file,sitecode_CSR,err
            # trn,trn,Trainou,47.9647,2.1125,131.0,180.0,,/proj/inversion/LUMIA/observations/eurocom2018/rona/TRN_180m_air.hdf.all.COMBI_Drought2018_20190522.co2,dtTR4i,1.5
            sFileNameOnCarbonPortal = cpDir+pid+'.cpb'
            logger.info(f"station name: {dob.station['org']['name']}")
            logger.info(f"file name cpb: {sFileNameOnCarbonPortal}")
            logger.info(f"file name (csv) for download {dob.meta['fileName']}")
            logger.info(f"file name (url): {dob.meta['accessUrl']}")
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
              "fnameCpb": sFileNameOnCarbonPortal ,
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
            
        # TODO: The observational data does not provide the background concentration. For testing we can brutally estimate a background. 
        # However, we expect to read the background from a separate external file, read it into a DF, and only if points are missing could we either 
        # abort LUMIA or use the crude estimate - perhaps with a threshold if no more than NmaxMissing points are missing, then use the crude 
        # estimate -- and ample uncertainties(!) for those points.
        bruteForceObsBgBias=float(2.739) # ppm  Average over 65553 observations (drought 2018 data set), on average this estimate is wrong by 7.487ppm            
        allSitesDfs.loc[:,'crudeBgEstimate']=(allSitesDfs.loc[:,'obs'] - bruteForceObsBgBias) 
        allSitesDfs.loc[:,'background']=None
        # instead of replacing all, replace only those background points we are missing   allSitesDfs.loc[:,'background']=allSitesDfs.loc[:,'crudeBgEstimate']
        # TODO bgDf=readBackground(self.rcf,allSitesDfs)
        # allSitesDfs.loc[:,'background']=bgDf.loc[:,'background']
        # allSitesDfs['background'] = np.where(allSitesDfs['background'] is None, allSitesDfs['crudeBgEstimate'], allSitesDfs['background'])
        allSitesDfs.loc[allSitesDfs['background']  is None, 'background'] = allSitesDfs.loc['crudeBgEstimate']
        # df.loc[df['Fee'] > 22000, 'Fee'] = 15000
        # TODO: add ample background error for crude estimates.
        setattr(self, 'observations', allObsDfs)
        setattr(self, 'sites', allSitesDfs)
        self.observations.to_csv('obsDataAll2-slf-load_fromCPortal-201.csv', encoding='utf-8', mode='w', sep=',')
        allObsDfs.to_csv('obsDataAll.csv', encoding='utf-8', mode='w', sep=',')
        allSitesDfs.to_csv('mySitesAll.csv', encoding='utf-8', sep=',', mode='w')
        # self.load_tar(filename)
        logger.info(f"{self.observations.shape[0]} observation read from {filename}")
        return(self)

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
