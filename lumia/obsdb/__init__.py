import datetime
import os
import shutil
import tarfile
import tempfile

from numpy import unique, nan
from pandas import DataFrame, read_csv, read_hdf, Series, Timestamp, to_datetime, concat
from loguru import logger
from typing import List, Union
from numpy import datetime64
from rctools import RcFile
from icoscp.cpb import metadata as meta
from icoscp.cpb.dobj import Dobj
from icosPortalAccess.readObservationsFromCarbonPortal import readObservationsFromCarbonPortal,  getSitecodeCsr



class obsdb:
    def __init__(self, filename=None, start=None, end=None, db=None,  rcf: Union[dict, RcFile]=None,  errorEstimate=None):
        # errorEstimate: it may be better to set this to be calculated dynamically in the yml file by setting the
       # 'optimize.observations.uncertainty.type' key to 'dyn' (setup_uncertainties in ui/main_functions.py, ~L.174) 
       # The value actually matters, in some cases: it can be used as a value for the weekly uncertainty, or for the default 
       # single-obs uncertainty (in the "lumia.obsdb.InversionDb.obsdb.setup_uncertainties_cst" and 
       # "lumia.obsdb.InversionDb.obsdb.setup_uncertainties_weekly" methods). But it's not something you can retrieve 
       # from the observations themselves (it accounts for model uncertainty as well, to a degree), so it's more of a user settings. 
        CrudeErrorEstimate="1.5" # 1.5 ppm
        bFromCarbonportal=False
        if db is not None:
            self._parent = db
        else:
            self.sites = DataFrame(columns=['code', 'name', 'lat', 'lon', 'alt', 'height', 'mobile'])
            self.observations = DataFrame(columns=['time', 'site', 'lat', 'lon', 'alt'])
            self.observations.loc[:, 'time'] = self.observations.time.astype(datetime64)
            self.files = DataFrame(columns=['filename'])
            self.start = Timestamp(start) if start is not None else None
            self.end = Timestamp(end) if end is not None else None
            self.setup = False
            self.io = {
                'observations': {
                    'write': [self.observations.to_csv.__func__, {'path_or_buf': 'observations.csv', 'date_format': '%Y%m%d%H%M%S', 'encoding': 'utf8'}],
                    'read': (read_csv, {'infer_datetime_format': '%Y%m%d%H%M%S', 'index_col': 0, 'parse_dates': ['time']}),
                    'filename': 'observations.csv'
                },
                'sites': {
                    'write': [self.sites.to_csv.__func__, {'path_or_buf': 'sites.csv', 'encoding': 'utf-8'}],
                    'read': (read_csv, {'index_col': 0}),
                    'filename': 'sites.csv',
                },
                'files': {
                    'write': [self.files.to_csv.__func__, {'path_or_buf': 'files.csv', 'encoding': 'utf-8'}],
                    'read': (read_csv, {'index_col': 0}),  # 'engine':'python', 'skipfooter':1}),
                    'filename': 'files.csv',
                }
            }
            self.extraFields = {}
        if filename is not None:
            # logger.info(rcf)
            timeStep=rcf['run']['timestep']
            sLocation=rcf['observations']['file']['location']
            if ('CARBONPORTAL' in sLocation):
                # We need to provide the contents of "observations.csv" and "sites.csv". "files.csv" is empty and so is the data frame resulting from it.
                bFromCarbonportal=True
                bFirstDf=True
                # we attempt to locate and read the tracer observations directly from the carbon portal - given that this code is executed on the carbon portal itself
                # readObservationsFromCarbonPortal(sKeyword=None, tracer='CO2', pdTimeStart=None, pdTimeEnd=None, year=0,  sDataType=None,  iVerbosityLv=1)
                cpDir=rcf['observations']['file']['cpDir']
                #remapObsDict=rcf['observations']['file']['renameCpObs']
                pdTimeStart = to_datetime(start, format="%Y-%m-%d %H:%M:%S")
                pdTimeStart=pdTimeStart.tz_localize('UTC')
                pdTimeEnd = to_datetime(end, format="%Y-%m-%d %H:%M:%S")
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
                    obsData1site.rename(columns={'TIMESTAMP':'time','Site':'code','co2':'obs','Stdev':'err','Flag':'icos_flag'}, inplace=True)
                    # one might argue that 'err' should be named 'err_obs' straight away, but in the case of using a local
                    # observations.tar.gz file, that is not the case and while e.g.uncertainties are being set up, the name of 'err' is assumed 
                    # for the name of the column  containing the observational error in that dataframe and is only being renamed later.
                    # Hence I decided to mimic the behaviour of a local observations.tar.gz file
                    # These are not read, thus need not be renamed: 'SamplingHeight':'height' (taken from metadata), 'QcBias': 'lat', 'QcBiasUncertainty': 'lon', 'DecimalDate':'alt',  
                    # Hence this idea is obsolete: Add latitude and longitude - we can abuse the existing (yet unused) QcBias coulmns for this without making the file bigger.
                    #                                              and along the same line of thought we can abuse DecimalDate for the site altitude
                    # logger.info(f"obsData1site= {obsData1site}")
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
                    # I tried to pull my hair out converting the series into a timestamp object or likewise and format the output,
                    # but that is not necessary. When reading a local tar file with all observations, it is also a pandas series object, 
                    # not timestamp and since I'm reading the data here and not elsewhere no further changes are required.
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
                    
                setattr(self, 'observations', allObsDfs)
                setattr(self, 'sites', allSitesDfs)
                allObsDfs.to_csv('obsDataAll.csv', encoding='utf-8', mode='w', sep=',')
                allSitesDfs.to_csv('mySitesAll.csv', encoding='utf-8', sep=',', mode='w')
                # self.load_tar(filename)
            else:
                self.load_tar(filename)
            self.filename = filename
            if self.start is None:
                self.start = self.observations.time.min()
            if self.end is None:
                self.end = self.observations.time.max()

    def __getattr__(self, item):
        if '_parent' in vars(self):
            return getattr(self._parent, item)
        # else :
        #    logger.error(f"Unknown method or attribute for obsdb: {item}")
        #    raise AttributeError(item)

    def __getitem__(self, item: str) -> DataFrame:
        """
        Return the slice of the observations dataframe corresponding to one observation site.
        """
        return self.observations.loc[self.observations.site == item]

    def __setattr__(self, key, value):
        if '_parent' in vars(self):
            setattr(self._parent, key, value)
        else:
            super().__setattr__(key, value)

    def load_db(self, db):
        """
        This is a method to import an existing obsdb instance. This enables expanding it with additional methods, from
        a derived class. For instance :
        db1 = obsdb(filename=db.tar.gz)  # Open an archived database
        db2 = footprintdb(db1)           # this will expand the initial "db1" object with methods from the "footprintdb"
                                         # class, which is a derived class of obsdb
        :param db:
        :return:
        """
        self.observations = db.observations
        self.sites = db.sites
        self.files = db.files

    def SelectTimes(self, tmin=None, tmax=None, copy=True):
        tmin = self.start if tmin is None else tmin
        tmax = self.end if tmax is None else tmax
        tmin = self.observations.time.min() if tmin is None else tmin
        tmax = self.observations.time.max() if tmax is None else tmax
        observations = self.observations.loc[(
                (self.observations.time >= tmin) &
                (self.observations.time <= tmax)
        )]
        sites = self.sites.loc[unique(self.observations.site), :]
        if copy:
            new = self.__class__(start=tmin, end=tmax)
            new.observations = observations
            new.sites = sites
            new.files = self.files
            return new
        else:
            self.observations = observations
            self.sites = sites

    def SelectSites(self, sitelist):
        selection = self.observations.site.isin(sitelist)
        # selection = [x in sitelist for x in self.observations.site]
        self.SelectObs(selection)

    def SelectObs(self, selection):
        self.observations = self.observations.loc[selection, :]
        sites = unique(self.observations.site)
        self.sites = self.sites.loc[sites]
        # if hasattr(self, 'files'):
        #    self.files = self.files.loc[unique(self.observations.file)]

    def get_iloc(self, selection):
        db = obsdb()
        db.observations = self.observations.iloc[selection]
        sites = unique(db.observations.site)
        db.sites = self.sites.loc[sites, :]
        # Often that part of the database is corrupted, so we try to read it, but we don't try too hard ...
        if hasattr(self, 'files') and 'file' in db.observations.columns:
            file_indices = unique(db.observations.file.dropna())
            files_in_db = [f for f in file_indices if f in db.files.index]
            files_not_in_db = [f for f in file_indices if f not in files_in_db]
            if len(files_in_db) > 0:
                db.files = self.files.loc[files_in_db, :]
            if len(files_not_in_db) > 0:
                for file in files_not_in_db:
                    db.observations.loc[db.observations.file == file, 'file'] = nan
        return db

    def save_tar(self, filename):
        logger.info(f"Writing observation database to {filename}")

        # Create a unique temporary directory, save the current directory
        dirname, filename = os.path.split(filename)
        dirname = './' if dirname == '' else dirname
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        tmpdir = tempfile.mkdtemp(dir=dirname)
        curdir = os.getcwd()
        os.chdir(tmpdir)

        # Create a tar file (and the intermediate files that go in the tar) in that temporary directory
        tmpfile = filename
        with tarfile.open(tmpfile, 'w:gz') as tar:
            for field in self.io:
                method, kwargs = self.io[field]['write']
                method(getattr(self, field), **kwargs)
                tar.add(self.io[field]['filename'])
                os.remove(self.io[field]['filename'])

        # Move back to the original directory, and move the tarfile in it
        os.chdir(curdir)
        os.rename(os.path.join(tmpdir, tmpfile), os.path.join(dirname, filename))

        # Delete the temporary directory
        shutil.rmtree(tmpdir)
        return os.path.join(dirname, filename)

    def load_tar(self, filename):
        with tarfile.open(filename, 'r:gz') as tar:
            for field in self.io:
                method, kwargs = self.io[field]['read']
                data = method(tar.extractfile(self.io[field]['filename']), **kwargs)
                setattr(self, field, data)
        self.SelectTimes(self.start, self.end, copy=False)
        logger.info(f"{self.observations.shape[0]} observation read from {filename}")

    @classmethod
    def from_tgz(cls, filename: str, start: datetime.datetime = None, end: datetime.datetime = None) -> "obsdb":
        obs = cls(start=start, end=end)
        obs.load_tar(filename)
        return obs

    def to_dataframe(self) -> DataFrame:
        """
        Combine the "sites" and "observations" dataframes in a single dataframe
        The columns of the "sites" dataframe are added to the "observations" dataframe if they are not already present
        """
        obs = self.observations.copy()
        for site in self.sites.itertuples():
            for field in set(self.sites.columns) - set(self.observations.columns):
                try:
                    obs.loc[self.observations.site == site.Index, field] = getattr(site, field)
                except AttributeError:
                    import pdb;
                    pdb.set_trace()
        return obs

    def map_fields(self, mapping: Union[dict, List[str]]) -> None:
        """
        Rename (copy in fact) fields in the observation dataframe. Fields to rename are passed as a "mapping" argument, which is either:
        - a dictionary of {source : dest} column names
        - a list of "source:dest" strings

        e.g. with the "mapping" argument set to ['bg:background', 'fg:foreground'] (or {'bg':'background', 'fg':'foreground'}, the "bg" and "fg" columns will respectively be copied to the "background" and "foreground" columns. The original columns (bg and fg) are kept, for reference.
        """

        if isinstance(mapping, list):
            mapping = {field.split(':')[0]: field.split(':')[1] for field in mapping}

        for source, dest in mapping.items():
            self.observations.loc[:, dest] = self.observations.loc[:, source]

    @classmethod
    def from_dataframe(cls, df: DataFrame) -> "obsdb":
        obs = cls()
        for site in df.site.drop_duplicates():
            dfs = df.loc[df.site == site]
            site = {}
            for col in dfs.columns:
                values = dfs.loc[:, col].drop_duplicates().values
                if len(values) == 1:
                    site[col] = values[0]
            obs.sites.loc[site['site']] = site

        # Remove columns that have been transferred to "sites", except for the "site" column, which is used for establishing correspondance
        obs.observations = df.loc[:, ['site'] + list(set(df.columns) - set(obs.sites.columns))]

        return obs

    def to_hdf(self, filename: str) -> str:
        df = self.to_dataframe()
        # TODO: ad-hoc fix to convert "object" bool to standard bool. Need to make sure these don't be created in the 1st place
        for col in df.columns:
            if df.loc[:, col].dtype == 'O' and isinstance(df.loc[:, col].iloc[0], bool):
                logger.warning(f"Converting column {col} from {df.loc[:, col].dtype} to {bool}")
                df.loc[:, col] = df.loc[:, col].astype(bool)
        df.to_hdf(filename, key='observations')
        return filename

    @classmethod
    def from_hdf(cls, filename: str) -> "obsdb":
        df = read_hdf(filename, key='observations')
        return cls.from_dataframe(df)

    def checkIndex(self, reindex=False):
        if True in self.observations.index.duplicated():
            if reindex:
                logger.warning("Duplicated indices found in the observations table! The table will be reindexed and the original indices will be lost!")
                self.observations.reset_index(inplace=True)
            else:
                logger.error("Duplicated indices found in the observations table!")
                raise RuntimeError

# Observations and sites data are now read and created correctly for the first data set found. The time field is actually also a series in case it is read from a tarbll - so I don't need to convert it in any way. Thus the obs data fields seem to be fine now. In the data for the sites file I was able to extract most info from the dobj meta data except the following: err (seems always to be 1.5), mobile: seems always to be None (according to Ute there presently are no mobile ICOS stations),  and it is unclear how the sitecode_CSR is created. It is not in the meta data. I have made a look-up table from an existing sites file, but it may be incvomplete. I'll check with Guillaume to clarify these. Next steps are reading all observational files matching the user criteria. The yml will need additional variables so a user can select say station altitude or intake height..
