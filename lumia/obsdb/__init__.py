import datetime
import os
import sys
import shutil
import tarfile
import tempfile
from numpy import unique, nan
from pandas import DataFrame, read_csv, read_hdf, Timestamp, to_datetime
from pandas.api.types import is_float_dtype
from loguru import logger
from typing import List, Union
from numpy import datetime64


class obsdb:
    def __init__(self, filename=None, start=None, end=None, db=None,  bFromCPortal=False,  rcFile=None, useGui: bool = False, ymlFile: str=None):
        if db is not None:
            self._parent = db
        else:
            self.sites = DataFrame(columns=['code', 'name', 'lat', 'lon', 'alt', 'height', 'mobile'])
            self.observations = DataFrame(columns=['time', 'site', 'lat', 'lon', 'alt'])
            # self.observations.loc[:, 'time'] = self.observations.time.astype(datetime64)
            # Moving from Python 3.9 to 3.10.10 this results in the error:
            #   TypeError: Casting to unit-less dtype 'datetime64' is not supported. Pass e.g. 'datetime64[ns]' instead.
            # However, if I use datetime64[ns], then I get another error: NameError: name 'ns' is not defined. Did you mean: 'os'?
            # Hence I tried to fix this from a pandas angle instead (https://stackoverflow.com/questions/16158795/cant-convert-dates-to-datetime64), which seems to work fine:
            self.observations.loc[:, 'time'] = to_datetime(self.observations.time)
            
            # self.observations.loc[:, 'time'] = self.observations.time.astype('datetime64[ns]')
            self.files = DataFrame(columns=['filename'])
            self.start = Timestamp(start)  #gibberish  if start is not None else None  -- if start was not given on the commandline, then start is None and it needs to come from the yml file
            self.end = Timestamp(end) # if end is not None else None
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
            bFromCPortal=False
            tracer='co2'
            try:
                if (isinstance(rcFile['run']['tracers'], str)):
                    tracer=rcFile['run']['tracers']
                else:
                    trac=rcFile['run']['tracers']
                    tracer=trac[0]
            except:
                tracer='co2'
            try:
                sLocation=rcFile.rcfGet(f'observations.{tracer}.file.location', default='LOCAL')
            except:
                logger.error(f'Either you have not specified the key observations.{tracer}.file.location in your yml config file or due to some bug a obsdb data object has been tried to create without providing a reference to valid rcFile object.')
                sys.exit(-17)
            if('CARBONPORTAL' in sLocation):
                bFromCPortal=True

            if (bFromCPortal):
                self=self.load_fromCPortal(rcFile, useGui, ymlFile)
            else:
                self.load_tar(filename)
            # self.observations.to_csv(sTmpPrfx+'_dbg_obsDataAll-ini48.csv', encoding='utf-8', mode='w', sep=',')
            self.filename = filename
            try:
                if self.start is None:
                    self.start = self.observations.time.min()
            except:
                self.start = self.observations.time.min()
            logger.debug(f'obsdb__ini__: self.start ={self.start}')
            try:
                if self.end is None:
                    self.end = self.observations.time.max()
            except:
                self.end = self.observations.time.max()
            #if (bFromCPortal):
            #    return(self)  ...not allowed


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
        selobsTmin = self.observations.time.min() 
        try:
            print(f'selobsTmin={selobsTmin}')
        except:
            pass
        try:
            print(f'tmin={tmin},  tmax={tmax}')
        except:
            pass
        try:
            print(f'self.start={self.start}')
        except:
            pass
        tmin = self.start if tmin is None else tmin
        tmax = self.end if tmax is None else tmax
        try:
            print(f'tmin={tmin},  tmax={tmax}')

        except:
            pass
        tmin = self.observations.time.min() if tmin is None else tmin
        tmax = self.observations.time.max() if tmax is None else tmax
        try:
            print(f'selobsTmin={selobsTmin},  tmin={tmin},  tmax={tmax}')
        except:
            pass
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
            logger.debug(f'mapping={mapping}')
            mapping = {field.split(':')[0]: field.split(':')[1] for field in mapping}
            
        for source, dest in mapping.items():
            self.observations.loc[:, dest] = self.observations.loc[:, source]

    @classmethod
    def from_dataframe(cls, df: DataFrame, rcFile=None) -> "obsdb":
        '''
        Class method  from_dataframe()

        @param df DESCRIPTION pandas dataframe that holds the observational data
        @type DataFrame pandas dataframe
        @return an obsdb object that contains (in a df?) the unique names of all observation sites (3-letter-code) and their respective heights among other.
        @rtype TYPE  obsdb object
        There are some issues: How does it pick the most adequate height at sites that report at multiple heights? Or is it whatever height is
        the first or last one to be read for a site that is being pushed into the returned object? Even so, there are lots of NaN in the resulting 
        obs object when reading all data from the carbon portal.  TODO:  This needs fixing.
        '''
        obs = cls()
        for site in df.site.drop_duplicates():
            dfs = df.loc[df.site == site]  # create one dfs dataframe for each site in turn
            # dfs gives us 10977 instead of 175422 rows, columns 'code' and 'site' are identical and present in all rows
            site = {}  # confusing naming - collects all columns that have only one value. But several sites may have up to 5 heights
            for col in dfs.columns: # ['index', 'icos_flag', 'NbPoints', 'stddev', 'time', 'obs', 'code', 'err_obs', 'err', 'site', 'lat', 'lon', 'alt', 'height', 'background', 'tracer', 'file', 'dataObject', 'mobile', 'fnameUrl', 'name',  'sitecode_CSR']
                values = dfs.loc[:, col].drop_duplicates().values
                if ('height' in col):  # grab the highest above ground measurement available
                    if ((len(values) > 1) and (len(values) < 8)):
                        maxh = max(values)
                        values[0] = maxh
                    site[col] =  values[0]
                elif (len(values) == 1):
                    site[col] = values[0]
            obs.sites.loc[site['site']] = site

        print('obs.sites=',  flush=True) 
        print(obs.sites,  flush=True)
        sTmpPrfx=rcFile[ 'run']['thisRun']['uniqueOutputPrefix']
        obs.sites.to_csv(sTmpPrfx+'obsSites.csv',  encoding='utf-8', sep=',',  mode='w')
        # Remove columns that have been transferred to "sites", except for the "site" column, which is used for establishing correspondence
        obs.observations = df.loc[:, ['site'] + list(set(df.columns) - set(obs.sites.columns))]
        sOutputPrfx=rcFile[ 'run']['thisRun']['uniqueOutputPrefix']
        obs.sites.to_csv(sOutputPrfx+'obsSites.csv',  encoding='utf-8', sep=',',  mode='w')
        print('obsSites=',  flush=True) # obs.to_csv('./obs1.csv',  encoding='utf-8', sep=',',  mode='w')
        print(obs,  flush=True)

        # Remove columns that have been transferred to "sites", except for the "site" column, which is used for establishing correspondence
        # obs.observations = df.loc[:, ['site'] + list(set(df.columns) - set(obs.sites.columns))]
        # if(rcFile is None):
        #     obs.sites.to_csv('./obsSites.csv',  encoding='utf-8', sep=',',  mode='w')
        # else:
        #     obs.sites.to_csv(sOutputPrfx+'obsSites.csv',  encoding='utf-8', sep=',',  mode='w')
        return obs

    def to_hdf(self, filename: str) -> str:
        df = self.to_dataframe()
        fname = os.path.basename(filename)
        sTmpPrfx=self.rcf[ 'run']['thisRun']['uniqueTmpPrefix']
        if(('departures.hdf' in filename) and (len(filename)>24)):
            print('ok')
        else:
            filename=sTmpPrfx+fname  
        # dirname=os.path.dirname(sTmpPrfx)
        #if('control.hdf' in fname):
        #    sOutputPrfx=self.rcf[ 'run']['thisRun']['uniqueOutputPrefix']
        #    filename=sOutputPrfx+'control.hdf'
        logger.debug(f'df.to_hdf (writes {filename}) df columns={df.columns}')
        # TODO: ad-hoc fix to convert "object" bool to standard bool. Need to make sure these don't be created in the 1st place
        logger.debug(f'Columns read from hdf file: {df.columns}')
        for col in df.columns:
            if df.loc[:, col].dtype == 'O' and isinstance(df.loc[:, col].iloc[0], bool):
                logger.warning(f"Converting column {col} from {df.loc[:, col].dtype} to {bool}")
                df.loc[:, col] = df.loc[:, col].astype(bool)
        df.to_csv( sTmpPrfx+'_dbg_init_obsdb_'+fname[:-4]+'.csv', encoding='utf-8', mode='w', sep=',')
        # TODO:  /home/arndt/dev/lumia/lumiaDA/lumia/lumia/obsdb/__init__.py:250: PerformanceWarning: 
        # your performance may suffer as PyTables will pickle object types that it cannot
        # map directly to c-types [inferred_type->mixed,key->block3_values] [items->Index(['icos_flag', 'site', 'code', 'tracer', 'sitecode_CSR', 'fnameUrl',
        #        'file', 'name', 'fnameCpb'],
        #       dtype='object')]
        df.to_hdf(filename, key='observations')  # this is the pandas dataframe .to_hdf() method being called
        return filename

    @classmethod
    def from_hdf(cls, filename: str, rcFile=None) -> "obsdb":
        df = read_hdf(filename, key='observations')
        logger.debug(f'Columns read from hdf file: {df.columns}')        
        #  We need to ensure that all columns containing float values are perceived as such and not as object or string dtypes -- or havoc rages down the line
        knownColumns=['stddev', 'obs','err_obs', 'err', 'lat', 'lon', 'alt', 'height', 'background', 'mix_fossil', 'mix_biosphere', 'mix_ocean', 'mix_background', 'mix']
        for col in knownColumns:
            if col in df.columns:
                if(is_float_dtype(df[col])==False):
                    df[col]=df[col].astype(float)
        clsdf= cls.from_dataframe(df, rcFile=rcFile)
        print(clsdf, flush=True)
        return cls.from_dataframe(df, rcFile=rcFile)

    def checkIndex(self, reindex=False):
        if True in self.observations.index.duplicated():
            if reindex:
                logger.warning("Duplicated indices found in the observations table! The table will be reindexed and the original indices will be lost!")
                self.observations.reset_index(inplace=True)
            else:
                logger.error("Duplicated indices found in the observations table!")
                raise RuntimeError
