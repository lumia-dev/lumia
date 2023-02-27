import datetime
import os
import shutil
import tarfile
import tempfile

from numpy import unique, nan
from pandas import DataFrame, read_csv, read_hdf, Series, Timestamp, to_datetime
from loguru import logger
from typing import List, Union
from numpy import datetime64
from rctools import RcFile
from icosPortalAccess.readObservationsFromCarbonPortal import readObservationsFromCarbonPortal



class obsdb:
    def __init__(self, filename=None, start=None, end=None, db=None,  rcf: Union[dict, RcFile]=None):
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
            logger.info(rcf)
            sLocation=rcf['observations']['file']['location']
            timeStep=rcf['run']['timestep']
            if ('CARBONPORTAL' in sLocation):
                bFromCarbonportal=True
                # we attempt to locate and read the tracer observations directly from the carbon portal - given that this code is executed on the carbon portal itself
                # readObservationsFromCarbonPortal(sKeyword=None, tracer='CO2', pdTimeStart=None, pdTimeEnd=None, year=0,  sDataType=None,  iVerbosityLv=1)
                cpDir=rcf['observations']['file']['cpDir']
                remapObsDict=rcf['observations']['file']['renameCpObs']
                pdTimeStart = to_datetime(start, format="%Y-%m-%d %H:%M:%S")
                pdTimeStart=pdTimeStart.tz_localize('UTC')
                pdTimeEnd = to_datetime(end, format="%Y-%m-%d %H:%M:%S")
                pdTimeEnd=pdTimeEnd.tz_localize('UTC')
                readObservationsFromCarbonPortal(tracer='CO2',  cpDir=cpDir,  pdTimeStart=pdTimeStart, pdTimeEnd=pdTimeEnd, timeStep=timeStep,  sDataType=None,  iVerbosityLv=1)
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
