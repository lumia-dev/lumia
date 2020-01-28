from pandas import DataFrame, read_hdf, read_json, errors, read_csv
import logging
from datetime import datetime
from numpy import unique
from copy import deepcopy
import tarfile
import os
from io import BytesIO

# Disable "PerformanceWarning" when saving the database to a hdf file
import warnings
warnings.simplefilter(action='ignore', category=errors.PerformanceWarning)

logger = logging.getLogger(__name__)

class obsdb:
    def __init__(self, filename=None, start=None, end=None, db=None):
        if db is not None :
            self._parent = db
        else :
            self.sites = DataFrame(columns=['code', 'name', 'lat', 'lon', 'alt', 'height', 'mobile'])
            self.observations = DataFrame(columns=['time', 'site', 'lat', 'lon', 'alt', 'file'])
            self.files = DataFrame(columns=['filename'])
            self.start = start
            self.end = end
            self.setup = False
        if filename is not None :
            self.load_tar(filename)

    def __getattr__(self, item):
        if '_parent' in vars(self):
            return getattr(self._parent, item)
        else :
            raise AttributeError

    def __setattr__(self, key, value):
        if '_parent' in vars(self):
            setattr(self._parent, key, value)
        else :
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


    def load_hdf(self, filename):
        self.observations = read_hdf(filename, 'observations')
        self.sites = read_hdf(filename, 'sites')
        self.files = read_hdf(filename, 'files')
        self.SelectTimes(self.start, self.end)
        logger.info(f"{self.observations.shape[0]} observation read from {filename}")

    def load_json(self, prefix):
        self.observations = read_json('%s.obs.json'%prefix)
        self.sites = read_json('%s.sites.json'%prefix)
        self.files = read_json('%s.files.json'%prefix)
        self.observations.loc[:, 'time'] = [datetime.strptime(str(d), '%Y%m%d%H%M%S') for d in self.observations.time]
        self.SelectTimes(self.start, self.end)

    def SelectTimes(self, tmin=None, tmax=None):
        tmin = self.start if tmin is None else tmin
        tmax = self.end if tmax is None else tmax
        tmin = self.observations.time.min() if tmin is None else tmin
        tmax = self.observations.time.max() if tmax is None else tmax
        self.observations = self.observations.loc[(
            (self.observations.time >= tmin) &
            (self.observations.time <= tmax)
        )]
        self.sites = self.sites.loc[unique(self.observations.site), :]

    def SelectObs(self, selection):
        self.observations = self.observations.loc[selection,:]
        sites = unique(self.observations.site)
        self.sites = self.sites.loc[sites]
        self.files = self.files.loc[unique(self.observations.file)]

    def get_iloc(self, selection):
        db = obsdb()
        db.observations = self.observations.iloc[selection]
        sites = unique(db.observations.site)
        db.sites = self.sites.loc[sites, :]
        if hasattr(self, 'files') and 'file' in db.observations.columns:
            db.files = self.files.loc[unique(db.observations.file.dropna()), :]
        return db

    def save_hdf(self, filename):
        logger.info("Writing observation database to %s"%filename)
        self.observations.to_hdf(filename, 'observations')
        self.sites.to_hdf(filename, 'sites')
        self.files.to_hdf(filename, 'files')
        return filename

    def save_tar(self, filename):
        logger.info("Writing observation database to %s"%filename)
        with tarfile.open(filename, 'w:gz') as tar :
            data = self.observations.to_csv(date_format='%Y%m%d%H%M%S').encode('utf-8')
            info = tarfile.TarInfo('observations.csv')
            info.size = len(data)
            tar.addfile(info, BytesIO(data))

            data = self.sites.to_csv().encode('utf-8')
            info = tarfile.TarInfo('sites.csv')
            info.size = len(data)
            tar.addfile(info, BytesIO(data))

            data = self.files.to_csv().encode('utf-8')
            info = tarfile.TarInfo('files.csv')
            info.size = len(data)
            tar.addfile(info, BytesIO(data))
        return filename

    def load_tar(self, filename):
        with tarfile.open(filename, 'r:gz') as tar:
            self.observations = read_csv(tar.extractfile('observations.csv'), infer_datetime_format='%Y%m%d%H%M%S', index_col=0, parse_dates=['time'])
            self.sites = read_csv(tar.extractfile('sites.csv'), index_col=0)
            self.files = read_csv(tar.extractfile('files.csv'), index_col=0)
        self.SelectTimes(self.start, self.end)
        logger.info(f"{self.observations.shape[0]} observation read from {filename}")

    def checkIndex(self, reindex=False):
        if True in self.observations.index.duplicated():
            if reindex :
                logger.warning("Duplicated indices found in the observations table! The table will be reindexed and the original indices will be lost!")
                self.observations.reset_index(inplace=True)
            else :
                logger.error("Duplicated indices found in the observations table!")
                raise RuntimeError
