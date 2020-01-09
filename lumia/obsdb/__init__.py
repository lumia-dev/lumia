from pandas import DataFrame, read_hdf, read_json, errors
import logging
from datetime import datetime
from numpy import unique
from copy import deepcopy

# Disable "PerformanceWarning" when saving the database to a hdf file
import warnings
warnings.simplefilter(action='ignore', category=errors.PerformanceWarning)

logger = logging.getLogger(__name__)

class obsdb:
    def __init__(self, filename=None, start=None, end=None, db=None):
        self.sites = DataFrame(columns=['code', 'name', 'lat', 'lon', 'alt', 'height', 'mobile'])
        self.observations = DataFrame(columns=['time', 'site', 'lat', 'lon', 'alt', 'file'])
        self.files = DataFrame(columns=['filename'])
        self.start = start
        self.end = end
        self.setup = False
        if filename is not None :
            self.load(filename)
        if db is not None :
            self.observations = db.observations
            self.sites = db.sites
            self.files = db.files

    def load(self, filename):
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
        db.files = self.files.loc[unique(db.observations.file.dropna()), :]
        return db

    def save(self, filename):
        logger.info("Writing observation database to %s"%filename)
        self.observations.to_hdf(filename, 'observations')
        self.sites.to_hdf(filename, 'sites')
        self.files.to_hdf(filename, 'files')
        return filename

    def checkIndex(self, reindex=False):
        if True in self.observations.index.duplicated():
            if reindex :
                logger.warning("Duplicated indices found in the observations table! The table will be reindexed and the original indices will be lost!")
                self.observations.reset_index(inplace=True)
            else :
                logger.error("Duplicated indices found in the observations table!")
                raise RuntimeError
