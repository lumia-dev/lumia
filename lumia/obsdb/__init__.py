import os
import shutil
import logging
import tarfile
import tempfile
from numpy import unique, nan
from pandas import DataFrame, read_csv

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
            self.io = {
                'observations':{
                    'write':[self.observations.to_csv.__func__, {'path_or_buf':'observations.csv','date_format':'%Y%m%d%H%M%S', 'encoding':'utf8'}],
                    'read':(read_csv, {'infer_datetime_format':'%Y%m%d%H%M%S', 'index_col':0, 'parse_dates':['time']}),
                    'filename':'observations.csv'
                },
                'sites':{
                    'write':[self.sites.to_csv.__func__, {'path_or_buf':'sites.csv','encoding':'utf-8'}],
                    'read':(read_csv, {'index_col':0}),
                    'filename':'sites.csv',
                },
                'files':{
                    'write':[self.files.to_csv.__func__, {'path_or_buf':'files.csv', 'encoding':'utf-8'}],
                    'read':(read_csv, {'index_col':0}),# 'engine':'python', 'skipfooter':1}),
                    'filename':'files.csv',
                }
            }
            self.extraFields = {}
        if filename is not None :
            self.load_tar(filename)
            self.filename = filename
            if self.start is None :
                self.start = self.observations.time.min()
            if self.end is None :
                self.end = self.observations.time.max()

    def __getattr__(self, item):
        if '_parent' in vars(self):
            return getattr(self._parent, item)
        #else :
        #    logger.error(f"Unknown method or attribute for obsdb: {item}")
        #    raise AttributeError(item)

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
        if copy :
            new = self.__class__(start=tmin, end=tmax)
            new.observations = observations
            new.sites = sites
            new.files = self.files
            return new
        else :
            self.observations = observations
            self.sites = sites

    def SelectSites(self, sitelist):
        selection = [x in sitelist for x in self.observations.site]
        self.SelectObs(selection)

    def SelectObs(self, selection):
        self.observations = self.observations.loc[selection,:]
        sites = unique(self.observations.site)
        self.sites = self.sites.loc[sites]
        #if hasattr(self, 'files'):
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
            if len(files_in_db) > 0 :
                db.files = self.files.loc[files_in_db, :]
            if len(files_not_in_db) > 0 :
                for file in files_not_in_db :
                    db.observations.loc[db.observations.file == file, 'file'] = nan
        return db

    def save_tar(self, filename):
        logger.info("Writing observation database to %s", filename)

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
        with tarfile.open(tmpfile, 'w:gz') as tar :
            for field in self.io :
                method, kwargs = self.io[field]['write']
                method(getattr(self, field), **kwargs)
                tar.add(self.io[field]['filename'])
                os.remove(self.io[field]['filename'])

        # Move back to the original directory, and move the tarfile in it
        os.chdir(curdir)
        os.rename(os.path.join(tmpdir, tmpfile), os.path.join(dirname, filename))

        # Delete the temporary directory
        shutil.rmtree(tmpdir)
        return filename

    def load_tar(self, filename):
        with tarfile.open(filename, 'r:gz') as tar:
            for field in self.io :
                method, kwargs = self.io[field]['read']
                data = method(tar.extractfile(self.io[field]['filename']), **kwargs)
                setattr(self, field, data)
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
