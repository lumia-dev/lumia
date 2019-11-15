from pandas import DataFrame, read_hdf, read_json
import logging
from datetime import datetime
from numpy import unique
from lumia.Tools import colorize

class obsdb:
    def __init__(self, filename=None, start=None, end=None):
        self.sites = DataFrame(columns=['code', 'name', 'lat', 'lon', 'alt', 'height', 'mobile'])
        self.observations = DataFrame(columns=['time', 'site', 'lat', 'lon', 'alt', 'file'])
        self.files = DataFrame(columns=['filename'])
        self.start = start
        self.end = end
        if filename is not None :
            self.load(filename)

    def load(self, filename):
        self.observations = read_hdf(filename, 'observations')
        self.sites = read_hdf(filename, 'sites')
        self.files = read_hdf(filename, 'files')
        self._fix_sites_indices()
        self.SelectTimes(self.start, self.end)

    def load_json(self, prefix):
        self.observations = read_json('%s.obs.json'%prefix)
        self.sites = read_json('%s.sites.json'%prefix)
        self.files = read_json('%s.files.json'%prefix)
        self.observations.loc[:, 'time'] = [datetime.strptime(str(d), '%Y%m%d%H%M%S') for d in self.observations.time]
        self._fix_sites_indices()
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

    def _fix_sites_indices(self):
        # Make sure that the site codes are used as site indices:
        for id in unique(self.observations.loc[:, 'site']):
            try :
                self.observations.loc[self.observations.site == id, 'site'] = self.sites.loc[id].code
            except :
                import pdb; pdb.set_trace()
        self.sites.set_index('code', inplace=True)
        self.sites.loc[:, 'code'] = self.sites.index

    def save(self, filename):
        logging.info(colorize("Writing observation database to <p:%s>"%filename))
        self.observations.to_hdf(filename, 'observations')
        self.sites.to_hdf(filename, 'sites')
        self.files.to_hdf(filename, 'files')
        return filename
