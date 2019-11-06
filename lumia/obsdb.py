from pandas import DataFrame, read_hdf, read_json
import logging
from datetime import datetime

class obsdb:
    def __init__(self, filename=None):
        self.sites = DataFrame(columns=['code', 'name', 'lat', 'lon', 'alt', 'height', 'mobile'])
        self.observations = DataFrame(columns=['time', 'site', 'lat', 'lon', 'alt', 'file'])
        self.files = DataFrame(columns=['filename'])
        if filename is not None :
            self.load(filename)
            
    def load_json(self, prefix):
        self.observations = read_json('%s.obs.json'%prefix)
        self.sites = read_json('%s.sites.json'%prefix)
        self.files = read_json('%s.files.json'%prefix)
        self.observations.loc[:, 'time'] = [datetime.strptime(str(d), '%Y%m%d%H%M%S') for d in self.observations.time]
            
    def load(self, filename):
        self.observations = read_hdf(filename, 'observations')
        self.sites = read_hdf(filename, 'sites')
        self.files = read_hdf(filename, 'files')
        
    def save(self, filename):
        logging.info("Writing observation database to <p:%s>"%file.name)
        self.observations.to_hdf(filename, 'observations')
        self.sites.to_hdf(filename, 'sites')
        self.files.to_hdf(filename, 'files')
        return filename