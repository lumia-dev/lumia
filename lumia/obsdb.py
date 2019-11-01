from pandas import DataFrame, read_hdf
import logging

class obsdb:
    def __init__(self, filename=None):
        self.sites = DataFrame(columns=['code', 'name', 'lat', 'lon', 'alt', 'height', 'mobile'])
        self.observations = DataFrame(columns=['time', 'site', 'lat', 'lon', 'alt', 'file'])
        self.files = DataFrame(columns=['filename'])
        if filename is not None :
            self.load(filename)
        
    def load(self, filename):
        self.observations = read_hdf(filename, 'observations')
        self.sites = read_hdf(filename, 'sites')
        self.files = read_hdf(filename, 'files')
        
    def save(self, filename):
        logging.info("Writing observation database to <p:%s>"%file.name)
        self.observations.to_hdf(filename, 'observations')
        self.sites.to_hdf(filename, 'sites')
        self.files.to_hdf(filename, 'files')