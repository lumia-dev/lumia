#!/usr/bin/env python

from pandas import DataFrame, read_hdf
from lumia.rctools import rc
from .Tools import Region, Categories, colorize
from numpy import *
import os
import logging
import h5py

class control:
    def __init__(self, rcf=None, savefile=None):
        if savefile is None :
            # Data containers :
            self.horizontal_correlations = {}
            self.temporal_correlations = {}
            self.vectors = DataFrame(columns=['state_prior', 'state_prior_preco'], dtype=float64)
        else :
            rcf = self.load(savefile)
        self.loadrc(rcf)
        
        # Interfaces :
        self.save = self._to_hdf
        self.load = self._from_hdf
        
    def loadrc(self, rcf):
        self.rcf = rcf
        self.categories = Categories(rcf)
        self.region = Region(self.rcf)
        self.start = datetime(*self.rcf.get('time.start'))
        self.end = datetime(*self.rcf.get('time.end'))
    
    def fillVectors(self, apri, **kwargs):
        """
        """
        self.vectors.loc[:, 'state_prior'] = apri
        self.vectors.loc[:, 'state_prior_preco'] = 0
        for field in kwargs.keys():
            self.vectors.loc[:, field] = kwargs.get(field)
            
    def setupUncertainties(self, dapri=None, Hc=None, Tc=None):
        if dapri is not None :
            self.vectors.loc[:, 'prior_uncertainty'] = dapri
        if 'prior_uncertainty' in self.vectors :
            self.horizontal_correlations = Hc
            self.temporal_correlations = Tc
            
#    def setupInterfaces(self, interface):
#        self.vec2mod = interface.vec2mod
#        self.mod2vec = interface.mod2vec
#        self.mod2file = interface.mod2file
#        self.file2mod = interface.file2mod
#        if vec2mod_adj is not None :
#            self.vec2mod_adj = interface.vec2mod_adj
            
    def setupPreco(self, xc_to_x, g_to_gc):
        self.xc_to_c = xc_to_x
        self.g_to_gc = g_to_gc
        
    def _to_hdf(self, filename):
        savedir = os.path.dirname(filename)
        if not os.path.exists(savedir):
            os.makedirs(savedir)
        logging.info(colorize("Write control savefile to <p:%s>"%filename))
        
        # Vectors
        self.vectors.to_hdf(filename, 'vectors')
        
        # Re-open the file to add auxiliary data
        fid = h5py.File(filename, 'a')
        
        # Correlations
        corr = fid.create_group('correlations')
        hc = corr.create_group('hor')
        for cor in self.horizontal_correlations :
            hc[cor] = self.horizontal_correlations[cor]
        tc = corr.create_group('temp')
        for cor in self.temporal_correlations :
            tc[cor] = self.temporal_correlations[cor]
        
        # rcf
        rcf = fid.create_group('rcf')
        for key in self.rcf.keys :
            rcf.attrs[key] = self.rcf.get(key)
            
        fid.close()
        
    def _from_hdf(self, filename, loadrc=True):
        
        self.vectors = read_hdf(filename, 'vectors')
        
        with h5py.File(filename, 'r') as fid :

            # rcf
            if loadrc :
                rcf = rc()
                for key in fid['rcf'].attrs:
                    rcf.setkey(key, fid['rcf'].attrs[key])
            else :
                try :
                    rcf = self.rcf
                except AttributeError :
                    logging.critical("no rcf info in the object")
                    raise

            # correlations :
            for cor in fid['correlations/hor']:
                self.horizontal_correlations[cor] = fid['correlations/hor'][cor][:]
            for cor in fid['correlations/temp']:
                self.temporal_correlations[cor] = fid['correlations/temp'][cor][:]
                
        return rcf