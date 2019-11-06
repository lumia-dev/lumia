#!/usr/bin/env python

from pandas import DataFrame, read_hdf
from lumia.rctools import rc
from .Tools import Region, Categories, colorize, g_to_gc, xc_to_x
from numpy import *
import os
import logging
import h5py
from tqdm import tqdm

class control:
    def __init__(self, rcf=None, savefile=None, prior=None):
        if savefile is None :
            # Data containers :
            self.horizontal_correlations = {}
            self.temporal_correlations = {}
            self.vectors = DataFrame(columns=[
                'state_prior', 
                'state_prior_preco', 
                'category'
            ], dtype=float64)
            self.vectors.loc[:, 'state_prior_preco'] = 0.
            if prior is not None :
                self.vectors.loc[:, 'state_prior'] = prior
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
    
#    def fillVectors(self, apri, **kwargs):
#        """
#        """
#        self.vectors.loc[:, 'state_prior'] = apri
#        self.vectors.loc[:, 'state_prior_preco'] = 0
#        for field in kwargs.keys():
#            self.vectors.loc[:, field] = kwargs.get(field)
            
    def setupUncertainties(self, dapri=None, Hc=None, Tc=None):
        if dapri is not None :
            self.vectors.loc[:, 'prior_uncertainty'] = dapri
        if 'prior_uncertainty' in self.vectors :
            self.horizontal_correlations = Hc
            self.temporal_correlations = Tc
            
    def xc_to_x(self, state_preco, add_prior=True):
        uncertainty = self.vectors.loc[:, 'prior_uncertainty'].values
        state = 0*uncertainty
        catIndex = self.vectors.category.tolist()
        for cat in tqdm(self.categories) :
            if cat.optimize :
                Hor_L = self.horizontal_correlations[cat.horizontal_correlation]
                Temp_L = self.temporal_correlations[cat.temporal_correlation]
                ipos = catIndex.index(cat.name)
                state += xc_to_x(uncertainty, Temp_L, Hor_L, state_preco, ipos, 1)
        if add_prior: state += self.vectors.loc[:, 'state_prior']
        return state
    
    def g_to_gc(self, g):
        g_c = zeros_like(g)
        state_uncertainty = self.vectors.loc[:, 'prior_uncertainty'].values
        catIndex = self.vectors.category.tolist()
        for cat in tqdm(self.categories):
            if cat.optimize :
                Hor_Lt = self.horizontal_correlations[cat.horizontal_correlation].transpose()
                Temp_Lt = self.temporal_correlations[cat.temporal_correlation].transpose()
                ipos = catIndex.index(cat.name)
                g_c += g_to_gc(state_uncertainty, Temp_Lt, Hor_Lt, g, ipos, 1)
        return g_c
        
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
    
    def get(self, item):
        try 
            return self.vectors.loc[:, item].values
        except KeyError :
            logging.critical(colorize("Parameter <b:%s> doesn't exist ..."%item))
            raise 
            
    def set(self, item, values):
        try:
            self.vectors.loc[:, item] = values
        except ValueError:
            logging.critical(colorize("Parameter value for <b:%s> could not be stored, because its dimension (%i) doesn't conform with that of the control vector (%i)"%(key, len(values), self.size)))
            raise
    
    def __getattr__(self, item):
        if item is 'size' :
            return len(self)
        else :
            if hasattr(self, item):
                return getattr(self, item)
            else :
                raise AttributeError(item)