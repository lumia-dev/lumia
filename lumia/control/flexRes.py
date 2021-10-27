#!/usr/bin/env python

import os
import logging
from datetime import datetime
import h5py
from numpy import float64, zeros_like
from pandas import DataFrame, read_hdf
from lumia.Tools.rctools import rc
from lumia.precon import preconditioner as precon
from lumia.Tools import Region, Categories, Tracers

logger = logging.getLogger(__name__)


class Control:
    name = 'flexRes'

    def __init__(self, rcf=None, filename=None, preconditioner=precon):
        # Data containers :
        self.horizontal_correlations = {}
        self.temporal_correlations = {}
        self.vectors = DataFrame(columns=[
            'state_prior',
            'state_prior_preco',
            'state',
            'state_preco',
            'land_fraction',
            'tracer',
            'category',
            'lat',
            'lon',
            'time',
        ], dtype=float64)   #TODO: check if the float64 here could be avoided

        # Interfaces :
        self.save = self._to_hdf
        self.load = self._from_hdf

        # Preconditioner (+ initialization)
        self.preco = preconditioner
        self.preco.init()

        if rcf is not None :
            self.loadrc(rcf)
        elif filename is not None :
            rcf = self.load(filename, loadrc=True)
            self.loadrc(rcf)

    def loadrc(self, rcf):
        self.rcf = rcf
        # self.categories = Categories(rcf)
        self.tracers = Tracers(rcf)
        self.region = Region(self.rcf)
        self.start = datetime(*self.rcf.get('time.start'))
        self.end = datetime(*self.rcf.get('time.end'))

    def setupPrior(self, prior):
        self.vectors.loc[:, ['tracer', 'category', 'time', 'lat', 'lon', 'land_fraction']] = prior.loc[:, ['tracer', 'category', 'time', 'lat', 'lon', 'land_fraction']]
        self.vectors.loc[:, 'state_prior'] = prior.value
        self.vectors.loc[:, 'state_prior_preco'] = 0.
        self.vectors.loc[:, 'iloc'] = prior.loc[:, 'iloc']
        self.vectors.loc[:, 'itime'] = prior.loc[:, 'itime']

    def setupUncertainties(self, uncdict):
        self.vectors.loc[:, 'prior_uncertainty'] = uncdict['prior_uncertainty']
        self.horizontal_correlations = uncdict['Hcor']
        self.temporal_correlations = uncdict['Tcor']

    def xc_to_x(self, state_preco, add_prior=True):
        uncertainty = self.vectors.loc[:, 'prior_uncertainty'].values
        state = 0*uncertainty
        catIndex = list(zip(self.vectors.tracer.tolist(), self.vectors.category.tolist()))
        for tr in self.tracers.list:
            for cat in self.tracers[tr].categories:
                if cat.optimize :
                    Hor_L = self.horizontal_correlations[tr][cat.name][cat.horizontal_correlation]
                    Temp_L = self.temporal_correlations[tr][cat.name][cat.temporal_correlation]
                    ipos = catIndex.index((tr, cat.name))
                    state += self.preco.xc_to_x(uncertainty, Temp_L, Hor_L, state_preco, ipos, 1, path=self.rcf.get('path.run'))
        if add_prior: 
            state += self.vectors.loc[:, 'state_prior']

        # Store the current state and state_preco
        self.vectors.loc[:,'state'] = state
        self.vectors.loc[:,'state_preco'] = state_preco

        return state

    def g_to_gc(self, g):
        g_c = zeros_like(g)
        state_uncertainty = self.vectors.loc[:, 'prior_uncertainty'].values
        catIndex = list(zip(self.vectors.tracer.tolist(), self.vectors.category.tolist()))
        for tr in self.tracers.list:
            for cat in self.tracers[tr].categories:
                if cat.optimize :
                    Hor_Lt = self.horizontal_correlations[tr][cat.name][cat.horizontal_correlation].transpose()
                    Temp_Lt = self.temporal_correlations[tr][cat.name][cat.temporal_correlation].transpose()
                    ipos = catIndex.index((tr, cat.name))
                    g_c += self.preco.g_to_gc(state_uncertainty, Temp_Lt, Hor_Lt, g, ipos, 1, path=self.rcf.get('path.run'))
        return g_c
        
    def _to_hdf(self, filename):
        savedir = os.path.dirname(filename)
        if not os.path.exists(savedir):
            os.makedirs(savedir)
        elif os.path.exists(filename):
            os.remove(filename)
        logger.info(f"Write control savefile to {filename}")
        
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
                    logger.critical("no rcf info in the object")
                    raise

            # correlations :
            for cor in fid['correlations/hor']:
                self.horizontal_correlations[cor] = fid['correlations/hor'][cor][:]
            for cor in fid['correlations/temp']:
                self.temporal_correlations[cor] = fid['correlations/temp'][cor][:]

        return rcf
    
    def get(self, item):
        try :
            return self.vectors.loc[:, item].values
        except KeyError :
            logger.critical("Parameter %s doesn't exist ...", item)
            raise 
            
    def set(self, item, values):
        try:
            self.vectors.loc[:, item] = values
        except ValueError:
            logger.critical(f"Parameter value for '{item}' could not be stored, because its dimension '{len(values)}' doesn't conform with that of the control vector ({self.size})")
            raise
    
    def __getattr__(self, item):
        if item == 'size' :
            return len(self.vectors)
        elif item in self.__dict__:
            return getattr(self, item)
        else :
            raise AttributeError(item)
