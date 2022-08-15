#!/usr/bin/env python

import os
from datetime import datetime
import h5py
from numpy import float64, zeros_like
from pandas import DataFrame, read_hdf
from rctools import RcFile as rc
from lumia.precon import preconditioner as precon
from lumia.Tools import Region, Categories
from loguru import logger
from lumia import paths
from types import SimpleNamespace


class Control:
    name = 'flexRes'

    def __init__(self, preconditioner=precon, optimized_categories=None):
        # Data containers :
        self.horizontal_correlations = {}
        self.temporal_correlations = {}
        self.vectors = DataFrame(columns=[
            'category', 'tracer', 'ipos', 'itime', 'lat', 'lon', 'time', 'area', 'land_fraction', 
            'state_prior', 'state_prior_preco', 'state', 'state_preco',
        ], dtype=float64)   #TODO: check if the float64 here could be avoided

        # Interfaces :
        self.save = self._to_hdf
#        self.load = self._from_hdf

        # Preconditioner (+ initialization)
        self.preco = preconditioner
        self.preco.init()

        if optimized_categories is not None :
            self.optimized_categories = optimized_categories

    def setupPrior(self, prior):
        self.vectors.loc[:, ['category', 'time', 'lat', 'lon', 'land_fraction']] = prior.loc[:, ['category', 'time', 'lat', 'lon', 'land_fraction']]
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
        catIndex = self.vectors.category.tolist()
        for cat in self.optimized_categories :
            Hor_L = self.horizontal_correlations[(cat.tracer, cat.name)]
            Temp_L = self.temporal_correlations[(cat.tracer, cat.name)]
            ipos = catIndex.index(cat.name)
            state += self.preco.xc_to_x(uncertainty, Temp_L, Hor_L, state_preco, ipos)
        if add_prior: 
            state += self.vectors.loc[:, 'state_prior']
        else :
            state += self.vectors.loc[:, 'state_prior'] * 0.  # we still need it converted to a dataframe

        # Store the current state and state_preco
        self.vectors.loc[:,'state'] = state
        self.vectors.loc[:,'state_preco'] = state_preco

        return state

    def g_to_gc(self, g):
        g_c = zeros_like(g)
        state_uncertainty = self.vectors.loc[:, 'prior_uncertainty'].values
        catIndex = self.vectors.category.tolist()
        for cat in self.optimized_categories:
            Hor_Lt = self.horizontal_correlations[(cat.tracer, cat.name)].transpose()
            Temp_Lt = self.temporal_correlations[(cat.tracer, cat.name)].transpose()
            ipos = catIndex.index(cat.name)
            g_c += self.preco.g_to_gc(state_uncertainty, Temp_Lt, Hor_Lt, g, ipos)
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
        with h5py.File(filename, 'a') as fid :
        
            # Correlations
            corr = fid.create_group('correlations')
            for tr, cat in self.horizontal_correlations:
                if tr not in corr :
                    corr.create_group(tr)
                corr[tr].create_group(cat)
                corr[tr][cat]['hori'] = self.horizontal_correlations[(tr, cat)]
                corr[tr][cat]['temp'] = self.temporal_correlations[(tr, cat)]

            fid.attrs['optimized_categories'] = [f'{cat.tracer}:{cat.name}' for cat in self.optimized_categories]

    @classmethod
    def from_hdf(cls, filename: str) -> "Control":
        ctl = cls()
        ctl.vectors = read_hdf(filename, 'vectors')
        with h5py.File(filename, 'r') as fid:
            for tr in fid['correlations']:
                for cat in fid['correlations'][tr]:
                    ctl.horizontal_correlations[(tr, cat)] = fid['correlations'][tr][cat]['hori'][:]
                    ctl.temporal_correlations[(tr, cat)] = fid['correlations'][tr][cat]['temp'][:]

            cats = [c.split(':') for c in fid.attrs['optimized_categories']]
        ctl.optimized_categories = [SimpleNamespace(tracer=c[0].strip(), name=c[1].strip()) for c in cats]

        return ctl
        
    # def _from_hdf(self, filename, loadrc=True):
    #
    #     self.vectors = read_hdf(filename, 'vectors')
    #
    #     with h5py.File(filename, 'r') as fid :
    #
    #         # rcf
    #         if loadrc :
    #             rcf = rc()
    #             for key in fid['rcf'].attrs:
    #                 rcf.setkey(key, fid['rcf'].attrs[key])
    #         else :
    #             try :
    #                 rcf = self.rcf
    #             except AttributeError :
    #                 logger.critical("no rcf info in the object")
    #                 raise
    #
    #         # correlations :
    #         for cor in fid['correlations/hor']:
    #             self.horizontal_correlations[cor] = fid['correlations/hor'][cor][:]
    #         for cor in fid['correlations/temp']:
    #             self.temporal_correlations[cor] = fid['correlations/temp'][cor][:]
    #
    #     return rcf
    
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
