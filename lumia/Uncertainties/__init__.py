#!/usr/bin/env python
import os
import logging
from numpy import dot, unique
from lumia.Tools import Categories
from lumia.Tools import Region
from .tools import read_latlon, horcor, calc_temp_corr
logger = logging.getLogger(__name__)

class Uncertainties:
    def __init__(self, rcf):
        self.data = None # data
        self.categories = Categories(rcf)
        self.horizontal_correlations = {}
        self.temporal_correlations = {}
        self.rcf = rcf
        self.region = Region(self.rcf)

    def __call__(self):
        self.calcPriorUncertainties()
        self.setup_Hcor()
        self.setup_Tcor()
        return {
            'prior_uncertainty' : self.data.loc[:, 'prior_uncertainty'],
            'Hcor':self.horizontal_correlations,
            'Tcor':self.temporal_correlations
        }

    def calcPriorUncertainties(self, data):
        """
        example method, but instead, use that of one of the derived classes
        """
        self.data = data
        for cat in self.categories :
            if cat.optimize :
                errfact = cat.uncertainty*0.01
                errcat = abs(self.data.loc[self.data.category == cat, 'state_prior'].values)*errfact
                min_unc = cat.min_uncertainty*errcat.max()/100.
                land_filter = self.data.land_fraction.values
                errcat[(errcat < min_unc) & (land_filter > 0)] = min_unc
                self.data.loc[self.data.category == cat, 'prior_uncertainty'] = errcat

    def setup_Hcor_old(self):
        for cat in self.categories :
            if cat.optimize :
                if not cat.horizontal_correlation in self.horizontal_correlations :
                    fname = self.checkCorFile(cat.horizontal_correlation, cat) # TODO: Move this to a completely external code/module?
                    P_h, D_h = read_latlon(fname)
                    Hor_L = P_h * D_h
                    self.horizontal_correlations[cat.horizontal_correlation] = Hor_L
                    del P_h, D_h

    def setup_Hcor(self):
        for cat in self.categories :
            if cat.optimize :
                if not cat.horizontal_correlation in self.horizontal_correlations :
                    fname = self.checkCorFile_vres(cat.horizontal_correlation, cat)
                    P_h, D_h = read_latlon(fname)
                    Hor_L = P_h * D_h
                    self.horizontal_correlations[cat.horizontal_correlation] = Hor_L
                    del P_h, D_h

    def setup_Tcor(self):
        for cat in self.categories :
            if cat.optimize :
                if not cat.temporal_correlation in self.temporal_correlations :
                    temp_corlen = float(cat.temporal_correlation[:3].strip())

                    # Time interval of the optimization
                    dt = {'y':12., 'm':1, 'd':1/30.}[cat.optimization_interval]

                    # Number of time steps :
                    times = self.data.loc[self.data.category == cat, 'time'].drop_duplicates()
                    nt = times.shape[0]

                    P_t, D_t = calc_temp_corr(temp_corlen, dt, nt)
                    self.temporal_correlations[cat.temporal_correlation] = dot(P_t, D_t)

    def checkCorFile(self, hcor, cat):
        # Generate the correlation file name
        data = self.data.loc[self.data.category == cat, ('lat', 'lon')].drop_duplicates()
        corlen, cortype = hcor.split('-')
        corlen = int(corlen)
        fname = 'Bh:%s:%5.5i_%s.nc'%(self.region.name, corlen, cortype)
        fname = os.path.join(self.rcf.get('correlation.inputdir'), fname)
        if not os.path.exists(fname):
            logger.info("Correlation file <p:%s> not found. Computing it",fname)
            hc = horcor(corlen, cortype, data)
            hc.calc_latlon_covariance()
            hc.write(fname)
            del hc
        return fname

    def checkCorFile_vres(self, hcor, cat):
        # Generate the correlation file name
        data = self.data.loc[self.data.category == cat, ('lat', 'lon')].drop_duplicates()
        corlen, cortype = hcor.split('-')
        corlen = int(corlen)
        nclusters = data.shape[0] 
        fname = f'Bh:{self.region.name}-{nclusters}clusters:{corlen}:{cortype}'
        fname = os.path.join(self.rcf.get('correlation.inputdir'), fname)
        if not os.path.exists(fname):
            logger.info("Correlation file <p:%s> not found. Computing it",fname)
            hc = horcor(corlen, cortype, data)
            hc.calc_latlon_covariance()
            hc.write(fname)
            del hc
        return fname

class PercentHourlyPrior(Uncertainties):
    def __init__(self, interface, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.interface = interface

    def calcPriorUncertainties(self, struct):
        for cat in self.categories :
            if cat.optimize :
                errfact = cat.uncertainty*0.01
                errcat = abs(self.data.loc[self.data.category == cat, 'state_prior'].values)*errfact
                land_filter = self.data.land_fraction.values

                # Calculate the uncertainties in the model space, then convert to optimization space

                
                # TODO: This is a temporary fix to reproduce EUROCOM inversions. Needs to be moved to a "Uncertainties_eurocom" module or class
                min_unc = cat.min_uncertainty*errcat.max()/100.
                errcat[(errcat < min_unc) & (land_filter > 0)] = min_unc
                
                self.data.loc[self.data.category == cat, 'prior_uncertainty'] = errcat


class EUROCOM(Uncertainties):
    def calcPriorUncertainties(self):
        for cat in self.categories :
            if cat.optimize :
                errfact = cat.uncertainty*0.01
                errcat = abs(self.data.loc[self.data.category == cat, 'state_prior'].values)*errfact
                
                # TODO: This is a temporary fix to reproduce EUROCOM inversions. Needs to be moved to a "Uncertainties_eurocom" module or class
                min_unc = cat.min_uncertainty*errcat.max()/100.
                land_filter = self.data.land_fraction.values
                errcat[(errcat < min_unc) & (land_filter > 0)] = min_unc
                
                self.data.loc[self.data.category == cat, 'prior_uncertainty'] = errcat