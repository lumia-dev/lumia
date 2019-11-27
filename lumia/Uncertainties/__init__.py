#!/usr/bin/env python
from lumia.Tools import Categories
import logging
import os
from .tools import read_latlon, horcor, calc_temp_corr
from lumia.Tools import Region
from numpy import dot
logger = logging.getLogger(__name__)

class Uncertainties:
    def __init__(self, rcf, data):
        self.data = data
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

    def calcPriorUncertainties(self):
        for cat in self.categories :
            if cat.optimize :
                errfact = cat.uncertainty*0.01
                errcat = abs(self.data.loc[self.data.category == cat, 'state_prior'].values)*errfact
                self.data.loc[self.data.category == cat, 'prior_uncertainty'] = errcat

    def setup_Hcor(self):
        for cat in self.categories :
            if cat.optimize :
                if not cat.horizontal_correlation in self.horizontal_correlations :
                    fname = self.checkCorFile(cat.horizontal_correlation, cat) # TODO: Move this to a completely external code/module?
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
        corlen, cortype = hcor.split('-')
        corlen = int(corlen)
        fname = 'Bh:%s:%5.5i_%s.nc'%(self.region.name, corlen, cortype)
        fname = os.path.join(self.rcf.get('correlation.inputdir'), fname)
        if not os.path.exists(fname):
            logger.info("Correlation file <p:%s> not found. Computing it"%fname)
            hc = horcor(
                self.region,
                corlen,
                cortype,
                self.data.loc[self.data.category == cat,('lat', 'lon')].drop_duplicates()
            )
            hc.calc_latlon_covariance()
            hc.write(fname)
            del hc
        return fname

