#!/usr/bin/env python
import os
from copy import deepcopy
from datetime import datetime
from numpy import dot, unique, array
from lumia.Tools import Categories
from lumia.Tools import Region
from .tools import read_latlon, horcor, calc_temp_corr
from loguru import logger


class Uncertainties:
    def __init__(self, rcf, interface=None):
        self.data = None # data
        self.categories = Categories(rcf)
        self.horizontal_correlations = {}
        self.temporal_correlations = {}
        self.rcf = rcf
        self.region = Region(self.rcf)
        self.interface = interface
        self.corrfile = None

    def __call__(self, data, *args, **kwargs):
        self.calcPriorUncertainties(data, *args, **kwargs)
        self.setup_Hcor()
        self.setup_Tcor()
        return {
            'prior_uncertainty' : self.data.loc[:, 'prior_uncertainty'],
            'Hcor':self.horizontal_correlations,
            'Tcor':self.temporal_correlations
        }

    # def setup_Hcor_old(self):
    #     for cat in self.categories :
    #         if cat.optimize :
    #             if cat.horizontal_correlation not in self.horizontal_correlations :
    #                 fname = self.checkCorFile(cat.horizontal_correlation, cat) # TODO: Move this to a completely external code/module?
    #                 P_h, D_h = read_latlon(fname)
    #                 Hor_L = P_h * D_h
    #                 self.horizontal_correlations[cat.horizontal_correlation] = Hor_L
    #                 del P_h, D_h

    def setup_Hcor(self):
        for cat in self.categories :
            if cat.optimize :
                if cat.horizontal_correlation not in self.horizontal_correlations :
                    fname = self.checkCorFile_vres(cat.horizontal_correlation, cat)
                    self.corrfile = fname
                    P_h, D_h = read_latlon(fname)
                    Hor_L = P_h * D_h
                    self.horizontal_correlations[cat.horizontal_correlation] = Hor_L
                    del P_h, D_h

    def setup_Tcor(self):
        for cat in self.categories :
            if cat.optimize :
                if cat.temporal_correlation not in self.temporal_correlations :
                    temp_corlen = float(cat.temporal_correlation[:3].strip())

                    # Time interval of the optimization
                    #dt = {'y':12., 'm':1, 'd':1/30.}[cat.optimization_interval]
                    dt = cat.optimization_interval.months + 12*cat.optimization_interval.years + cat.optimization_interval.days/30. + cat.optimization_interval.hours/30/24

                    # Number of time steps :
                    times = self.data.loc[self.data.category == cat, 'time'].drop_duplicates()
                    nt = times.shape[0]

                    P_t, D_t = calc_temp_corr(temp_corlen, dt, nt)
                    self.temporal_correlations[cat.temporal_correlation] = dot(P_t, D_t)

    # def checkCorFile(self, hcor, cat):
    #     # Generate the correlation file name
    #     data = self.data.loc[self.data.category == cat, ('lat', 'lon')].drop_duplicates()
    #     corlen, cortype = hcor.split('-')
    #     corlen = int(corlen)
    #     fname = 'Bh:%s:%5.5i_%s.nc'%(self.region.name, corlen, cortype)
    #     fname = os.path.join(self.rcf.rcfGet('correlation.inputdir'), fname)
    #     if not os.path.exists(fname):
    #         logger.info("Correlation file <p:%s> not found. Computing it",fname)
    #         hc = horcor(corlen, cortype, data)
    #         hc.calc_latlon_covariance()
    #         hc.write(fname)
    #         del hc
    #     return fname

    def checkCorFile_vres(self, hcor, cat):
        # Generate the correlation file name
#        data = self.data.loc[self.data.category == cat, ('lat', 'lon')].drop_duplicates()
        data = self.data.loc[(self.data.category == 'biosphere') & (self.data.time == self.data.iloc[0].time), ('lat', 'lon')]
        corlen, cortype = hcor.split('-')
        corlen = int(corlen)
        nclusters = data.shape[0] 
        fname = f'Bh:{self.region.name}-{nclusters}clusters:{corlen}:{cortype}'
        fname = os.path.join(self.rcf.rcfGet('correlation.inputdir'), fname)
        if not os.path.exists(fname):
            logger.info("Correlation file <p:%s> not found. Computing it",fname)
            hc = horcor(corlen, cortype, data)
            hc.calc_latlon_covariance()
            logger.debug(f'lumia.uncertainties.checkCorFile_vres.write(fname={fname})')
            hc.write(fname)
            del hc
        return fname

    def errStructToVec(self, errstruct):
        data = self.interface.StructToVec(errstruct, lsm_from_file=self.rcf.rcfGet('emissions.lsm.file', default=False))
        #data = self.interface.StructToVec(errstruct, lsm_from_file=self.rcf.getAlt('emissions','lsm','file', default=False))
        data.loc[:, 'prior_uncertainty'] = data.loc[:, 'value']
        return data.drop(columns=['value'])


class PercentMonthlyPrior(Uncertainties):
    def calcPriorUncertainties(self, struct):
        fluxes = deepcopy(struct)
        self.data = self.interface.StructToVec(fluxes)
        for cat in self.categories :
            # TODO: We probably need to specify the tracer here as well. Though it might work, because I believe
            # we read the error values from the first tracer into the cat structure, meaning same values for all tracers
            # of any one category....so the code should run. Question remains, whether that is okay to do from a science perspective...
            field = self.rcf.rcfGet(f'emissions.{cat.name}.error_field', default=cat)
            if cat.optimize :
                errfact = cat.uncertainty*0.01
                errcat = abs(self.data.loc[self.data.category == field, 'value'].values)*errfact
                min_unc = cat.min_uncertainty*errcat.max()/100.
                land_filter = self.data.land_fraction.values
                errcat[(errcat < min_unc) & (land_filter > 0)] = min_unc
                self.data.loc[self.data.category == cat, 'prior_uncertainty'] = errcat


class ErrorFromTruth(Uncertainties):
    def calcPriorUncertainties(self, struct):
        data = deepcopy(struct)
        for cat in self.categories :
            if cat.optimize :
                errfact = cat.uncertainty*0.01
                data[cat.name]['emis'] = errfact*abs(data[cat.name]['emis']-data[f'true_{cat.name}']['emis'])
        self.data = self.errStructToVec(data)


class PercentHourlyPrior(Uncertainties):
    def calcPriorUncertainties(self, struct):
        data = deepcopy(struct)
        for cat in self.categories :
            if cat.optimize :
                errfact = cat.uncertainty*0.01
                field = self.rcf.rcfGet(f'emissions.{cat.name}.error_field', default=cat.name)
                data[cat.name]['emis'] = abs(data[field]['emis'])*errfact
        self.data = self.errStructToVec(data)


class PercentAnnualPrior(Uncertainties):
    def calcPriorUncertainties(self, struct):
        data = deepcopy(struct)
        for cat in self.categories :
            if cat.optimize :
                errfact = cat.uncertainty*0.01
                field = self.rcf.rcfGet(f'emissions.{cat.name}.error_field', default=cat.name)
                data[cat.name]['emis'][:] = (abs(data[field]['emis'])*errfact).mean(0)
        self.data = self.errStructToVec(data)


class eurocom(Uncertainties):
    def calcPriorUncertainties(self, struct):
        data = deepcopy(struct)
        data = self.errStructToVec(data)
        for cat in self.categories :
            if cat.optimize :
                errfact = cat.uncertainty*0.01
                errcat = abs(data.loc[data.category == cat, 'prior_uncertainty'].values)*errfact
                errcat[(errcat < 0.01*errcat.max())*(data.loc[:, 'land_fraction']>0)] = errcat.max()/100
                data.loc[data.category == cat, 'prior_uncertainty'] = errcat
        self.data = data


class PercentHourlyPrior_homogenized(Uncertainties):
    def calcPriorUncertainties(self, struct):
        data = deepcopy(struct)
        for cat in self.categories :
            if cat.optimize :
                # Optionally, use a different field
                field = self.rcf.rcfGet(f'emissions.{cat.name}.error_field', default=cat.name)

                # base error is the flux itself
                err = abs(data[field]['emis'])

                # Calculate the monthly and annual error, then scale the monthly flux so that the total error remains similar along the year
                start = data[field]['time_interval']['time_start']
                months = array([datetime(t.year, t.month, 1) for t in start])
                for month in unique(months):
                    err[months == month,:,:] *= err.mean(0).sum()/err[months == month,:,:].mean(0).sum()

                # save, accounting for the optional uncertainty scaling
                errfact = cat.uncertainty*0.01
                data[cat.name]['emis'][:] = err*errfact
        self.data = self.errStructToVec(data)
