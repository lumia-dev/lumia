#!/usr/bin/env python

from datetime import timedelta
from numpy import zeros, sqrt
import rctools
from lumia.obsdb import obsdb
from multiprocessing import Pool
from loguru import logger
from typing import Union


def _calc_weekly_uncertainty(site, times, err):
    err = zeros(len(times)) + err
    nobs = zeros((len(times)))
    for it, tt in enumerate(times):
        nobs[it] = sum((times >= tt-timedelta(days=3.5)) & (times < tt+timedelta(days=3.5)))
    err *= sqrt(nobs)
    return site, err


class obsdb(obsdb):
    #def __init__(self, rcf, setupUncertainties=True):

    @classmethod
    def from_rc(cls, rcf: Union[dict, rctools.RcFile], setup_uncertainties: bool = True, filekey : str = 'file') -> "obsdb":
        """
        Construct an observation database based on a rc-file. The class does the following:
        - load an obs file in tar.gz format
        - rename fields, if required by the rc-file
        - calculate uncertainties (according to how specified in the rc-file)

        Arguments:
            - rcf: rctools.RcFile object (or dict)
            - setupUncertainties (optional): determine if the uncertainties need to be computed
            - filename (optional): path of the file containing the obs (overrides the one given by filekey)
            - filekey (optional): name of the section containing the file path and relevant keys

        """
        db = cls(
            rcf['observations'][filekey]['path'], 
            start=rcf['observations'].get('start', None), 
            end=rcf['observations'].get('end', None), 
            rcf=rcf)
            #location=rcf['observations'].get('location',  None))
        db.rcf = rcf

        # Rename fields, if required by the config file or dict:
        # the config file can have a key "observations.file.rename: col1:col2". In this case, the column "col1" will be renamed in "col2".
        # this can also be a list of columns: "observations.file.rename: [col1:col2, colX:colY]"
        renameLst=[] # lumia/obsdb/_init_.py expects a List/Dict in map_fields(), not a string
        renameLst.append(rcf['observations'][filekey].get('rename', []))
        logger.info('Renaming the following columns in the observations data:')
        logger.info(renameLst)
        db.map_fields(renameLst)
        #db.map_fields(rcf['observations'][filekey].get('rename', []))

        # If no "tracer" column in the observations file, it can also be provided through the rc-file (observations.file.tracer key)
        if "tracer" not in db.observations.columns:
            tracer = rcf['observations'][filekey]['tracer']
            logger.warning(f'No "tracer" column provided. Attributing all observations to tracer {tracer}')
            db.observations.loc[:, 'tracer'] = tracer
        return db

    def setup_uncertainties(self, *args, **kwargs):
        errtype = self.rcf.get('observations.uncertainty.frequency')
        if errtype == 'weekly':
            self.setup_uncertainties_weekly()
        elif errtype == 'cst':
            self.setup_uncertainties_cst()
        elif errtype == 'dyn':
            self.setup_uncerainties_dynamic(*args, **kwargs)
        else :
            logger.error(f'The rc-key "obs.uncertainty" has an invalid value: "{errtype}"')
            raise NotImplementedError

    def setup_uncertainties_weekly(self):
        res = []
        # if 'observations.uncertainty.default_weekly_error' in self.rcf.keys :  TODO: TypeError: self.rcf.keys  is not iterable
        if (1==1):
            default_error = self.rcf.get('observations.uncertainty.default_weekly_error')
            if 'err' not in self.sites.columns :
                self.sites.loc[:, 'err'] = default_error
                
        with Pool() as pp :
            for site in self.sites.itertuples():
                dbs = self.observations.loc[self.observations.site == site.Index]
                if dbs.shape[0] > 0 :
                    res.append(pp.apply_async(_calc_weekly_uncertainty, args=(site.code, dbs.time, site.err)))
        
            for r in res :
                s, e = r.get()
                self.observations.loc[self.observations.site == s, 'err'] = e
                logger.info(f"Error for site {s:^5s} set to an averge of {e.mean():^8.2f} ppm")

    def setup_uncertainties_cst(self):
        for site in self.sites.itertuples():
            self.observations.loc[self.observations.site == site.Index, 'err'] = site.err

    def setup_uncertainties_dynamic(self, model_var: str = 'mix_apri', freq: str = '7D', err_obs: str = 'err_obs', err_min : float = 3):
        """
        Estimate the uncertainties based on the standard deviation of the (prior) detrended model-data mismatches. Done in three steps:
        1. Calculate weekly moving average of the concentrations, both for the model and the observed data.
        2. Subtract these weekly averages from the original concentration time series.
        3. Calculate the standard deviation of the difference of residuals

        The original uncertainties are then scaled to reach the values calculated above:
        - the target uncertainty (s_target) corresponds to the standard deviation in step 3 above
        - the obs uncertainty s_obs are inflated by a constant (site-specific) model uncertainty s_mod, computed from s_target^2 = sum(s_obs^2 + s_mod^2)/n (with n the number of observations).

        The calculation is done for each site code, so if two sites share the same code (e.g. sampling at the same location by two data providers), they will have the same observation uncertainty.
        """

        # Ensure that all observations have a measurement error:
        sel = self.observations.loc[:, err_obs] <= 0
        self.observations.loc[sel, err_obs] = self.observations.loc[sel, 'obs'] * err_min / 100.

        for code in self.observations.code.drop_duplicates():
            # 1) select the data
            mix = self.observations.loc[self.observations.code == code].loc[:, ['time', 'obs',model_var, err_obs]].set_index('time').sort_index()

            # 2) Calculate weekly moving average and residuals from it
            #trend = mix.rolling(freq).mean()
            #resid = mix - trend

            # Use a weighted rolling average, to avoid giving too much weight to the uncertain obs:
            weights = 1./mix.loc[:, err_obs]**2
            total_weight = weights.rolling(freq).sum()   # sum of weights in a week (for normalization)
            obs_weighted = mix.obs * weights
            mod_weighted = mix.loc[:, model_var] * weights
            obs_averaged = obs_weighted.rolling(freq).sum() / total_weight
            mod_averaged = mod_weighted.rolling(freq).sum() / total_weight
            resid_obs = mix.obs - obs_averaged
            resid_mod = mix.loc[:, model_var] - mod_averaged

            # 3) Calculate the standard deviation of the residuals model-data mismatches. Store it in sites dataframe for info.
            sigma = (resid_obs - resid_mod).values.std()
            self.sites.loc[self.sites.code == code, 'err'] = sigma
            logger.info(f'Model uncertainty for site {code} set to {sigma:.2f}')

            # 4) Get the measurement uncertainties and calculate the error inflation
#            s_obs = self.observations.loc[:, err_obs].values
#            nobs = len(s_obs)
#            s_mod = sqrt((nobs * sigma**2 - (s_obs**2).sum()) / nobs)

            # 5) Store the inflated errors:
            self.observations.loc[self.observations.code == code, 'err'] = sqrt(self.observations.loc[self.observations.code == code, err_obs]**2 + sigma**2).values
            self.observations.loc[self.observations.code == code, 'resid_obs'] = resid_obs.values
            self.observations.loc[self.observations.code == code, 'resid_mod'] = resid_mod.values
            self.observations.loc[self.observations.code == code, 'obs_detrended'] = obs_averaged.values
            self.observations.loc[self.observations.code == code, 'mod_detrended'] = mod_averaged.values
