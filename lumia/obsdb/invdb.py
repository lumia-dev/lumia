#!/usr/bin/env python

from lumia.obsdb import obsdb
from numpy import sqrt, nan, isnan
from loguru import logger

class invdb(obsdb):
    def setupUncertainties(self, err_obs_min=0, err_obs_fac=1., err_mod_min=0., err_mod_fac=1., err_bg_min=0., err_bg_fac=1., err_tot_min=0., err_tot_max=None, err_bg_field='err_bg', err_mod_field='err_mod', err_obs_field='err_obs'):
        """
        Setup the obs uncertainties based on the following rc-keys:
        - obs.errtot.min ==> minimum total observation error
        - obs.errtot.max ==> maximum total observation error (obs with error larger are discarded)

        The obs uncertainty is set as the quadratic sum of the background, foreground and measurement uncertainties
        Minimum values can be set for the three individual components, and they can be also be scaled, using the optional
        arguments :
          - err_obs_min ==> minimum measurement uncertainty
          - err_obs_fac ==> measurement uncertainty scaling factor (default = 1)
          - err_mod_min ==> minimum representation error for the regional transport
          - err_mod_fac ==> scaling factor for the regional representation error (default = 1)
          - err_bg_min ==> minimum uncertainty on background concentrations
          - err_bg_fac ==> scaling factor for the uncertainty on background concentrations
        In addition, min and max values for the total uncertainty can be enforced (obs with lower uncertainty get it
        inflated, observations with higher uncertainty are discarded), with the err_tot_min and err_tot_max arguments

        This requires the three following column to be present in the database:
        - err_meas     == err_obs?
        - err_mod
        - err_bg

        This sets (and overwrite, if present) the "err" column
        :return:
        """

        err_bg = self.observations.loc[:, err_bg_field].values
        err_mod = self.observations.loc[:, err_mod_field].values
        err_obs = self.observations.loc[:, err_obs_field].values
        logger.info(f"In invdb.setupUncertainties() err_obs_field={err_obs_field}")
        logger.info(f"In invdb.setupUncertainties() self={self}")
        err_bg[isnan(err_bg)] = 0.
        err_obs[isnan(err_obs)] = 0.
        err_mod[isnan(err_mod)] = 0.

        err_bg[err_bg < err_bg_min] = err_bg_min
        err_obs[err_obs < err_obs_min] = err_obs_min
        err_mod[err_mod < err_mod_min] = err_mod_min

        err_tot = sqrt((err_obs*err_obs_fac)**2 + (err_mod*err_mod_fac)**2 + (err_bg*err_bg_fac)**2)
        err_tot[err_tot < err_tot_min] = err_tot_min

        if err_tot_max is not None :
            err_tot[err_tot > err_tot_max] = nan

        self.observations.loc[:, 'err'] = err_tot
        self.observations.dropna(subset=['err'], inplace=True)
