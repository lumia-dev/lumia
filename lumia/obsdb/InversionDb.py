#!/usr/bin/env python

from datetime import datetime, timedelta
from numpy import zeros, sqrt
from lumia.obsdb import obsdb
from lumia.Tools.logging_tools import logger
from multiprocessing import Pool


def _calc_weekly_uncertainty(site, times, err):
    err = zeros(len(times)) + err
    nobs = zeros((len(times)))
    for it, tt in enumerate(times):
        nobs[it] = sum((times >= tt-timedelta(days=3.5)) & (times < tt+timedelta(days=3.5)))
    err *= sqrt(nobs)
    return site, err


class obsdb(obsdb):
    def __init__(self, rcf, setupUncertainties=True):
        self.rcf = rcf
        start = datetime(*self.rcf.get('time.start'))
        end = datetime(*self.rcf.get('time.end'))

        super().__init__(self.rcf.get('obs.file'), start=start, end=end)

        for field in self.rcf.get('obs.fields.rename', tolist=True, default=[]):
            source, dest = field.split(':')
            self.observations.loc[:, dest] = self.observations.loc[:, source]

        if self.rcf.get('obs.uncertainty.setup', default=setupUncertainties):
            self.SetupUncertainties()

    def SetupUncertainties(self):
        errtype = self.rcf.get('obs.uncertainty')
        if errtype == 'weekly':
            self.SetupUncertainties_weekly()
        elif errtype == 'cst':
            self.SetupUncertainties_cst()
        else :
            logger.error(f'The rc-key "obs.uncertainty" has an invalid value: "{errtype}"')
            raise NotImplementedError

    def SetupUncertainties_weekly(self):
        res = []
        with Pool() as pp :
            for site in self.sites.itertuples():
                dbs = self.observations.loc[self.observations.site == site.Index]
                if dbs.shape[0] > 0 :
                    res.append(pp.apply_async(_calc_weekly_uncertainty, args=(site.code, dbs.time, site.err)))
        
            for r in res :
                s, e = r.get()
                self.observations.loc[self.observations.site == s, 'err'] = e
                logger.info(f"Error for site {s:^5s} set to an averge of {e.mean():^8.2f} ppm")

    def SetupUncertainties_cst(self):
        for site in self.sites.itertuples():
            self.observations.loc[self.observations.site == site.Index, 'err'] = site.err
