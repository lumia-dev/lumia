#!/usr/bin/env python

from tqdm import tqdm
from datetime import datetime, timedelta
from numpy import zeros
from lumia.obsdb import obsdb
from lumia.Tools.logging_tools import logger


class obsdb(obsdb):
    def __init__(self, rcf, setupUncertainties=True):
        self.rcf = rcf
        start = datetime(*self.rcf.get('time.start'))
        end = datetime(*self.rcf.get('time.end'))

        super().__init__(self.rcf.get('obs.file'), start=start, end=end)

        if setupUncertainties:
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
        for site in self.sites.itertuples():
            dbs = self.observations.loc[self.observations.site == site.Index]
            times = dbs.time
            err = zeros(len(times)) + site.err
            nobs = zeros((len(times)))
            for it, tt in tqdm(enumerate(times), desc=site.code, total=dbs.shape[0]):
                nobs[it] = sum((times >= tt-timedelta(days=3.5)) * (times < tt+timedelta(days=3.5)))
            err *= nobs**.5
            self.observations.loc[self.observations.site == site.Index, 'err'] = err

    def SetupUncertainties_cst(self):
        for site in self.sites.itertuples():
            self.observations.loc[self.observations.site == site.Index, 'err'] = site.err