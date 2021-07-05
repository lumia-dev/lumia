#!/usr/bin/env python

from datetime import datetime, timedelta
from numpy import zeros, sqrt
from lumia.obsdb import obsdb
from lumia.Tools.logging_tools import logger
from multiprocessing import Pool


infokeys = {
    'time.start' : 'Earliest date for the observations in the database.',
    'time.end'   : 'Latest date for the observations in the database.',
    'obs.file'   : 'Path to the observation file (in tar.gz format).',
    'obs.fields.rename':'List of columns of the observation file that need to be renamed on import, in the form of "oldname:newname" (where "oldname" is the name of the column in the file. If necessary, a list of fields can be provided (e.g. "colA:colX, colB:colY, colC:colZ".',
    'obs.uncertainty.setup': 'Determines whether the observation uncertainties need to be computed (default True).',
    'obs.uncertainty':'Approach used to compute the observation uncertainties. Possible values: "cst" (the observation uncertainties are constant, at each site, and taken after the "err" value in the "sites" table), or "weekly" (the observation uncertainty is set so that the aggregated uncertainties of all the observations in a week matches the "err" value in the "sites" table.'
}


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
        start = datetime(*self.rcf.get('time.start', info=infokeys))
        end = datetime(*self.rcf.get('time.end', info=infokeys))
        super().__init__(self.rcf.get('obs.file', info=infokeys), start=start, end=end)

        for field in self.rcf.get('obs.fields.rename', tolist='force', default=[], info=infokeys):
            source, dest = field.split(':')
            self.observations.loc[:, dest] = self.observations.loc[:, source]

        if self.rcf.get('obs.uncertainty.setup', default=setupUncertainties, info=infokeys):
            self.SetupUncertainties()

    def SetupUncertainties(self):
        errtype = self.rcf.get('obs.uncertainty', info=infokeys)
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
