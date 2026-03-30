#!/usr/bin/env python

from datetime import datetime, timedelta
from numpy import zeros, sqrt, isnan, nanmedian
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


def _calc_weekly_uncertainty(site, times, dbs, xstd, minerr, wndw, col):
    err = zeros(len(times))
    for it, tt in enumerate(times):
        err[it] = dbs[col][(dbs.time >= tt-timedelta(days=wndw/2)) & (dbs.time < tt+timedelta(days=wndw/2))].std() * xstd
    if  isnan(err).any():
        err[isnan(err)] = nanmedian(err)
    if minerr > 0:
        err[err < minerr] = minerr
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
            for tr in self.rcf.get('obs.tracers'):
                for ty in self.rcf.get(f'obs.type.{tr}', tolist='force'):
                    self.SetupUncertainties(tr, ty)

    def SetupUncertainties(self, tr, ty):
        errtype = self.rcf.get(f'obs.uncertainty.{tr}.{ty}') #, info=infokeys)
        if errtype == 'weekly':
            xstd = self.rcf.get(f'obs.uncertainty.{tr}.{ty}.xstd', totype=float, default=1)
            minerr = self.rcf.get(f'obs.uncertainty.{tr}.{ty}.min', totype=float, default=0)
            wndw = self.rcf.get(f'obs.uncertainty.{tr}.{ty}.wndw', totype=float, default=7)
            col = self.rcf.get(f'obs.uncertainty.{tr}.{ty}.col', default='obs')
            self.SetupUncertainties_weekly(tr, ty, xstd, minerr, wndw, col)
        elif errtype == 'cst':
            err = self.rcf.get(f'obs.uncertainty.{tr}.{ty}.err', totype=float, default=1)
            self.SetupUncertainties_cst(tr, ty, err)
        elif errtype == 'tracer':
            self.SetupUncertainties_tracer(tr, ty)
        else :
            logger.error(f'The rc-key "obs.uncertainty" has an invalid value: "{errtype}"')
            raise NotImplementedError

    def SetupUncertainties_weekly(self, tr, ty, xstd, minerr, wndw, col):
        res = []
        with Pool() as pp :
            for site in self.sites.itertuples():
                dbs = self.observations.loc[(self.observations.site == site.Index) & (self.observations.tracer == tr) & (self.observations.type == ty)]
                if dbs.shape[0] > 0 :
                    res.append(pp.apply_async(_calc_weekly_uncertainty, args=(site.Index, dbs.time, dbs, xstd, minerr, wndw, col)))
        
            for r in res :
                s, e = r.get()
                self.observations.loc[(self.observations.site == s) & (self.observations.tracer == tr) & (self.observations.type == ty), 'err'] = e
                logger.info(f"Error for {tr}, {ty} and site {s:^5s} set to an average of {e.mean():^8.2f} ppm")

    def SetupUncertainties_cst(self, tr, ty, err):
        for site in self.observations.loc[(self.observations.tracer == tr) & (self.observations.type == ty), 'site'].unique():
            self.observations.loc[(self.observations.site == site) & (self.observations.tracer == tr) & (self.observations.type == ty), 'err'] = err
            logger.info(f"Error for {tr}, {ty} and site {site:^5s} set to an average of {err:^8.2f} ppm")

    def SetupUncertainties_tracer(self, tr, ty):
        self.observations.loc[self.observations.tracer == tr, 'err'] = self.observations.loc[self.observations.tracer == tr, 'obs'] * self.rcf.get(f'obs.uncertainty.{tr}.{ty}.scale', totype=float)
        for site in self.observations.loc[(self.observations.tracer == tr) & (self.observations.type == ty), 'site'].unique():
            logger.info(f"Error for {tr}, {ty} and site {site:^5s} set to an average of {self.observations.loc[(self.observations.site == site) & (self.observations.tracer == tr) & (self.observations.type == ty), 'err'].mean():^8.2f} ppm")
