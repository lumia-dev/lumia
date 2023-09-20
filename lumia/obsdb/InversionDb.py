# #!/usr/bin/env python

# from tqdm import tqdm
# from datetime import datetime, timedelta
# from numpy import zeros
# from lumia.obsdb import obsdb
# from lumia.Tools.logging_tools import logger


# class obsdb(obsdb):
#     def __init__(self, rcf, setupUncertainties=True):
#         self.rcf = rcf
#         start = datetime(*self.rcf.get('time.start'))
#         end = datetime(*self.rcf.get('time.end'))

#         super().__init__(self.rcf.get('obs.file'), start=start, end=end)

#         if setupUncertainties:
#             self.SetupUncertainties()

#     def SetupUncertainties(self):
#         errtype = self.rcf.get('obs.uncertainty')
#         if errtype == 'weekly':
#             self.SetupUncertainties_weekly()
#         elif errtype == 'cst':
#             self.SetupUncertainties_cst()
#         else :
#             logger.error(f'The rc-key "obs.uncertainty" has an invalid value: "{errtype}"')
#             raise NotImplementedError

#     def SetupUncertainties_weekly(self):
#         for site in self.sites.itertuples():
#             dbs = self.observations.loc[self.observations.site == site.Index]
#             times = dbs.time
#             err = zeros(len(times)) + site.err
#             nobs = zeros((len(times)))
#             for it, tt in tqdm(enumerate(times), desc=site.code, total=dbs.shape[0]):
#                 nobs[it] = sum((times >= tt-timedelta(days=3.5)) * (times < tt+timedelta(days=3.5)))
#             err *= nobs**.5
#             self.observations.loc[self.observations.site == site.Index, 'err'] = err

#     def SetupUncertainties_cst(self):
#         for site in self.sites.itertuples():
#             self.observations.loc[self.observations.site == site.Index, 'err'] = site.err

### NEW CODE

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


def _calc_weekly_uncertainty(site, times, dbs):
    # err = zeros(len(times)) + err
    # nobs = zeros((len(times)))
    err = zeros(len(times))
    for it, tt in enumerate(times):
        # nobs[it] = sum((times >= tt-timedelta(days=3.5)) & (times < tt+timedelta(days=3.5)))
        err[it] = dbs.obs[(dbs.time >= tt-timedelta(days=3.5)) & (dbs.time < tt+timedelta(days=3.5))].std() * 2 #TODO: Remember to go back to obs instead of old_obs
    # err *= sqrt(nobs)
    if  isnan(err).any():
        err[isnan(err)] = nanmedian(err)
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
                self.SetupUncertainties(tr)

    def SetupUncertainties(self, tr):
        errtype = self.rcf.get(f'obs.uncertainty.{tr}') #, info=infokeys)
        if errtype == 'weekly':
            self.SetupUncertainties_weekly(tr)
        elif errtype == 'cst':
            self.SetupUncertainties_cst(tr)
        elif errtype == 'tracer':
            self.SetupUncertainties_tracer(tr)
        else :
            logger.error(f'The rc-key "obs.uncertainty" has an invalid value: "{errtype}"')
            raise NotImplementedError

    def SetupUncertainties_weekly(self, tr):
        res = []
        with Pool() as pp :
            for site in self.sites.itertuples():
                dbs = self.observations.loc[(self.observations.site == site.Index) & (self.observations.tracer == tr)]
                if dbs.shape[0] > 0 :
                    res.append(pp.apply_async(_calc_weekly_uncertainty, args=(site.code, dbs.time, dbs))) #site._asdict()[f'err_{tr}'])))
        
            for r in res :
                s, e = r.get()
                self.observations.loc[(self.observations.site == s) & (self.observations.tracer == tr), 'err'] = e
                logger.info(f"Error for {tr} and site {s:^5s} set to an averge of {e.mean():^8.2f} ppm")

    def SetupUncertainties_cst(self, tr):
        for site in self.sites.loc[self.sites[tr] == True].itertuples():
            self.observations.loc[(self.observations.site == site.Index) & (self.observations.tracer == tr), 'err'] = site._asdict()[f'err_{tr}']
            logger.info(f"Error for {tr} and site {site.Index:^5s} set to an averge of {site._asdict()[f'err_{tr}']:^8.2f} ppm")

    def SetupUncertainties_tracer(self, tr):
        self.observations.loc[self.observations.tracer == tr, 'err'] = self.observations.loc[self.observations.tracer == tr, 'obs'] * self.rcf.get(f'obs.uncertainty.scale.{tr}', totype=float)
        logger.info(f"Error for tracer {tr:^5s} set to an averge of {self.observations.loc[self.observations.tracer == tr, 'err'].mean():^8.2f} ppm")
