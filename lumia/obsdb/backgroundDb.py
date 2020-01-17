#!/usr/bin/env python
from lumia.obsdb import obsdb
from numpy import *
import os
from pandas import to_datetime
import xarray as xr
from lumia.Tools.system_tools import checkDir
import logging

logger = logging.getLogger(__name__)

class backgroundDb(obsdb):
    def read_backgrounds(self, path, prefix='mix.', suffix='.nc', step='apos', var='CO2.bg', field='background'):
        for site in self.sites.iterrows():
            ds = xr.load_dataarray(os.path.join(path, f'{prefix}{site.code}{suffix}'))

            # Select the good level
            level = site.height
            if not level in ds.level and len(ds.level == 1):
                level = -1 # take the only level available
                logger.warning(f"Use level {level} for site {site.code} with sampling height of {site.height} m")
            else :
                import pdb; pdb.set_trace()

            # Load the data
            bg = ds.sel(step=step, var=var, level=level)

            # Temporal interpolation
            bg_interp = interp(self.observations.time, bg.time, bg)
            self.observations.loc[self.observations.site == site.index, field] = bg_interp

class lumiaBgFile:
    def __init__(self, filename):
        self.filename = filename

    def write(self, data, step, mode='a', priority=None):
        data = data.expand_dims(step=[step])
        exists = os.path.exists(self.filename)
        if exists and mode != 'w':
            ds2 = self.merge(data, step, priority)
        else :
            ds2 = data.to_dataset(name='mix')

        checkDir(os.path.dirname(self.filename))
        ds2.to_netcdf(self.filename, mode='w')

    def merge(self, data, step, priority=None):
        ds = xr.load_dataarray(self.filename)
        if step in ds.step:
            ds2 = self.merge_step(data, ds, priority)
        else:
            ds2 = xr.concat([ds, data], dim='step')
        return ds2

    def merge_step(self, data0, data1, priority=None):
        """
        Extend an existing time series (along its time dimension).
        Priority is a boolean array, of the same length as the "time" dimension in data1, which determines whether (True)
        or not (False) the data from data1 should be used over that of data0, in case of overlap
        :param data0:
        :param data1:
        :param priority:
        :return:
        """
        t0 = to_datetime(data0.time)
        t1 = to_datetime(data1.time)
        overlap = array([t in t0 for t in t1])
        remove_in_data0 = overlap * priority
        remove_in_data1 = overlap * (True-priority)
        data0 = data0.sel(time=True-remove_in_data0)
        data1 = data1.sel(time=True-remove_in_data1)
        return xr.concat([data0, data1], dim='time')