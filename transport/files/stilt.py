#!/usr/bin/env python

from pandas import Timedelta
from numpy import inf, zeros
from netCDF4 import Dataset
from typing import List
from datetime import datetime
from pandas import Timestamp
from gridtools import Grid
from types import SimpleNamespace


class StiltFootprintFile:
    # hard-coded value since info is not available in the files
    timestep = Timedelta(hours=1)

    def __init__(self, *args, maxlength:Timedelta=inf, **kwargs):
        self.ds = Dataset(*args, **kwargs)
        times = [datetime.strptime(_.split('_')[1], '%Y%m%d%H') for _ in list(self.ds.variables) if _.endswith('val')]
        code = self.ds.Filename.split('_')[1]
        height = self.ds.sampling_height.split()[0]
        self._footprints = {f'{code}.{height}m.{tt.strftime("%Y%m%d-%H%M%S")}' : tt for tt in times}
        self.grid = Grid(
            lon0=self.ds.lon_ll,
            lat0=self.ds.lat_ll,
            dlon=self.ds.lon_res,
            dlat=self.ds.lat_res,
            nlon=self.ds.numpix_x,
            nlat=self.ds.numpix_y
        )

        self.maxlength : Timedelta = inf
        self.dest = SimpleNamespace(grid = None, origin = None, timestep = None)

    @property
    def footprints(self) -> List[str]:
        return list(self._footprints.keys())

    def align(self, grid: Grid, timestep: Timedelta, origin: Timestamp):
        self.dest.grid = grid
        self.dest.origin = origin
        self.dest.timestep = timestep

    def get(self, obsid: str):
        time = self._footprints[obsid]

        # Read raw data
        # The lat/lon coordinates in the file indicate the ll corner, but we want the center. So add half steps
        lons = self.ds[time.strftime('ftp_%Y%m%d%H_lon')][:] + self.grid.dlon/2.
        lats = self.ds[time.strftime('ftp_%Y%m%d%H_lat')][:] + self.grid.dlat/2.
        npt = self.ds[time.strftime('ftp_%Y%m%d%H_indptr')][:]
        sensi = self.ds[time.strftime('ftp_%Y%m%d%H_val')][:]

        # Select :
        select = (lats >= self.dest.grid.lat0) * (lats <= self.dest.grid.lat1)
        select *= (lons >= self.dest.grid.lon0) * (lons <= self.dest.grid.lon1)

        # Reconstruct ilats/ilons :
        ilats = (lats[select]-self.dest.grid.lat0)/self.dest.grid.dlat
        ilons = (lons[select]-self.dest.grid.lon0)/self.dest.grid.dlon
        ilats = ilats.astype(int)
        ilons = ilons.astype(int)

        # Reconstruct itims
        itims = zeros(len(lons))
        prev = 0
        for it, n in enumerate(npt[1:]):
            itims[prev:prev+n] = -it
            prev = n
        itims = itims.astype(int)[select]

        # align the footprint
        # (now it's aligned with the obs time)
        assert self.timestep == self.dest.timestep
        shift_t = (time - self.dest.origin) / self.dest.timestep
        assert int(shift_t) - shift_t == 0
        shift_t = int(shift_t)

        return SimpleNamespace(
            name = obsid,
            shift_t = shift_t,
            itims = itims,
            ilons = ilons,
            ilats = ilats,
            sensi = sensi[select]
        )

