#!/usr/bin/env python

from loguru import logger
import os
from pandas import Timedelta
from transport.observations import Observations
from transport.emis import Emissions
from transport.core import Model
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


class Stilt(Model):
    _footprint_class = StiltFootprintFile

    @property
    def footprint_class(self):
        return self._footprint_class


if __name__ == '__main__':
    import sys
    from argparse import ArgumentParser, REMAINDER

    p = ArgumentParser()
    p.add_argument('--setup', action='store_true', default=False, help="Setup the transport model (copy footprints to local directory, check the footprint files, ...)")
    p.add_argument('--forward', '-f', action='store_true', default=False, help="Do a forward run")
    p.add_argument('--adjoint', '-a', action='store_true', default=False, help="Do an adjoint run")
    p.add_argument('--footprints', '-p', help="Path where the footprints are stored")
    p.add_argument('--check-footprints', action='store_true', help='Determine which footprint file correspond to each observation')
    p.add_argument('--copy-footprints', default=None, help="Path where the footprints should be copied during the run (default is to read them directly from the path given by the '--footprints' argument")
    p.add_argument('--adjtest', '-t', action='store_true', default=False, help="Perform and adjoint test")
    p.add_argument('--serial', '-s', action='store_true', default=False, help="Run on a single CPU")
    p.add_argument('--tmp', default='/tmp', help='Path to a temporary directory where (big) files can be written')
    p.add_argument('--ncpus', '-n', default=os.cpu_count())
    p.add_argument('--max-footprint-length', type=Timedelta, default='14D')
    p.add_argument('--verbosity', '-v', default='INFO')
    p.add_argument('--obs', required=True)
    p.add_argument('--emis')#, required=True)
    p.add_argument('args', nargs=REMAINDER)
    args = p.parse_args(sys.argv[1:])

    # Set the verbosity in the logger (loguru quirks ...)
    logger.remove()
    logger.add(sys.stderr, level=args.verbosity)

    obs = Observations.read(args.obs)

    if args.check_footprints or 'footprint' not in obs.columns:
        obs.check_footprints(args.footprints, StiltFootprintFile, local=args.copy_footprints)

    model = Stilt(parallel=not args.serial, ncpus=args.ncpus, tempdir=args.tmp)

    emis = Emissions.read(args.emis)
    if args.forward:
        obs = model.run_forward(obs, emis)
        obs.write(args.obs)

    elif args.adjoint :
        adj = model.run_adjoint(obs, emis)
        adj.write(args.emis)

    elif args.adjtest :
        model.adjoint_test(obs, emis)