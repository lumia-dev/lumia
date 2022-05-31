#!/usr/bin/env python
import os
import logging
from transport.core import Model, Footprint, Emissions
from lumia.obsdb import obsdb
from netCDF4 import Dataset
from typing import List
from types import SimpleNamespace
from pandas import Timedelta, Timestamp, DataFrame
from gridtools import Grid
from numpy import array, nan


class Observations(DataFrame):

    @classmethod
    def read(cls, filename) -> "Observations":
        return cls(obsdb(filename).observations)

    def check_footprints(self, path: str) -> None:
        # Create the file names:
        fnames = [f'{o.code.lower()}.{o.height:.0f}m.{o.time:%Y-%m.hdf}' for o in self.itertuples()]
        self.loc[:, 'footprint'] = fnames
        exists = array([os.path.exists(os.path.join(path, f)) for f in fnames])
        self.loc[~exists, 'footprint'] = nan

        # Construct the obs ids:
        obsids = self.site + self.height.map('.{:.0f}m.'.format) + self.time.dt.strftime('%Y%m%d-%H%M%S')
        self.loc[exists, 'obsid'] = obsids


class LumiaFootprintFile(Dataset):
    __slots__ = ['shift_t']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.shift_t = 0

    @property
    def footprints(self) -> List[Footprint]:
        return list(self.groups.keys())

    def align(self, grid: Grid, timestep: Timedelta, origin: Timestamp):
        assert Grid(latc=grid.latc, lonc=grid.lonc) == Grid(latc=self.variables['latitudes'][:], lonc=self.variables['longitudes'][:])
        assert timestep == Timedelta(seconds=self.tres)
        shift_t = (self.origin - origin)/timestep
        assert int(shift_t) - shift_t == 0
        self.shift_t = int(shift_t)

    def __getitem__(self, obsid) -> SimpleNamespace :
        itims = self[obsid]['itims'][:] + self.shift_t
        ilons = self[obsid]['ilons'][:]
        ilats = self[obsid]['ilats'][:]
        sensi = self[obsid]['sensi'][:]
        sel = itims >= 0
        return SimpleNamespace(itims=itims[sel], ilons=ilons[sel], ilats=ilats[sel], sensi=sensi[sel])


class MultiTracer(Model):
    _footprint_class = LumiaFootprintFile

    @property
    def footprint_class(self):
        return self._footprint_class


if __name__ == '__main__':
    import sys
    from argparse import ArgumentParser, REMAINDER

    # Avoid excessive warning messages
    logging.captureWarnings(True)

    p = ArgumentParser()
    p.add_argument('--forward', '-f', action='store_true', default=False, help="Do a forward run")
    p.add_argument('--adjoint', '-a', action='store_true', default=False, help="Do an adjoint run")
    p.add_argument('--footprints', '-p', help="Path where the footprints are stored")
    p.add_argument('--adjtest', '-t', action='store_true', default=False, help="Perform and adjoint test")
    p.add_argument('--serial', '-s', action='store_true', default=False, help="Run on a single CPU")
    p.add_argument('--ncpus', '-n', default=None)
    p.add_argument('--verbosity', '-v', default='INFO')
    p.add_argument('--db', required=True)
    p.add_argument('--emis', required=True)
    p.add_argument('--check-footprints', action='store_true', help="Locate the footprint files and check them. Should be set to False if a `footprints` column is already present in the observation file", dest='checkFootprints')
    p.add_argument('args', nargs=REMAINDER)
    args = p.parse_args(sys.argv[1:])

    model = MultiTracer(parallel=not args.serial, ncpus=args.ncpus)

    emis = Emissions.read(args.emis)
    obs = Observations.read(args.obs)

    if args.check_footprints:
        obs.check_footprints(args.footprints)

    if args.forward:
        obs = model.run_forward(obs, emis)
        obs.write(args.db)

    elif args.adjoint :
        adj = model.run_adjoint(obs, emis)
        adj.write(args.emis)

    elif args.adjtest :
        model.adjoint_test(obs, emis)