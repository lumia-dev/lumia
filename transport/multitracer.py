#!/usr/bin/env python
import os
from loguru import logger
from transport.core import Model
from transport.emis import Emissions
from transport.observations import Observations
import h5py
from typing import List
from types import SimpleNamespace
from pandas import Timedelta, Timestamp
from gridtools import Grid
from numpy import inf
from dataclasses import asdict


class LumiaFootprintFile(h5py.File):
    maxlength : Timedelta = inf
    __slots__ = ['shift_t', 'origin', 'timestep', 'grid']

    def __init__(self, *args, maxlength:Timedelta=inf, **kwargs):
        super().__init__(*args, mode='r', **kwargs)
        self.shift_t = 0

        try :
            self.origin = Timestamp(self.attrs['origin'])
            self.timestep = Timedelta(seconds=abs(self.attrs['run_loutstep']))
            if self.maxlength != inf :
                self.maxlength /= self.timestep
            assert self['latitudes'].dtype in ['f4', 'f8']
            self.grid = Grid(latc=self['latitudes'][:], lonc=self['longitudes'][:])

        except AssertionError :
            self.grid = Grid(
                lon0=self.attrs['run_outlon0'], dlon=self.attrs['run_dxout'], nlon=len(self['longitudes'][:]),
                lat0=self.attrs['run_outlat0'], dlat=self.attrs['run_dyout'], nlat=len(self['latitudes'][:])
            )

        except KeyError:
            logger.warning(f"Coordinate variables (latitudes and longitudes) not found in file {self.filename}. Is the file empty?")

    @property
    def footprints(self) -> List[str]:
        return [k for k in self.keys() if isinstance(self[k], h5py.Group)]

    def align(self, grid: Grid, timestep: Timedelta, origin: Timestamp):
        assert Grid(latc=grid.latc, lonc=grid.lonc) == self.grid, f"Can't align the footprint file grid ({self.grid}) to the requested grid ({Grid(**asdict(grid))})"
        assert timestep == self.timestep, "Temporal grid mismatch"
        shift_t = (self.origin - origin)/timestep
        assert int(shift_t) - shift_t == 0
        self.shift_t = int(shift_t)

    def get(self, obsid) -> SimpleNamespace :
        itims = self[obsid]['itims'][:] 
        ilons = self[obsid]['ilons'][:]
        ilats = self[obsid]['ilats'][:]
        sensi = self[obsid]['sensi'][:]

        if self[obsid]['sensi'].attrs.get('units') == 's m3 kg-1':
            sensi *= 0.0002897

        #if Timestamp(self[obsid]['sensi'].attrs.get('runflex_version', '2000.1.1')) < Timestamp(2022, 9, 1):
        #    sensi *= 0.0002897

        # If the footprint is empty, return here:
        if len(itims) == 0:
            return SimpleNamespace(shift_t=0, itims=itims, ilats=ilats, ilons=ilons, sensi=sensi)

        # sometimes, the footprint will have non-zero sentivity for the time-step directly after the observation time. This is because FLEXPART calculates the concentration after releasing the particles, and before going to the next step. In this case, re-attribute the sensitivity to the previous time step.

        # Check if the time of the last time step is same as release time (it should be lower by 1 timestep normally)
        # if it's the case, decrement that time index by 1
        if self.origin + itims[-1] * self.timestep == Timestamp(self[obsid].attrs['release_end']):
            itims[-1] -= 1

        # Trim the footprint if needed
        sel = itims.max() - itims <= self.maxlength

        # Apply the time shift
        itims += self.shift_t

        # Exclude negative time steps
        if itims.min() < 0 :
            sel *= False

        return SimpleNamespace(
            name=obsid,
            shift_t=self.shift_t,
            itims=itims[sel], 
            ilons=ilons[sel], 
            ilats=ilats[sel], 
            sensi=sensi[sel])


class MultiTracer(Model):
    _footprint_class = LumiaFootprintFile

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

    # Set the max time limit for footprints:
    LumiaFootprintFile.maxlength = args.max_footprint_length

    if args.check_footprints or 'footprint' not in obs.columns:
        obs.check_footprints(args.footprints, LumiaFootprintFile, local=args.copy_footprints)

    model = MultiTracer(parallel=not args.serial, ncpus=args.ncpus, tempdir=args.tmp)
    emis = Emissions.read(args.emis)
    if args.forward:
        obs = model.run_forward(obs, emis)
        obs.write(args.obs)

    elif args.adjoint :
        adj = model.run_adjoint(obs, emis)
        adj.write(args.emis)

    elif args.adjtest :
        model.adjoint_test(obs, emis)