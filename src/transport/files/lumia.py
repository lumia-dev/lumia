#!/usr/bin/env python

from pandas import Timedelta, Timestamp, DataFrame, TimedeltaIndex, concat
import h5py
from gridtools import Grid
from numpy import inf
from loguru import logger
from typing import List
from types import SimpleNamespace
from dataclasses import asdict


class LumiaFootprintFile(h5py.File):
    maxlength : Timedelta = inf
    __slots__ = ['shift_t', 'origin', 'timestep', 'grid']

    def __init__(self, *args, maxlength: Timedelta = inf, **kwargs):
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

    @property
    def endpoints(self) -> DataFrame | None:
        # Get list of footprints that have a "background" subgroup:
        footprints_with_background = [f for f in self.footprints if 'background' in self[f]]
        
        # If endpoints have not been calculated by FLEXPART, just return None
        if not footprints_with_background :
            return None
        
        # Retrieve all endpoints and return them in as a DataFrame:
        return self.get_endpoints(footprints_with_background)
        
    def get_endpoint(self, obsid : str) -> DataFrame:
        
        df = DataFrame(dict(
            lon = self[obsid]['background']['lon'][:],
            lat = self[obsid]['background']['lat'][:],
            height = self[obsid]['background']['height'][:],
            active = self[obsid]['background']['active'][:],
            mass = self[obsid]['background']['mass'][:],
            pressure = self[obsid]['background']['pressure'][:],
            time = self.origin + TimedeltaIndex(self[obsid]['background']['time'][:], unit='s')
        ))
        df.loc[:, 'obsid'] = obsid
        return df

    def get_endpoints(self, obsids : List[str]) -> DataFrame :
        return concat([self.get_endpoint(obsid) for obsid in obsids])

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

        #Convert to s/m
        if self[obsid]['sensi'].attrs.get('units') == 's':
            sensi /= 100

        #if Timestamp(self[obsid]['sensi'].attrs.get('runflex_version', '2000.1.1')) < Timestamp(2022, 9, 1):
        #    sensi *= 0.0002897

        # If the footprint is empty, return here:
        if len(itims) == 0:
            return SimpleNamespace(shift_t=0, itims=itims, ilats=ilats, ilons=ilons, sensi=sensi)

        # sometimes, the footprint will have non-zero sentivity for the time-step directly after the observation time,
        # because FLEXPART calculates the concentration after releasing the particles, and before going to the next step.
        # This causes issues at the end of the simulations, as the corresponding emissions aren't available. The work
        # around below deletes these sensitivity components and re-attribute their values to the time step just before the obs

        # Check if the time of the last time step is same as release time (it should be lower by 1 timestep normally)
        # if it's the case, decrement that time index by 1
        if self.origin + itims[-1] * self.timestep == Timestamp(self[obsid].attrs['release_end']):
            ii = ilons[itims == itims[-1]]
            jj = ilats[itims == itims[-1]]
            s = sensi[itims == itims[-1]]
            sensi[(itims == itims[-1] - 1) & (ilats == jj) & (ilons == ii)] += s
            sensi = sensi[:-len(s)]
            ilons = ilons[:-len(s)]
            ilats = ilats[:-len(s)]
            itims = itims[:-len(s)]

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
