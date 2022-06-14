#!/usr/bin/env python

from datetime import datetime
from typing import Iterator, List
from pandas import Timedelta, Timestamp
from pandas.tseries.frequencies import to_offset
from dataclasses import dataclass
import netCDF4 as nc
import xarray as xr
from numpy import ndarray, array, append
from types import SimpleNamespace
from loguru import logger


@dataclass
class Grid:
    lonc: ndarray
    latc: ndarray

    @property
    def nlon(self) -> int:
        return len(self.lonc)

    @property
    def nlat(self) -> int:
        return len(self.latc)


@dataclass
class Times:
    time_start: ndarray
    timestep: Timedelta

    @property
    def min(self) -> Timestamp:
        return Timestamp(self.time_start.min())

    @property
    def nt(self) -> int:
        return len(self.time_start)


class EmissionFields(xr.Dataset):
    __slots__ = []

    @property
    def grid(self) -> SimpleNamespace:
        return Grid(
            lonc=self.lon.data,
            latc=self.lat.data
        )

    @property
    def times(self) -> Times:
        return Times(
            time_start=array(self.time.to_dict()['data']),
            timestep=Timedelta(to_offset(self.timestep))
        )

    @property
    def start(self) -> datetime:
        return self.time.to_dict()['data'][0]

    @property
    def categories(self) -> List[str]:
        return self.attrs['categories']

    @property
    def tracer(self) -> str:
        return self.attrs['tracer']

    def setzero(self) -> None:
        for cat in self.categories :
            self[cat].data *= 0.

    @classmethod
    def open_dataset(cls, source: str, group: str=None):
        with xr.open_dataset(source, group=group) as ds :
            obj = cls(data_vars=ds.data_vars, coords=ds.coords, attrs=ds.attrs)
            obj.load()
        return obj

    # def to_netcdf_(self, filename: str, group=None, **args) -> str:
    #     """
    #     This re-implements the default xarray to_netcdf method, but not using xarray, which leads to significant performance improvement (I don't understand why ...). The usage is the same:
    #     - filename ==> path to the output file
    #     - group (optional) ==> name of the netcdf group. If no name provided, then data is stored at the root of the file
    #     The filename is returned, for convenience.
    #     """
    #     with nc.Dataset(filename, 'w') as ds :
    #         if group is not None :
    #             ds = ds.createGroup(group)

    #         # Create dimensions:
    #         for dim in self.dims:
    #             ds.createDimension(dim, self.dims[dim])

    #         # Create variables:
    #         for var in self.data_vars:
    #             ds.createVariable(var, self[var].dtype, self[var].dims)
    #             ds[var][:] = self[var].data
    
    #             # Copy var attributes:
    #             for k, v in self[var].attrs.items():
    #                 setattr(ds[var], k, v)
    
    #         # Create coordinate variables:
    #         for var in self.coords:
    #             vartype = self[var].dtype
    #             if vartype == 'datetime64[ns]':
    #                 data = (self[var].data - self[var].data[0]) / 1.e9
    #                 ds.createVariable(var, 'int64', self[var].dims)
    #                 ds[var].units = f'seconds since {self[var][0].dt.strftime("%Y-%m-%d").data}'
    #                 ds[var].calendar = 'proleptic_gregorian'
    #                 ds[var][:] = data
    #             else :
    #                 ds.createVariable(var, self[var].dtype, self[var].dims)
    #                 ds[var][:] = self[var].data

    #         # Global attributes
    #         for k, v in self.attrs.items():
    #             setattr(ds, k, v)

    #         return filename


class Emissions(dict):

    @property
    def tracers(self) -> Iterator[EmissionFields]:
        for tracer in self.values():
            yield tracer

    @classmethod
    def read(cls, filename) -> "Emissions":
        obj = cls()
        with nc.Dataset(filename, 'r') as fid :
            if 'tracers' in fid.ncattrs():
                tracers = fid.tracers
            else :
                tracers = list(fid.groups.keys())
        for tracer in tracers :
            obj[tracer] = EmissionFields.open_dataset(filename, group=tracer)
        return obj

    def write(self, fname: str) -> None:
        for trname, tracer in self.items() :
            tracer.to_netcdf(fname, group=trname, engine='h5netcdf')

    def asvec(self) -> ndarray:
        """
        Simple conversion of the emissions to an array form, for adjoint test purpose:
        """
        vec = array(())
        for tracer in self.tracers :
            for cat in self[tracer].categories :
                vec = append(vec, self[tracer][cat].reshape(-1))
        return vec
