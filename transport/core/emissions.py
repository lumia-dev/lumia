#!/usr/bin/env python

from typing import Union, Iterator, List
from pandas import DataFrame, Timedelta, Timestamp
from pandas.tseries.frequencies import to_offset
from dataclasses import dataclass
from h5py import File
import netCDF4 as nc
import xarray as xr
from numpy import ndarray, array, append
from types import SimpleNamespace


@dataclass
class Times:
    time_start: ndarray
    timestep: Timedelta

    @property
    def min(self) -> Timestamp:
        return Timestamp(self.time_start.min())


class EmissionField(xr.DataArray):
    __slots__ = []

    def __init__(self, grid=None, time=None, tracer=None):
        super().__init__(
            dims=['time', 'lat', 'lon'],
            coords={'time': time.time_start, 'lat': grid.latc, 'lon': grid.lonc},
            attrs={'tracer': tracer})

    @property
    def grid(self) -> SimpleNamespace:
        return SimpleNamespace(
            lonc=self.lon.data,
            latc=self.lat.data
        )

    @property
    def times(self) -> Times:
        return Times(
            time_start=array(self.time.to_dict()['data']),
            timestep=Timedelta(to_offset(self.timestep))
        )

    def save(self, fname: str, field: str) -> None:
        with File(fname, 'w') as fid :
            fid[field] = self.data.values

    def __setitem__(self, indices, value):
        self.data.values[indices] = value

    def __getitem__(self, indices) -> ndarray :
        return self.data.values[indices]


class EmissionFields(xr.Dataset):
    __slots__ = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def grid(self) -> SimpleNamespace:
        return SimpleNamespace(
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
    def categories(self) -> List[str]:
        return self.attrs['categories']

    @property
    def tracer(self) -> str:
        return self.attrs['tracer']

    def __setitem__(self, key, value):
        self[key].data = value

    def __getitem__(self, key):
        return self[key].data

    def setzero(self) -> None:
        for cat in self.categories :
            self[cat].data *= 0.

    @classmethod
    def open_dataset(cls, source: Union[str, nc.Group]):
        if isinstance(source, str):
            source = nc.Dataset(source, 'r')
        ds = xr.open_dataset(xr.backends.NetCDF4DataStore(source))
        return cls(data_vars=ds.data_vars, coords=ds.coords, attrs=ds.attrs)


@dataclass
class Emissions(dict):

    @property
    def tracers(self) -> Iterator[EmissionFields]:
        for tracer in self.values():
            return tracer

    @classmethod
    def read(cls, filename) -> "Emissions":
        obj = cls()
        with nc.Dataset(filename, 'r') as fid :
            if 'tracers' in fid.ncattrs():
                tracers = fid.tracers
            else :
                tracers = list(fid.groups.keys())
            for tracer in tracers :
                obj[tracer] = EmissionFields.open_dataset(fid[tracer])
        return obj

    def write(self, fname: str) -> None:
        for trname, tracer in self.items() :
            tracer.to_netcdf(fname, group=trname)

    def asvec(self) -> ndarray:
        """
        Simple conversion of the emissions to an array form, for adjoint test purpose:
        """
        vec = array(())
        for tracer in self.tracers :
            for cat in self[tracer].categories :
                vec = append(vec, self[tracer][cat].reshape(-1))
        return vec
