#!/usr/bin/env python

from typing import Protocol, Iterable
from pathlib import Path
import xarray as xr
from ...prior.protocols import Category


class Emissions(Protocol):
    def to_netcdf(self, path : Path | str, zlib : bool = True, only_transported : bool = False) -> Path | str:
        """
        Method that writes the emissions in a format that can be read by the transport model.
        :param path: destination file of the path to be written
        :param zlib: whether zlib compression should be used
        :param only_transported: whether non-transported categories should be written as well
        :return: the path where the emissions have been written
        """
        ...

    @property
    def transported_categories(self) -> Iterable[Category]: ...

    def __getitem__(self, item) -> xr.Dataset: ...
    
    
