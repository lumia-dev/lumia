#!/usr/bin/env pythomn

from pandas import DataFrame
from typing import Protocol
from pathlib import Path
from numpy.typing import NDArray


class Observations(Protocol):
    observations : DataFrame
    sites : DataFrame

    def to_hdf(self, path : Path | str) -> Path:
        """
        Method that writes the observations to a HDF5 format that can be read by the transport model.
        :param path: destination file of the path to be written
        :return: the path where the emissions have been written
        """
        ...

    @classmethod
    def from_hdf(cls, path: Path | str) -> "Observations":
        """
        Method that creates an "Observations" instance by reading an HDF(5) file
        :param path: location of the HDF observation file
        :return: New instance of "Observations"
        """
        ...

    def save_tar(self, path : Path | str) -> Path:
        """
        Method that writes the observations to a tar.gz format (compressed CSV tables)
        :param path: destination path for the tar.gz file
        :return: path where the file has been written (for convenience)
        """

    @property
    def sigma(self) -> NDArray: ...

    @property
    def mismatch(self) -> NDArray: ...
