#!/usr/bin/env python

from typing import Protocol, Any, Dict, Hashable
from pandas import Series, DataFrame
from numpy.typing import NDArray
from pathlib import Path


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


class Departures(Protocol):
    """
    This class defines a template for the objects returned by the "forward_step" method of the optimizer, and used as argument for its "adjoint_step" method.
    """
    mismatch: Series    # model-data mismatches
    sigma: Series       # observation uncertainties (standard-deviation, incl. model error)
    
    
class ModelData(Protocol):
    """
    This class defines a template for the object passed to the model, via "calc_departures", and returned by the adjoint model (calc_departures_adj). 
    For now it's just a blank template ...
    """
    def __getitem__(self, item: Hashable) -> Any: ...
    
    
class Model(Protocol):
    """
    This class defines a template for the model object used by the optimizer
    """
    def setup_observations(observations: Observations) -> None: 
        """
        Provide the observations data to the model
        """
        ...
        
    @property
    def observations(self) -> DataFrame:
        """
        DataFrame storing the observations, their coordinates, model estimates, etc.
        """
        
    def calc_departures(
        self, 
        model_data: ModelData | Any, 
        step : str | None = None,
        setup_uncertainties: bool = False
        ) -> Departures: 
        """
        Calculates the departures + their uncertainties for a given set of model inputs
        """
        ...
    def calc_departures_adj(self, obs_departures : NDArray) -> ModelData: 
        """
        Calculate the adjoint model data corresponding to a vector of forcings (departures / sigma**2).
        """
        ...
        
        
class Mapping(Protocol):
    """
    The implementation of this class should handle conversions between data in the model format (i.e. implementation of the "ModelData" class, used by the relevant implementation of the "Model" class), and the optimization format (i.e. state-vector shaped 1D arrays).
    This includes operations such as aggregation/disaggregation, unit conversions, addition/substraction of non-optimized categories, etc.
    """
    def vec_to_struct(self, vector: NDArray) -> ModelData:
        """
        Constructs inputs for the (transport) model, based on a given state vector.
        """
        ...
        
    def vec_to_struct_adj(self, struct: Any | ModelData) -> NDArray:
        """
        Adjoint of Mapping.vec_to_struct
        """
        
        
class Prior(Protocol):
    """
    Template for objects storing prior information (prior estimates + uncertainties)
    """
    temporal_correlation : Dict
    horizontal_correlation : Dict
    sigmas : Dict
    vectors : DataFrame
    
    @property
    def size(self) -> int: return self.vectors.shape[0]
    
    @property
    def coordinates(self) -> DataFrame: 
        """
        """
        ...