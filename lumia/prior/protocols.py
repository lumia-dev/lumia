#!/usr/bin/env pythn

from typing import Protocol, Hashable, Any, Iterable
from dataclasses import dataclass
from pint import Quantity, Unit
from ..optimizer.protocols import Mapping, ModelData
from numpy.typing import NDArray


@dataclass
class Category(Hashable, Protocol):
    horizontal_correlation: str
    temporal_correlation: str
    optimization_interval: str
    total_uncertainty: Quantity
    unit_optim: Unit
    tracer: str
    name: str


@dataclass
class Mapping(Mapping, Protocol):
    model_data: ModelData

    @property
    def optimized_categories(self) -> Iterable[Category]: return ...

    def coarsen_cat(self, cat: Category, data: NDArray, value_field: str = 'prior_uncertainty') -> NDArray: ...


