#!/usr/bin/env python

from dataclasses import dataclass, asdict
from collections.abc import Hashable
from pandas import DateOffset
from pint import Quantity, Unit
from numpy import nan, array
from typing import Dict
from lumia.utils.units import units_registry
from loguru import logger


@dataclass
class Constructor:
    _value: str | dict = None

    def __post_init__(self):
        if isinstance(self._value, Constructor):
            self._value = self._value.dict

    @property
    def dict(self) -> dict:
        if isinstance(self._value, dict):
            return self._value
        cats = [c.split('*') for c in self._value.replace(' ', '').replace('-', '+-1*').split('+')]
        return {v[-1]: array(v[:-1], dtype=float).prod() for v in cats}

    @property
    def str(self) -> str:
        if isinstance(self._value, str):
            return self._value
        return '+'.join([f'{v}*{k}' for (k, v) in self._value.items()])

    def __str__(self):
        return self.str

    @property
    def items(self):
        return self.dict.items

    @property
    def keys(self):
        return self.dict.keys


@dataclass
class Category(Hashable):
    name: str
    tracer: str
    optimized: bool = False
    optimization_interval: DateOffset = None
    apply_lsm: bool = True
    is_ocean: bool = False
    n_optim_points: int = None
    horizontal_correlation: str = None
    temporal_correlation: str = None
    total_uncertainty: Quantity = nan
    unit_emis: Quantity = None
    unit_mix: Quantity = None
    # unit_budget : Quantity = None
    unit_optim: Quantity = None
    meta: bool = False
    constructor: Constructor = None
    transported: bool = True

    def as_dict(self):
        # Should be deprecated ...
        return self.dict

    @property
    def dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, name, kwargs):
        return cls(name, **{k: v for k, v in kwargs.items() if k in cls.__dataclass_fields__})

    def __hash__(self):
        return hash((self.name, self.tracer))

    @classmethod
    def from_ncattrs(cls, attrs: Dict) -> "Category":
        for k, v in attrs.items():
            try :
                attrs[k] = cls.__annotations__[k](v)
            except TypeError:
                logger.warning(f"Could not convert value {k} to type {cls.__annotations__[k]}. Keeping the attribute as str.")
                attrs[k] = v
            attrs[k] = str(v)
        name = attrs['name']
        return cls.from_dict(name, **attrs)

    @property
    def ncattrs(self) -> Dict:
        attrs = {}
        for k, v in self.dict.items():
            attrs[k] = str(v)
        return attrs


def attrs_to_nc(attrs: dict) -> dict:
    """
    Convert items of a dictionary that cannot be written as netCDF attributes to a netCDF-compliant format.
    """
    # Make sure we work on a copy of the dictionary
    attrs = attrs.copy()

    # Store the name of the variables that have been converted
    to_bool = []
    to_units = []
    to_none = []

    # Do the actual conversion
    for k, v in attrs.items():
        if isinstance(v, bool):
            attrs[k] = int(v)
            to_bool.append(k)
        if isinstance(v, Unit):
            attrs[k] = str(v)
            to_units.append(k)
        if isinstance(v, Constructor):
            attrs[k] = v.str
        if v is None :
            attrs[k] = str(v)
            to_none.append(k)

    # add attributes listing the variable conversions (for converting back)
    if to_bool:
        attrs['_bool'] = to_bool
    if to_units:
        attrs['_units'] = to_units
    if to_none:
        attrs['_none'] = to_none
    return attrs


def nc_to_attrs(attrs: dict) -> dict:
    for attr in attrs.get('_bool', []):
        attrs[attr] = bool(attrs[attr])
    for attr in attrs.get('_units', []):
        attrs[attr] = units_registry(attrs[attr]).units
    for attr in attrs.get('_none', []):
        attrs[attr] = None
    if '_bool' in attrs:
        del attrs['_bool']
    if '_units' in attrs:
        del attrs['_units']
    if '_none' in attrs:
        del attrs['_none']
    if 'constructor' in attrs:
        attrs['constructor'] = Constructor(attrs['constructor'])
    return attrs