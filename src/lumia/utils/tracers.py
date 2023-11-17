#!/usr/bin/env python

from dataclasses import dataclass
from .units import units_registry as units
from pint import Unit


@dataclass
class Specie:
    unit_emis   : Unit
    unit_mix    : Unit
    unit_budget : Unit
    unit_optim  : Unit

    def __hash__(self):
        # This is a workaround for python>=3.11, where dataclasses only accept hashable objects as defaults (but don't actually check the hash)
        raise NotImplementedError


CO2 = Specie(
    unit_emis=units('umol/m**2/s').units, 
    unit_mix=units('ppm').units,
    unit_budget=units('PgC').units,
    unit_optim=units('umol').units
)

CH4 = Specie(
    unit_emis=units('nmol/m**2/s').units, 
    unit_mix=units('ppb').units,
    unit_budget=units('TgCH4').units,
    unit_optim=units('nmol').units
)

BC = Specie(
    unit_emis=units('umol/m**2/s').units,
    unit_mix=units('ppt').units,
    unit_budget=units('GgC').units,
    unit_optim=units('umol').units
)


@dataclass
class Species:
    co2 : Specie = CO2
    ch4 : Specie = CH4
    bc  : Specie = BC

    def __getitem__(self, tracer_name):
        return getattr(Species, tracer_name.lower())


species = Species()
