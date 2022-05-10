#!/usr/bin/env python

from dataclasses import dataclass, field
from lumia.units import units_registry as units
from pint import Unit


@dataclass
class Tracer:
    unit_emis : Unit
    unit_mix  : Unit


CO2 = Tracer(unit_emis=units('umol/m**2/s'), unit_mix=units('ppm'))
CH4 = Tracer(unit_emis=units('nmol/m**2/s'), unit_mix=units('ppb'))


@dataclass
class Tracers:
    co2 : Tracer = CO2
    ch4 : Tracer = CH4

    def get(self, tracer_name):
        return getattr(self, tracer_name.lower())


tracers = Tracers()