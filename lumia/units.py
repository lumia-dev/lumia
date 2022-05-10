#!/usr/bin/env python
from dataclasses import dataclass, field
from pint import UnitRegistry


units_registry = UnitRegistry()
units_registry.define('ppm = umol/mol')
units_registry.define('ppb = nmol/mol')
units_registry.define('m2 = m**2')
units_registry.define(f'gC = 1/12.011 mol')