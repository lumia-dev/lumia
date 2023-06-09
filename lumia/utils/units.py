#!/usr/bin/env python
from pint import UnitRegistry, set_application_registry


units_registry = UnitRegistry()
units_registry.define('ppm = umol/mol')
units_registry.define('ppb = nmol/mol')
units_registry.define('m2 = m**2')
units_registry.define('gC = 1/12.011 mol')
units_registry.define('gCO2 = 1/44 mol')
units_registry.define('gCH4 = 1/16.04 mol')

set_application_registry(units_registry)