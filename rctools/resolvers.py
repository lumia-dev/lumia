#!/usr/bin/env python

from omegaconf import OmegaConf
from pandas import Timestamp
from types import SimpleNamespace
from gridtools import Grid

# Automatic type conversions:
# resolvers[type] = SimpleNamespace(forward=(prefix, basetype_to_type), reverse=type_to_basetype), with:
#   - type : class of the object that needs to be converted to a valid omegaconf basetype
#   - prefix : prefix to be used in the yaml files (${prefix:value}
#   - basetype_to_type : lambda function used to convert the key value to requested type (e.g. convert ${ts:20180101} to Timestamp(2018,1,1)
#   - type_to_basetype : reverse operation (e.g. convert Timestamp(2018,1,1) to "${ts:20180101}".

resolvers = {
    # Timestamp
    Timestamp: SimpleNamespace(
        forward=('ts', lambda s: Timestamp(s)),
        reverse=lambda v: f'${{ts: {v}}}'),
    Grid: SimpleNamespace(
        forward=('Grid', lambda d: Grid(**d)),
        reverse=lambda v: f'${{'
                          f'Grid: {{lon0:{v.lon0}, lon1:{v.lon1}, lat0:{v.lat0}, lat1:{v.lat1}, dlon:{v.dlon}, dlat={v.dlat}}}'
                          f'}}')
}

for resolver in resolvers.values():
    OmegaConf.register_new_resolver(*resolver.forward)