#!/usr/bin/env python

from rctools import RcFile
from omegaconf import OmegaConf
import lumia


# Prepend paths with the root of the lumia folder,
# e.g: ${lumia:transport/multitracer.py} will be converted to Path(/lumia/library/path/transport/multitracer.py)

# with the "/lumia/library/path" part being detected automatically
OmegaConf.register_new_resolver('lumia', lambda p: lumia.prefix / p)
