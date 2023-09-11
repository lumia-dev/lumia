#!/usr/bin/env python

from .utils.dconf import read_config
from .observations.observations import Observations
from . import models

from .utils.logging import setup_logging
setup_logging()
