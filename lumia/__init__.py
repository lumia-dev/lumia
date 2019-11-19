#!/usr/bin/env python
name = 'lumia'
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__).addHandler(logging.NullHandler())
from .obsdb import obsdb
from .rctools import rc
from .interfaces import Interface
from .obsoperator import transport
from .optimizer import Optimizer
from .minimizer import Minimizer
from .control import Control
from .Uncertainties import Uncertainties

# Setup the $LUMIA_ROOT environment variable
import os
os.environ['LUMIA_ROOT'] = os.path.dirname(__file__)