#!/usr/bin/env python
name = 'lumia'
from .Tools import logging_tools
from .obsdb import obsdb
from lumia.Tools.rctools import rc
from .interfaces import Interface
from .obsoperator import transport
from .optimizer import Optimizer
from .control import Control
from .Uncertainties import Uncertainties

# Setup the $LUMIA_ROOT environment variable
import os
os.environ['LUMIA_ROOT'] = os.path.dirname(__file__)


