#!/usr/bin/env python
name = 'lumia'
import sys

try :
    from tqdm import tqdm

except :
    class fake_tqdm:
        def __init__(self):
            pass

        def tqdm(self, iterable, *args, **kwargs):
            return iterable

        def write(self, message):
            sys.stdout.write(message)
            sys.stdout.write('\n')

    tqdm = fake_tqdm()

from .Tools import logging_tools
from .obsdb import obsdb
from rctools import rc
from .interfaces import Interface
from .obsoperator import transport
from .optimizer import Optimizer
#from .control import Control
from .Uncertainties import Uncertainties

# Setup the $LUMIA_ROOT environment variable
import os
os.environ['LUMIA_ROOT'] = os.path.dirname(__file__)