#!/usr/bin/env python
import sys

try :
    from tqdm import tqdm
except ModuleNotFoundError :
    class fake_tqdm:
        def __init__(self):
            pass

        def tqdm(self, iterable, *args, **kwargs):
            return iterable

        def write(self, message):
            sys.stdout.write(message)
            sys.stdout.write('\n')

    tqdm = fake_tqdm()

class Paths:
    _initialized : bool = False

    def __init__(self):
        self._temp = None

    def setup(self, rcf):
        self._temp = rcf.get('path.temp')
        self._initialized = True

    @property
    def temp(self):
        if self._initialized :
            return self._temp
        else :
            raise RuntimeError(f'{__name__}.paths has been instantiated but has yet to be initialized')


_data = {}
_data['paths'] = Paths()


def __getattr__(name):
    if name in _data :
        return _data[name]
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


from .timers import Timer

from .Tools import logging_tools
from .obsdb import obsdb
from rctools import RcFile as rc
from .interfaces import Interface
from .obsoperator import transport
from .optimizer import Optimizer
from .Uncertainties import Uncertainties

# Setup the $LUMIA_ROOT environment variable
import os

name = 'lumia'
timer = Timer("Main timer") 

os.environ['LUMIA_ROOT'] = os.path.dirname(__file__)