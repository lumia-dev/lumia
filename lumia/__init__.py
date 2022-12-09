#!/usr/bin/env python
from loguru import logger
from importlib import resources
import sys
from pathlib import Path


# Disabled: tqdm is available everywhere now, so following hack should never be needed
# try :
#     from tqdm import tqdm
# except ModuleNotFoundError :
#     class fake_tqdm:
#         def __init__(self):
#             pass
#
#         def tqdm(self, iterable, *args, **kwargs):
#             return iterable
#
#         def write(self, message):
#             sys.stdout.write(message)
#             sys.stdout.write('\n')
#
#     tqdm = fake_tqdm()


class Paths:
    _initialized : bool = False

    def __init__(self):
        self._temp = None

    def setup(self, rcf):
        self._temp = rcf.get('path.temp')
        if not os.path.exists(self._temp):
            os.makedirs(self._temp)
        self._initialized = True

    @property
    def temp(self):
        if self._initialized :
            logger.debug(f'lumia.paths.temp set to {self._temp}')
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

# from .Tools import logging_tools
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

# Determine if we are in a local installation or in a system (or env-) installation (i.e. if pip was called with -e option or not):
prefix = resources.files("lumia")
if Path(sys.prefix) in prefix.parents :
    prefix = Path(sys.prefix)
else :
    prefix = prefix.parent


