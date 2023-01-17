#!/usr/bin/env python
from importlib import resources
import sys
from pathlib import Path
from .timers import Timer
from .obsdb import obsdb
# TODO: I had to re-introduce the next line as in earlier versions before Guillaume moved
# the rc tool invocacion to a new run file. I need to review this point later.
# from rctools import RcFile as rc
from .interfaces import Interface
from .obsoperator import transport
from .optimizer import Optimizer
from .Uncertainties import Uncertainties

name = 'lumia'
timer = Timer("Main timer") 

# Determine if we are in a local installation or in a system (or env-) installation (i.e. if pip was called with -e option or not):
prefix = resources.files("lumia")
if Path(sys.prefix) in prefix.parents :
    prefix = Path(sys.prefix)
else :
    prefix = prefix.parent
