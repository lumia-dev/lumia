#!/usr/bin/env python

import os, logging
from lumia.Tools.logging_tools import colorize
import inspect
logger = logging.getLogger(__name__)


def checkDir(dirname, silent=False):
    if dirname == '':
        return
    if not os.path.exists(dirname): 
        os.makedirs(dirname)
        if not silent :
            frame = inspect.stack()[1]
            mod = inspect.getmodule(frame[0]).__name__
            logger.info(colorize(f"Create path <s>{dirname}</s> (called by {mod} at line {2})"))
