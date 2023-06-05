#!/usr/bin/env python

import os#, logging
from lumia.Tools.logging_tools import colorize
import inspect
import sys
import subprocess
from loguru import logger


def checkDir(dirname, silent=False):
    if dirname == '':
        return
    if not os.path.exists(dirname): 
        os.makedirs(dirname)
        if not silent :
            frame = inspect.stack()[1]
            mod = inspect.getmodule(frame[0]).__name__
            logger.info(colorize(f"Create path <s>{dirname}</s> (called by {mod} at line {2})"))


def runcmd(cmd):
    logger.info(colorize(' '.join([str(x) for x in cmd]), 'g'))
    try :
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    except subprocess.CalledProcessError :
        logger.error("external command failed, exiting ...")
        raise subprocess.CalledProcessError
    for line in p.stdout:
        sys.stdout.buffer.write(line)
        sys.stdout.buffer.flush()