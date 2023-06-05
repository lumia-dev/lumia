#!/usr/bin/env python

from datetime import datetime
import logging
import inspect
import os

logger = logging.getLogger(__name__)


class Timer:
    def __init__(self, name):
        self.t0 = datetime.now()
        self.name = name

    def info(self, msg=None):
        stack = inspect.stack()
        caller = inspect.getframeinfo(stack[1][0])
        t1 = datetime.now()
        msg = '' if msg is None else f'({msg})'
        logger.info(f"[{self.name}]{os.path.relpath(caller.filename)}:{caller.lineno}: {(t1-self.t0).total_seconds()} seconds elapsed {msg}")
        self.t0 = t1