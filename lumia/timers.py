#!/usr/bin/env python

from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class Timer:
    def __init__(self, name):
        self.t0 = datetime.now()
        self.name = name

    def info(self):
        t1 = datetime.now()
        logger.info(f"{self.name}: {(t1-self.t0).total_seconds()} elapsed")
        self.t0 = t1