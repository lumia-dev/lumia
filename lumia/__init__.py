#!/usr/bin/env python
name = 'lumia'
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__).addHandler(logging.NullHandler())
from .obsdb import obsdb
from .rctools import rc