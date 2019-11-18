#!/usr/bin/env python

import os

def checkDir(dirname):
    if not os.path.exists(dirname): os.makedirs(dirname)