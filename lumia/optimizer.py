#!/usr/bin/env python

class Optimizer(object):
    def __init__(self, rcf, control, obsop):
        self.rcf = rcf          # Settings
        self.obsop = obsop      # Model interface instance (initialized!)
        self.control = control  # modelData structure, containing the optimization data
        