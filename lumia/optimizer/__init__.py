#!/usr/bin/env python

from .optimizer import Var4D as optim_lanczos
from omegaconf import DictConfig
from .scipy_optimizer import Optimizer as optim_cg_scipy