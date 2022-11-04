#!/usr/bin/env python

import logging
from numpy import shape, zeros, dot
from tqdm import tqdm

logger = logging.getLogger(__name__)


def init():
    pass


def g_to_gc(G_state, Temp_Lt, Hor_Lt, g, ipos):
    n_state = len(G_state)
    nt = shape(Temp_Lt)[0]
    nhor = shape(Hor_Lt)[0]
    g_c = zeros([n_state])
    for i in tqdm(range(nt), desc='preconditioning gradient', leave=False):
        for j in range(nt):
            g_c[ipos+i*nhor:ipos+(i+1)*nhor] += dot(Temp_Lt[i,j]*Hor_Lt, G_state[ipos+j*nhor:ipos+(j+1)*nhor] * g[ipos+j*nhor:ipos+(j+1)*nhor])
    return g_c


def xc_to_x(G_state, Temp_L, Hor_L, x_c, ipos):
    n_state = len(G_state)
    nt = shape(Temp_L)[0]
    nhor = shape(Hor_L)[0]
    x = zeros(n_state)
    for i in tqdm(range(nt), desc='xc_to_x', leave=True):
        for j in tqdm(range(nt), desc='step %i/%i'%(i, nt), leave=False):
            x[ipos+i*nhor:ipos+(i+1)*nhor] += G_state[ipos+i*nhor:ipos+(i+1)*nhor]* dot(Temp_L[i,j]*Hor_L, x_c[ipos+j*nhor:ipos+(j+1)*nhor])
    return x