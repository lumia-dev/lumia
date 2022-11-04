#!/usr/bin/env python

import logging
from numpy import shape, zeros, dot, array
from tqdm import tqdm
from multiprocessing import Pool

logger = logging.getLogger(__name__)


common = {}


def init():
    pass


def loop_g(j):
    nh = common['nh']
    return (common['tBt'][j] * common['hBt']) @ common['g'][j*nh:(j+1)*nh]


def loop_x(j):
    nh = common['nh']
    return common['g'] * ( common['tB'][j] * common['hB']) @ common['x_c'][j*nh:(j+1)*nh]


def g_to_gc(G_state, Temp_Lt, Hor_Lt, g, ipos, dummy, path=None):
    n_state = len(G_state)
    nt = shape(Temp_Lt)[0]
    nhor = shape(Hor_Lt)[0]
    g_c = zeros([n_state])

    common['hBt'] = Hor_Lt
    common['g'] = (G_state * g)[ipos:ipos+nt*nhor]
    common['nh'] = nhor

    for i in tqdm(range(nt), desc='preconditioning gradient', leave=False):
        common['tBt'] = Temp_Lt[i, :]
        with Pool() as p:
            g_c[ipos+i*nhor:ipos+(i+1)*nhor] = array(p.map(loop_g, range(nt), chunksize=1)).sum(0)
        #for j in range(nt):
        #    g_c[ipos+i*nhor:ipos+(i+1)*nhor] += dot(Temp_Lt[i,j]*Hor_Lt, G_state[ipos+j*nhor:ipos+(j+1)*nhor] * g[ipos+j*nhor:ipos+(j+1)*nhor])
    return g_c


def xc_to_x(G_state, Temp_L, Hor_L, x_c, ipos, dummy, path=None):
    n_state = len(G_state)
    nt = shape(Temp_L)[0]
    nhor = shape(Hor_L)[0]
    x = zeros(n_state)

    common['hB'] = Hor_L
    common['x_c'] = x_c[ipos:ipos+nt*nhor]
    common['nh'] = nhor

    for i in tqdm(range(nt), desc='xc_to_x', leave=True):
        common['g'] = G_state[ipos+i*nhor:ipos+(i+1)*nhor]
        common['tB'] = Temp_L[i, :]
        with Pool() as p:
            x[ipos+i*nhor:ipos+(i+1)*nhor] = array(p.map(loop_x, range(nt), chunksize=1)).sum(0)
        #for j in tqdm(range(nt), desc='step %i/%i'%(i, nt), leave=False):
        #    x[ipos+i*nhor:ipos+(i+1)*nhor] += G_state[ipos+i*nhor:ipos+(i+1)*nhor]* dot(Temp_L[i,j]*Hor_L, x_c[ipos+j*nhor:ipos+(j+1)*nhor])
    return x