#!/usr/bin/env python

import logging
import ray
from numpy import zeros_like, dot

logger = logging.getLogger(__name__)

def init():
    ray.init()

@ray.remote
def xc_to_x_inner(G_state, Temp_L, Hor_L, x_c, ipos, i, j):
    x = zeros_like(G_state)
    nh = Hor_L.shape[0]
    x[ i*nh : (i+1)*nh ] += G_state[ i*nh : (i+1)*nh ] * dot( Temp_L[i,j]*Hor_L, x_c[ j*nh : (j+1)*nh ])
    return x

def xc_to_x(G_state, Temp_L, Hor_L, x_c, ipos, dummy, path=None):
    gs_id = ray.put(G_state)
    t_id = ray.put(Temp_L)
    h_id = ray.put(Hor_L)
    xc_id = ray.put(x_c)

    nt = Temp_L.shape[0]
    x = zeros_like(x_c)
    remotes = []
    for i in range(nt):
        for j in range(nt):
            remotes.append(xc_to_x_inner.remote(gs_id, t_id, h_id, xc_id, ipos, i, j))
    for r in remotes :
        x += ray.get(r)
    return x

@ray.remote
def g_to_gc_inner(G_state, Temp_Lt, Hor_Lt, g, ipos, i, j):
    gc = zeros_like(G_state)
    nh = Hor_Lt.shape[0]
    gc[ ipos + i*nh : ipos + (i+1)*nh] = dot( Temp_Lt[i,j] * Hor_Lt, G_state[ ipos + j*nh : ipos + (j+1)*nh] * g[ ipos + j*nh : ipos + (j+1)*nh])
    return gc

def g_to_gc(G_state, Temp_Lt, Hor_Lt, g, ipos, dummy, path=None):
    gs_id = ray.put(G_state)
    tt_id = ray.put(Temp_Lt)
    ht_id = ray.put(Hor_Lt)
    g_id = ray.put(g)

    nt = Temp_Lt.shape[0]
    gc = zeros_like(G_state)
    remotes = []
    for i in range(nt):
        for j in range(nt):
            remotes.append(g_to_gc_inner.remote(gs_id, tt_id, ht_id, g_id, ipos, i, j))
    for r in remotes :
        gc += ray.get(r)
    return gc