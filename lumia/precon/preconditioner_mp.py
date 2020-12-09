#!/usr/bin/env python

import logging
from multiprocessing import Pool
from numpy import zeros_like, dot
from tqdm import tqdm

logger = logging.getLogger(__name__)


class Xc_to_x_loop:
    def __init__(self, nt, Hor_L, x_c):
        self.Hor_L = Hor_L
        self.nt = nt
        self.nh = Hor_L.shape[0]
        self.x_c = x_c

    def apply(self, G_state, Temp_L):
        x = zeros_like(self.x_c)
        for j in range(self.nt):        
            x += G_state * dot(Temp_L[j]*self.Hor_L, self.x_c[j*self.nh: (j+1)*self.nh])
        return x


def xc_to_x(G_state, Temp_L, Hor_L, x_c, ipos, dummy, path=None):
    nt = Temp_L.shape[0]
    nh = Hor_L.shape[0]

    x = zeros_like(x_c)

    # Drop the part of the vectors that we are not interested in:
    G_state = G_state[ipos:nt*ipos]
    x_c = x_c[ipos:nt*ipos]
    x_cat = zeros_like(x_c)

    loop = Xc_to_x_loop(nt, Hor_L, x_c)

    p = Pool()
    items = [(G_state[i*nh:(i+1)*nh], Temp_L[i,:]) for i in range(nt)]
    res = p.starmap(loop.apply, items)

    for i, dx in tqdm(enumerate(res)):
        x_cat[i*nh:(i+1)*nh] += dx

    # for i in range(nt):
    #     x_cat[ i*nh : (i+1)*nh ] += loop.apply(G_state[ i*nh : (i+1)*nh ], Temp_L[i,:])

    # for i in range(nt):
    #     for j in range(nt):
    #         x_cat[ i*nh : (i+1)*nh ] += G_state[ i*nh : (i+1)*nh ] * dot( Temp_L[i,j]*Hor_L, x_c[ j*nh : (j+1)*nh ])

    # Return the full vector:
    x[ipos:nt*ipos] = x_cat
    return x


class G_to_gc_loop:
    def __init__(self, nt, gg, Hor_Lt):
        self.nt = nt
        self.Hor_Lt = Hor_Lt
        self.nh = Hor_Lt.shape[0]
        self.gg = gg

    def apply(self, Temp_Lt):
        gc = zeros_like(self.G_state)
        for j in range(self.nt):
            gc = dot(Temp_Lt[j]*self.Hor_lt, self.gg[j*self.nh:(j+1)*self.nh])
        return gc


def g_to_gc(G_state, Temp_Lt, Hor_Lt, g, ipos, dummy, path=None):
    nt = Temp_Lt.shape[0]
    nh = Hor_Lt.shape[0]

    gc = zeros_like(G_state)
    gg = G_state[ipos:nt*ipos] * g[ipos:nt*ipos]
    gc_cat = zeros_like(gg)

    loop = G_to_gc_loop(nt, gg, Hor_Lt)

    p = Pool()
    res = p.imap(loop.apply, [Temp_Lt[i] for i in range(nt)])
    for i, dg in enumerate(tqdm(res)):
        gc_cat[i*nh:(i+1)*nh] = res

    # for i in range(nt):
    #     gc_cat[ i*nh : (i+1)*nh] = loop.apply(Temp_Lt[i,:])
    #     # for j in range(nt):
    #     #     gc_cat[ i*nh : (i+1)*nh] = dot( Temp_Lt[i,j] * Hor_Lt, G_state[ j*nh : (j+1)*nh] * g[ j*nh : (j+1)*nh])

    gc[ipos:nt*ipos] = gc_cat
    return gc