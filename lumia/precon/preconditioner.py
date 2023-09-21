#!/usr/bin/env python

import logging
from numpy import shape, zeros, dot, ma
from lumia import tqdm

logger = logging.getLogger(__name__)


def init():
    pass

def g_to_gc(sig_x, Lt_t, Lh_t, g, ipos):
    nt = Lt_t.shape[0]
    nh = Lh_t.shape[0]
    gc = 0 * sig_x
    z = ((sig_x * g)[ipos : ipos + nt * nh]).reshape(nt, nh)
    gc[ipos: ipos + nt * nh] = (Lt_t @ z @ Lh_t.transpose()).reshape(-1)
    return gc

def xc_to_x(sig_x, Lt, Lh, xc, ipos):
    nt = Lt.shape[0]
    nh = Lh.shape[0]
    w = xc[ipos: ipos + nt * nh].reshape(nt, nh)
    x = 0 * sig_x
    if ma.isMaskedArray(w):
        w = w.data
    x[ipos: ipos + nt * nh] = sig_x[ipos: ipos + nt * nh] * (Lt @ w @ Lh.transpose()).reshape(-1)
    # x[ipos+i*nhor:ipos+(i+1)*nhor] += G_state[ipos+i*nhor:ipos+(i+1)*nhor]* dot(Temp_L[i,j]*Hor_L, x_c[ipos+j*nhor:ipos+(j+1)*nhor])
    return x


# def g_to_gc(G_state, Temp_Lt, Hor_Lt, g, ipos, dummy, path=None):
#     n_state = len(G_state)
#     nt = shape(Temp_Lt)[0]
#     nhor = shape(Hor_Lt)[0]
#     g_c = zeros([n_state])
#     for i in tqdm(range(nt), desc='preconditioning gradient', leave=False):
#         for j in range(nt):
#             g_c[ipos+i*nhor:ipos+(i+1)*nhor] += dot(Temp_Lt[i,j]*Hor_Lt, G_state[ipos+j*nhor:ipos+(j+1)*nhor] * g[ipos+j*nhor:ipos+(j+1)*nhor])
#     return g_c


# def xc_to_x(G_state, Temp_L, Hor_L, x_c, ipos, dummy, path=None):
#     n_state = len(G_state)
#     nt = shape(Temp_L)[0]
#     nhor = shape(Hor_L)[0]
#     x = zeros(n_state)
#     for i in tqdm(range(nt), desc='xc_to_x', leave=True):
#         for j in tqdm(range(nt), desc='step %i/%i'%(i, nt), leave=False):
#             x[ipos+i*nhor:ipos+(i+1)*nhor] += G_state[ipos+i*nhor:ipos+(i+1)*nhor]* dot(Temp_L[i,j]*Hor_L, x_c[ipos+j*nhor:ipos+(j+1)*nhor])
#     return x