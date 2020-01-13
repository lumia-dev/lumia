#!/usr/bin/env python

import sys
from numpy import *
import argparse
from lumia import tqdm
import logging
import os
from h5py import File
import subprocess
from lumia.Tools.logging_tools import colorize
from lumia.Tools import checkDir
#from lumia.Tools.logging_tools import logger

logger = logging.getLogger(__name__)

executable_mpi = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'preconditioner_mpi.py')

def g_to_gc(G_state, Temp_Lt, Hor_Lt, g, ipos, dummy, path=None):

    n_state = len(G_state)
    nt = shape(Temp_Lt)[0]
    nhor = shape(Hor_Lt)[0]
    g_c = zeros([n_state])
    for i in tqdm(range(nt), desc='preconditioning gradient', leave=False):
        for j in range(nt):
            g_c[ipos+i*nhor:ipos+(i+1)*nhor] += dot(Temp_Lt[i,j]*Hor_Lt, G_state[ipos+j*nhor:ipos+(j+1)*nhor] * g[ipos+j*nhor:ipos+(j+1)*nhor])
    return g_c

def xc_to_x(G_state, Temp_L, Hor_L, x_c, ipos, dummy, path=None):

    n_state = len(G_state)
    nt = shape(Temp_L)[0]
    nhor = shape(Hor_L)[0]

    x = zeros(n_state)
    for i in tqdm(range(nt), desc='xc_to_x', leave=True):
        for j in tqdm(range(nt), desc='step %i/%i'%(i, nt), leave=False):
            x[ipos+i*nhor:ipos+(i+1)*nhor] += G_state[ipos+i*nhor:ipos+(i+1)*nhor]* dot(Temp_L[i,j]*Hor_L, x_c[ipos+j*nhor:ipos+(j+1)*nhor])
    return x

def g_to_gc_MPI(G_state, Temp_Lt, Hor_Lt, g, ipos, dummy, path=None):

    fname = os.path.join(path, 'preco_data.hdf')
    with File(fname, 'w') as fid :
        fid['prior_uncertainties'] = G_state
        fid['Bt'] = Temp_Lt
        fid['Bh'] = Hor_Lt
        fid['g'] = g
        fid.attrs.create('ipos', ipos)

    cmd = ['mpiexec', '-n', '1', 'python', executable_mpi, '-g', '-f', fname, '-v', logging.getLevelName(logger.getEffectiveLevel())]
    logger.info(colorize(' '.join([*cmd]), 'g'))
    pid = subprocess.Popen(cmd, close_fds=True)
    pid.wait()

    with File(fname, 'r') as fid:
        g_c = fid['g_c'][:]

    return g_c

def xc_to_x_MPI(G_state, Temp_L, Hor_L, x_c, ipos, dummy, path=None):

    checkDir(path)
    fname = os.path.join(path, 'preco_data.hdf')
    with File(fname, 'w') as fid :
        fid['prior_uncertainties'] = G_state
        fid['Bt'] = Temp_L
        fid['Bh'] = Hor_L
        fid['x_c'] = x_c
        fid.attrs.create('ipos', ipos)
    
    cmd = ['mpiexec', '-n', '1', 'python', executable_mpi, '-x', '-f', fname, '-v', logging.getLevelName(logger.getEffectiveLevel())]
    logger.info(colorize(' '.join([*cmd]), 'g'))
    pid = subprocess.Popen(cmd, close_fds=True)
    pid.wait()
#    os.system('mpiexec python %s -x -f %s'%(executable_mpi, fname))

    with File(fname, 'r') as fid :
        x = fid['x'][:]
    return x
