from mpi4py import MPI
from argparse import ArgumentParser
import logging
from numpy import *
import sys
from h5py import File
import os

logger = logging.getLogger(__name__)

def xc_to_x(filename):
    sys.stdout.flush()

    with File(filename, 'r') as fid :
        G_state = fid['prior_uncertainties'][:]
        Temp_L = fid['Bt'][:]
        Hor_L = fid['Bh'][:]
        x_c = fid['x_c'][:]
        ipos = fid.attrs['ipos']

    nt = shape(Temp_L)[0]
    nh = shape(Hor_L)[0]

    comm = MPI.COMM_SELF.Spawn(sys.executable, args=[os.path.abspath(__file__), '--xw'], maxprocs=int(os.environ['NCPUS_LUMIA']))
    comm.bcast([nt, nh, ipos], root=MPI.ROOT)
    comm.Bcast([G_state, MPI.DOUBLE], root=MPI.ROOT)
    comm.Bcast([Hor_L, MPI.DOUBLE], root=MPI.ROOT)
    comm.Bcast([Temp_L, MPI.DOUBLE], root=MPI.ROOT)
    comm.Bcast([x_c, MPI.DOUBLE], root=MPI.ROOT)

    x = zeros(nt*nh)
    comm.Reduce(None, x, op=MPI.SUM, root=MPI.ROOT)

    with File(filename, 'a') as fid :
        fid['x'] = x

def xc_to_x_worker():
    # Initialize MPI
    comm = MPI.Comm.Get_parent()
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Receive data
    nt, nh, ipos = comm.bcast(None, root=0)

    G_state = empty(nt*nh, dtype=float)
    comm.Bcast([G_state, MPI.DOUBLE], root=0)

    Hor_L = empty([nh, nh], dtype=float)
    comm.Bcast([Hor_L, MPI.DOUBLE], root=0)

    Temp_L = empty([nt, nt], dtype=float)
    comm.Bcast([Temp_L, MPI.DOUBLE], root=0)

    x_c = empty(nt*nh, dtype=float)
    comm.Bcast([x_c, MPI.DOUBLE], root=0)

    # Do the partial calculation
    x = zeros(nt*nh)
    r = 0
    for i in range(nt):
        for j in range(nt):
            if rank == r :
                x[ipos+i*nh:ipos+(i+1)*nh] += G_state[ipos+i*nh:ipos+(i+1)*nh]* dot(Temp_L[i,j]*Hor_L, x_c[ipos+j*nh:ipos+(j+1)*nh])
            r += 1
            if r == size: r = 0

    # Send back
    comm.Reduce(x, None, op=MPI.SUM, root=0)

def g_to_gc(filename):

    with File(filename, 'r') as fid :
        G_state = fid['prior_uncertainties'][:]
        Temp_Lt = fid['Bt'][:]
        Hor_Lt = fid['Bh'][:]
        g = fid['g'][:]
        ipos = fid.attrs['ipos']

    nt = Temp_Lt.shape[0]
    nh = Hor_Lt.shape[0]

    comm = MPI.COMM_SELF.Spawn(sys.executable, args=[os.path.abspath(__file__), '--gw'], maxprocs=int(os.environ['NCPUS_LUMIA']))
    comm.bcast([nt, nh, ipos], root=MPI.ROOT)
    comm.Bcast([g, MPI.DOUBLE], root=MPI.ROOT)
    comm.Bcast([Hor_Lt, MPI.DOUBLE], root=MPI.ROOT)
    comm.Bcast([Temp_Lt, MPI.DOUBLE], root=MPI.ROOT)
    comm.Bcast([G_state, MPI.DOUBLE], root=MPI.ROOT)

    g_c = zeros((nt*nh))
    comm.Reduce(None, g_c, op=MPI.SUM, root=MPI.ROOT)

    with File(filename, mode='a') as fid :
        fid['g_c'] = g_c

def g_to_gc_worker():
    # Initialize MPI
    comm = MPI.Comm.Get_parent()
    rank = comm.Get_rank()

    # Receive data
    nt, nh, ipos = comm.bcast(None, root=0)

    g = empty(nt*nh, dtype=float)
    comm.Bcast([g, MPI.DOUBLE], root=0)

    Hor_Lt = empty([nh, nh], dtype=float)
    comm.Bcast([Hor_Lt, MPI.DOUBLE], root=0)

    Temp_Lt = empty([nt, nt], dtype=float)
    comm.Bcast([Temp_Lt, MPI.DOUBLE], root=0)

    G_state = empty(nt*nh, dtype=float)
    comm.Bcast([G_state, MPI.DOUBLE], root=0)

    # Do the partial computation
    g_c = zeros(nt*nh)
    r = 0
    for i in range(nt):
        for j in range(nt):
            if rank == r :
                g_c[ipos+i*nh:ipos+(i+1)*nh] += dot(Temp_Lt[i,j]*Hor_Lt, G_state[ipos+j*nh:ipos+(j+1)*nh] * g[ipos+j*nh:ipos+(j+1)*nh])
            r += 1
            if r == size: r = 0

    # Send back
    comm.Reduce(g_c, None, op=MPI.SUM, root=0)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--g", "-g", action='store_true', default=False)
    parser.add_argument("--gw", action='store_true', default=False)
    parser.add_argument("--x", "-x", action='store_true', default=False)
    parser.add_argument("--xw", action='store_true', default=False)
    parser.add_argument("--file", '-f')
    args = parser.parse_args()
    if args.g:
        g_to_gc(args.file)
    elif args.x :
        xc_to_x(args.file)
    elif args.gw:
        g_to_gc_worker()
    elif args.xw :
        xc_to_x_worker()
