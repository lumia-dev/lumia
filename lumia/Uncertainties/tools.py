import os
import logging
from tqdm import tqdm
from netCDF4 import Dataset
import multiprocessing
from datetime import datetime
from numpy import transpose, where, zeros, eye, dot, pi, sin, cos, arcsin, exp, \
    meshgrid, diag, sqrt, argsort, flipud
from scipy import linalg
from h5py import File
logger = logging.getLogger(__name__)

#TODO: create a proper "grid" module, which will create a file storing the grid definition and the lat/lon covariances


def read_latlon(file_name):
    if not os.path.exists(file_name):
        raise RuntimeError("%s does not exist"%file_name)
    f = Dataset(file_name)
    P = f.variables['P'][:]
    D = f.variables['sqrt_lam'][:]
    f.close()
    # Thanks to Fortran's idiosyncracy, the indices of arrays in netcdf files are swapped
    # So P is actually transpose of the eigenvector matrix, and we need to transpose it back
    return transpose(P), D


def build_matrix_inner(args):
    index, lons, lats, reflon, reflat, corlen, iexp = args
    V = zeros(len(lons))
    for (ind, lon, lat) in zip(index, lons, lats):
        dst = dist(reflon, reflat, lon, lat)
        V[ind] = exp(-(dst/corlen)**iexp)
    return ind, V


def buid_matrix(state, corlen, iexp):
    pp = multiprocessing.Pool()
    n_hor = state.shape[0]
    P = zeros((n_hor, n_hor))
    #res = [pp.apply(build_matrix_inner, args=[state, p1, corlen, iexp]) for p1 in state.itertuples()]
    res = pp.imap(build_matrix_inner, [(state.index.values, state.lon.values, state.lat.values, p1.lon, p1.lat, corlen, iexp) for p1 in tqdm(state.itertuples(), total=state.shape[0])])
    for i,v in tqdm(res, desc='Retrieving results', total=n_hor) :
        P[i, :] = v
        P[:, i] = v 
    return P


class horcor:
    def __init__(self, corlen, cortype, statevec, min_eigval=0.00001):
        self.corlen = corlen
        self.cortype = cortype
        self.state = statevec
        self.min_eigval = min_eigval

    def calc_latlon_covariance(self):
        logger.info("Use numpy.linalg to compute eigen decomposition of covariance matrix")
        logger.info(datetime.now())
        n_hor = self.state.shape[0]
        logger.info("Matrix size: (%i x %i)",n_hor, n_hor)
        #P = zeros((n_hor, n_hor))
        P_diag = zeros((n_hor, n_hor))
        iexp = {'e':1, 'g':2}[self.cortype]
        assert self.corlen >= 0, "ERROR - correlation length should be >= 0"
        # put stuff here to construct P for exponential or gaussian decay
        logger.info(datetime.now())
        if self.corlen == 0 :
            P = zeros((n_hor, n_hor))
            P = eye(n_hor)
            P_diag = 1.*P
            lam = 1.
        else :
            # loop first index over latlon grid
            logger.info(f'Start building the matrix: {datetime.now()}')

            # for p1 in self.state.itertuples():
            #     for p2 in self.state.itertuples():
            #         dst = dist(p1.lon, p1.lat, p2.lon, p2.lat)
            #         cor = exp(-(dst/self.corlen)**iexp)
            #         P[p1.Index, p2.Index] = cor
            #         P[p2.Index, p1.Index] = cor
            P = buid_matrix(self.state, self.corlen, iexp)
            with File("/proj/inversion/matrix.hdf", 'w') as f:
                f['P'] = P

            logger.info(f'Start the eigen value decomposition: {datetime.now()}')
            # Eigen decomposition of symmetric matrix
            lam, P = linalg.eigh(P)
            logger.info(datetime.now())
            lam = self.make_positive_semidef(lam)
            logger.info(datetime.now())
            for i in range(n_hor):
                P_diag[i, i] = lam[i]
            logger.info(datetime.now())

        self.n_hor = n_hor
        self.lam = lam
        self.P = P
        self.P_diag = P_diag

    def write(self, filename):
        ds = Dataset(filename, 'w')
        ds.corlen = self.corlen
        ds.corchoice = self.cortype
        ds.createDimension('n_hor', self.n_hor)
        ds.createVariable('sqrt_lam', 'd', ('n_hor', ), zlib=True)
        ds.createVariable('lam', 'd', ('n_hor', ), zlib=True)
        ds.createVariable('P', 'd', ('n_hor', 'n_hor'), zlib=True)
        ds.variables['lam'][:] = self.lam
        ds.variables['sqrt_lam'][:] = self.lam**.5
        ds.variables['P'][:] = self.P.transpose() # numpy.eigv returns the vectors in columns, but the equivalent fotran subroutine returns them in rows. So store it in rows for consistency.

        # Also write B itself (first recalculate), for use in postprocessing
        logger.info("Recalculating B matrix from eigenvectors/eigenvalues")
        ds.createVariable('B', 'd', ('n_hor', 'n_hor'), zlib=True)
        B = dot(self.P, self.P_diag)
        P = dot(B, self.P.transpose())
        ds.variables['B'][:] = P
        ds.close()

    def make_positive_semidef(self, lam):
        min_eigval = self.min_eigval+0.
        if self.min_eigval > 1.e-10:
            min_eigval = self.min_eigval*min((1., lam.max()))
        logger.info("Maximum eigenvalue = %10.3e, minimum eigenvalue = %10.3e", lam.min(), lam.max())
        n_neg = sum(lam < min_eigval)
        lam[lam<min_eigval] = min_eigval
        if n_neg > 0 :
            logger.info("Set %i eigenvalues to %15.11f",n_neg, min_eigval)
        return lam


def dist(lon1, lat1, lon2, lat2, ae=6.371e6):
    # Compute distance of two points on the globe
    # Based on TM5/misctools.F90/dist
    x1 = lon1*pi/180
    y1 = lat1*pi/180
    x2 = lon2*pi/180
    y2 = lat2*pi/180
    dy2 = (sin(0.5*(y2-y1)))**2
    dx2 = cos(y1)*cos(y2)*(sin(0.5*(x2-x1)))**2
    dd = 2*arcsin((dx2+dy2)**.5)
    ddg = dd*180/pi
    return (ddg*2*pi*0.001*ae)/360


def calc_temp_corr(corlen, dt, n):
    if corlen<1.e-20:
        P = eye(n)
        D = eye(n)
    else:
        dummy_X, dummy_Y = meshgrid(range(n), range(n))
        A = exp(-abs(dummy_X-dummy_Y)*dt/corlen)
        P,D = matrix_square_root(A)
        # Debug:
        # if abs((dot(dot(P, D**2), P.transpose())-A)) > 1.e-10 :
        #     raise SomeError
    return P, D


def matrix_square_root(B):
    # Given a real symmetric matrix B, calculate L such that LL^T = B
    # Actually, calculate L as the product of a unitary matrix and a diagonal matrix
    lam,P = linalg.eigh(B)
    # Sort eigenvalues and switch columns of P accordingly
    #sort_order = argsort(lam)
    sort_order = flipud(argsort(lam))
    lam = lam[sort_order]
    P = P[:, sort_order]
    D = diag(sqrt(lam))
    # Make sure that the elements in the top row of P are non-negative
    col_sign = where(P[0]<0.0, -1.0, 1.0)
    P = P*col_sign

    # Select positive values :
#    ii, = where(lam>0)
#    lam = lam[ii]
#    P = P[:, ii]
#    D = diag(sqrt(lam))
    return P, D
    #lam[lam<0.0] = 0.0
    #D = diag(sqrt(lam))
    # Make sure that the elements in the top row of P are non-negative
    #col_sign = where(P[0]<0.0, -1.0, 1.0)
    #P *= col_sign
    #return P, D
