#!/usr/bin/env python

from multiprocessing import Pool
from numpy import pi, cos, sin, arcsin, zeros, exp, linalg, eye, meshgrid, flipud, argsort, diag, sqrt, where, unique
from dataclasses import dataclass
from numpy.typing import NDArray
from loguru import logger
from pint import Unit, Quantity
from pandas import DateOffset, DataFrame
from tqdm.autonotebook import tqdm
from lumia.utils import debug
from typing import Tuple
from pathlib import Path
from h5py import File
import hashlib


_common = {}   # common for multiprocessing


def calc_dist(lon1, lat1, lon2, lat2, ae=6.371e6, stretch_ratio = 1.):
    """
    Computes distance between two points on the globe
    The "stretch_ratio" optional argument can be used to "stretch" (stretch_ratio > 1)
    the distances along the longitude axis (or to compress them if stretch_ratio < 1)
    """
    x1 = lon1 * pi / 180
    y1 = lat1 * pi / 180
    x2 = lon2 * pi / 180
    y2 = lat2 * pi / 180
    dy2 = (sin(0.5 * (y2 - y1))) ** 2
    dx2 = cos(y1) * cos(y2) * (sin(0.5 * (x2 - x1))) ** 2
    dd = 2 * arcsin((dx2 * stretch_ratio + dy2) ** .5)
    ddg = dd * 180 / pi
    dist = (ddg * 2 * pi * 0.001 * ae) / 360
    return dist
    # return (ddg * 2 * pi * 0.001 * ae) / 360


def calc_dist_vector(iloc, stretch_ratio = 1., debug: bool = False):
    lons = _common['lons']
    lats = _common['lats']
    stretch_ratio = _common.get('stretch_ratio', stretch_ratio)
    reflon = lons[iloc]
    reflat = lats[iloc]
    V = zeros(iloc+1)
    for ii, (lon, lat) in enumerate(zip(lons[:iloc+1], lats[:iloc+1])):
        # print(calc_dist(reflon, reflat, lon, lat, stretch_ratio))
        V[ii] = calc_dist(reflon, reflat, lon, lat, stretch_ratio=stretch_ratio)
    return V


def calc_dist_matrix(lats, lons, stretch_ratio=1.):
    M = zeros((len(lats), len(lons)))
    _common['lons'] = lons
    _common['lats'] = lats
    _common['stretch_ratio'] = stretch_ratio
    with Pool() as pp :
        res = pp.map(calc_dist_vector, range(len(lons)))
    for i, v in enumerate(res):
        M[:i+1, i] = v
        M[i, :i+1] = v
    del _common['lons'], _common['lats']
    return M


@dataclass(kw_only=True)
class SpatialCorrelation:
    corlen : float
    cortype : str
    lats : NDArray
    lons : NDArray
    min_eigval : float = 0.00001
    stretch_ratio : float = 1.
    min_corr : float = 1.e-7
    cache_dir : Path | None = None

    @debug.trace_args()
    def __post_init__(self):
        
        # Ensure that cache_dir is a Path and not a str (if not None)
        if self.cache_dir :
            self.cache_dir = Path(self.cache_dir)
            
        self.n = len(self.lats)

        # Calculate the covariance matrix:
        distmat = calc_dist_matrix(self.lats, self.lons, stretch_ratio = self.stretch_ratio)
        match self.cortype:
            case 'g':
                self.mat = exp(-(distmat/self.corlen)**2)
            case 'e':
                self.mat = exp(-(distmat/self.corlen))
            case 'h':
                self.mat = 1/(1+distmat/self.corlen)
            case _:
                logger.error(f'Correlation choice "{self.cortype}" should be one of ["g", "e", "h"]')
                raise ValueError
        self.mat[self.mat < self.min_corr] = 0.
        self.eigen_vectors, self.eigen_values = self.calc_eigen_decomposition()
        
    @property
    def hash(self) -> int :
        """
        hash of the class instance, used to avoid re-computing the eigen-decomposition if it has been done already
        """
        return hashlib.md5(self.mat).hexdigest()

    @debug.timer
    def calc_eigen_decomposition(self) -> Tuple[NDArray, NDArray]:
        """
        Calculate the eigen decomposition of the spatial correlation matrix.
        Since it can be quite time consuming for large matrices, the eigen decomposition can be read from a file, computed in a previous run. For that, the "cache_dir" attribute must be set to a valid path.
        The cached file is named after the "hash" attribute of the object (itself computed based on the combined hashes of the main attributes), e.g. "horizontal_correlation.{hash}.nc". 
        - If a valid cache file is found, then the eigen decomposition is simply read from it
        - If no valid cache file is found but a "cache_dir" attribute exists (and is not None), then the eigen decomposition will be calculated and written to a new cache file.
        """
        if self.cache_dir is not None :
            corrfile = self.cache_dir / f'horizontal_correlation.{self.hash}.nc'
            logger.info(f"Reading correlations from {corrfile}")
            if corrfile.exists():
                logger.info(f"Reading correlations from {corrfile}")
                with File(self.cache_dir / f'horizontal_correlation.{self.hash}.nc') as fid :
                    return fid['eigen_vectors'][:], fid['eigen_values'][:]**.5
                
        lam, p = linalg.eigh(self.mat)

        # Make positive semidefinite
        if self.min_eigval > 1.e-10 :
            min_eigval = self.min_eigval * min((1, lam.max()))
        else :
            min_eigval = self.min_eigval

        n_neg = sum(lam < min_eigval)
        n_neg2 = sum(abs(lam) < min_eigval)
        lam[lam < min_eigval] = min_eigval
        logger.debug(f"Maximum eigenvalue = {lam.max():10.3e}, minimum eigenvalue = {lam.min():10.3e}")
        if n_neg != n_neg2 :
            logger.error(f"{n_neg - n_neg2} large negative eigen values set to 0. Maybe it's a bug?")
        if n_neg > 0 :
            logger.debug(f"Set {n_neg} eigenvalues to {min_eigval:15.11f}")
            
        if self.cache_dir :
            # Create the directory if needed
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            
            # Write the eigen decomposition in it
            with File(self.cache_dir / f'horizontal_correlation.{self.hash}.nc', 'w') as fid :
                fid.create_dataset('eigen_vectors', p.shape, compression='gzip')
                fid.create_dataset('eigen_values', lam.shape, compression='gzip')
                fid.create_dataset('lats', self.lats.shape, compression='gzip')
                fid.create_dataset('lons', self.lons.shape, compression='gzip')
                #fid.create_dataset('B', self.mat.shape, compression='gzip')
                fid['eigen_vectors'][:] = p
                fid['eigen_values'][:] = lam
                fid['lats'][:] = self.lats
                fid['lons'][:] = self.lons
                #fid['B'] = self.mat
                fid.attrs['corlen'] = self.corlen
                fid.attrs['cortype'] = self.cortype
                fid.attrs['min_eigval'] = self.min_eigval
                fid.attrs['stretch_ratio'] = self.stretch_ratio
                fid.attrs['min_corr'] = self.min_corr

        return p, lam**.5

    @property
    def L(self) -> NDArray:
        return self.eigen_vectors * self.eigen_values 

    @property
    def B(self) -> NDArray:
        return self.mat


@dataclass(kw_only=True)
class TemporalCorrelation:
    corlen : float
    dt : float
    n : int
    min_corr : float = 0

    def __post_init__(self):
        if self.corlen < 1.e-20:
            self.B = eye(self.n)
        else :
            t1, t2 = meshgrid(range(self.n), range(self.n))
            self.B = exp(- abs(t1 - t2) * self.dt / self.corlen)
        self.B[self.B < self.min_corr] = 0.

        self.eigen_vectors, self.eigen_values = self.calc_eigen_decomposition()

    @debug.timer
    def calc_eigen_decomposition(self) -> Tuple[NDArray, NDArray]:
        lam, evec = linalg.eigh(self.B)
        sort_order = flipud(argsort(lam))
        lam = lam[sort_order]
        evec = evec[:, sort_order]
        lam_sqrt = diag(sqrt(lam))
        # Make sure that the elements in the top row of P are non-negative
        col_sign = where(evec[0] < 0.0, -1.0, 1.0)
        ev = evec * col_sign
        return ev, lam_sqrt

    @property
    def L(self) -> NDArray:
        # TODO: check why eigen_values is not just a vector here (instead of a diagonal matrix).
        return self.eigen_vectors @ self.eigen_values


@debug.timer
def aggregate_uncertainty(it1: int) -> float:
    itimes = _common['itimes']
    sig1 = _common['sigmas'][itimes == it1]
    Ct = _common['Ct']
    Ch = _common['Ch']
    nt = len(unique(itimes))
    err = 0
    for it2 in range(nt):
        sig2 = _common['sigmas'][itimes == it2]
        err += (Ct[it1, it2] * Ch * sig1[None, :] * sig2[:, None]).sum()
    return err


@debug.timer
def calc_total_uncertainty(
        errvec: DataFrame,
        temporal_correlation: NDArray,
        spatial_correlation: NDArray,
        unit_optim : Unit,
        unit_budget : Unit,
        field : str = 'prior_uncertainty') -> Quantity:
    unitconv = (1 * unit_optim).to(unit_budget).magnitude

    nt = temporal_correlation.shape[0]
    sigmas = errvec.loc[:, field].values * unitconv
    ch = spatial_correlation
    ct = temporal_correlation

    # The formula below is equivalent (but much faster) to:
    #for it1 in range(nt):
    #    for it2 in range(nt):
    #        for ip1 in range(nh):
    #            for ip2 in range(nh):
    #                errtot += sigmas[it1, ip1] * sigmas[it2, ip2] * Ct[it1, it2] * Ch[ip1, ip2]
    #errtot = sqrt(errtot)

    # This relies :
    # - on the property of the kronecker vector that:
    #   kron(A, B) @ vec(V) = vec(A @ V @ B.T)
    #   with "vec" the vectorization operator (i.e. V.reshape(-1) here
    # - on the property that the sum of a covariance matrix can be inferred from the equation s @ Q @ s,
    #   with "s" the vector of standard deviations (sigmas) and Q the correlation matrix
    # - combining the two, we have: s @ kron(Qt, Qh) @ s === s @ vec(Qh @ E @ Qt), with "E" the matrix form
    #   of the vector of standard deviations "s". 
    # - the matrix form of the standard deviations need to be (np, nt), however, the data are stored in a (nt, np) order ==> we must use the transpose of the reshaped matrix, and, likewise, we must transpose the outcome of the matrix product before reshaping it as a vector

    return (sigmas @ (ch @ sigmas.reshape(nt, -1).T @ ct).T.reshape(-1))**.5
    

@debug.timer
def calc_temporal_correlation(corlen: DateOffset, dt: DateOffset, sigmas: DataFrame) -> TemporalCorrelation:
    assert dt.base == corlen.base

    # Number of time steps :
    times = sigmas.loc[:, 'time'].drop_duplicates()
    nt = times.shape[0]

    return TemporalCorrelation(corlen=corlen.n / dt.n, dt=1., n=nt)


@debug.timer
def calc_horizontal_correlation(catname: str, corstring: str, sigmas: DataFrame, cache_dir : Path = None) -> SpatialCorrelation:
    corlen, cortype = corstring.split('-')
    corlen = int(corlen)
    logger.warning("poor implementation. fix might be needed ...")
    # Two categories with the same name can exist, in different tracers ...
    # It would be better to have unique categories that have cat name and cat tracer as properties
    vec = sigmas.loc[(sigmas.category == catname)]
    vec = vec.loc[vec.time == vec.iloc[0].time]
    return SpatialCorrelation(
        corlen=corlen, 
        cortype=cortype, 
        lats=vec.lat.values, 
        lons=vec.lon.values, 
        cache_dir=cache_dir
    )
