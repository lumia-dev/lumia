#!/usr/bin/env python

from multiprocessing import Pool
from numpy import pi, cos, sin, arcsin, zeros, exp, linalg, eye, meshgrid, flipud, argsort, diag, sqrt, where, unique
from dataclasses import dataclass
from numpy.typing import NDArray
from loguru import logger
from pint import Unit, Quantity
from pandas import DateOffset, DataFrame
from tqdm.autonotebook import tqdm


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

    def __post_init__(self):
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

    def calc_eigen_decomposition(self) -> (NDArray, NDArray):
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

        return p, lam**.5

    @property
    def L(self) -> NDArray:
        return self.eigen_vectors * self.eigen_values # TODO: check why this is not a dot product

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

    def calc_eigen_decomposition(self) -> (NDArray, NDArray):
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
        return self.eigen_vectors @ self.eigen_values


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


def calc_total_uncertainty(
        errvec: DataFrame,
        temporal_correlation: NDArray,
        spatial_correlation: NDArray,
        unit_optim : Unit,
        unit_budget : Unit,
        field : str = 'prior_uncertainty') -> Quantity:
    unitconv = (1 * unit_optim).to(unit_budget).magnitude
    _common['Ch'] = spatial_correlation
    _common['Ct'] = temporal_correlation
    _common['sigmas'] = errvec.loc[:, field].values * unitconv
    _common['itimes'] = errvec.itime.values

    nt = len(unique(_common['itimes']))

    with Pool() as pp :
        errm = pp.imap(aggregate_uncertainty, range(nt))
        err = sum(tqdm(errm, total=nt, leave=False))

    # here "err" is the variance, in units of [flux_unit]^2. We want something in [flux_unit] so take the square root.
    errtot = sqrt(err)

    for key in ['Ch', 'Ct', 'sigmas', 'itimes'] :
        del _common[key]
    return errtot


def calc_temporal_correlation(corlen: DateOffset, dt: DateOffset, sigmas: DataFrame) -> TemporalCorrelation:
    assert dt.base == corlen.base

    # Number of time steps :
    times = sigmas.loc[:, 'time'].drop_duplicates()
    nt = times.shape[0]

    return TemporalCorrelation(corlen=corlen.n / dt.n, dt=1., n=nt)


def calc_horizontal_correlation(catname: str, corstring: str, sigmas: DataFrame) -> SpatialCorrelation:
    corlen, cortype = corstring.split('-')
    corlen = int(corlen)
    logger.warning("poor implementation. fix needed")
    vec = sigmas.loc[(sigmas.category == catname)] # two categories with the same name can exist, in different tracers ...
    vec = vec.loc[vec.time == vec.iloc[0].time]
    return SpatialCorrelation(corlen=corlen, cortype=cortype, lats=vec.lat.values, lons=vec.lon.values)
