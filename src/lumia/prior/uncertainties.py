#!/usr/bin/env python

from multiprocessing import Pool
from numpy import pi, cos, sin, arcsin, zeros, exp, linalg, eye, meshgrid, flipud, argsort, diag, sqrt, where, unique, nan
from dataclasses import dataclass
from numpy.typing import NDArray
from loguru import logger
from pint import Unit, Quantity
from pandas import DateOffset, DataFrame, DatetimeIndex
from tqdm.autonotebook import tqdm
from lumia.utils import debug
from typing import Tuple
from pathlib import Path
from h5py import File
import hashlib
from functools import cache
import xarray as xr
from omegaconf import DictConfig
from pandas.tseries.frequencies import to_offset


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


#@cache
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


def read_spatial_correlations(filename : str, lats : NDArray, lons : NDArray) -> NDArray:
    """
    Create a correlation matrix for the given lat and lon coordinates, based on pre-computed correlation functions in a file
    """
    ds = xr.open_dataset(filename, group='correlations')
    
    # Ensure that all pairs of coordinates have a valid correlation value
    points = [(lat, lon) for (lat, lon) in zip(ds.lat, ds.lon)]             # coordinates of the points in the file
    
    # Construct the covariance matrix:
    mat = zeros((len(lats), len(lats)))
    try :
        for ip1, p1 in enumerate(zip(lats, lons)):
            ix1 = points.index(p1)
            for ip2, p2 in enumerate(zip(lats, lons)):
                ix2 = points.index(p2)
                mat[ip1, ip2] = ds.horizontal_correlations.values[ix1, ix2]
    except ValueError as e:
        logger.critical("Not all pairs of coordinates are present in the pre-processed correlation matrix file. Aborting.")
        logger.exception(e)
        
    return mat
    

@dataclass(kw_only=True)
class SpatialCorrelation:
    mat : NDArray
    cortype : str
    lats : NDArray
    lons : NDArray
    min_eigval : float = 0.00001
    corlen : float = None
    corfile : str = None
    stretch_ratio : float = 1.
    min_corr : float = 1.e-7
    cache_dir : Path | None = None

    @debug.trace_args()
    def __post_init__(self):
    #def __init__(self, B, cache_dir : Path | None = None, min_eigval : float = 0.00001):
        if self.cache_dir:
            self.cache_dir = Path(self.cache_dir)
        #self.mat = B
        #self.min_eigval = min_eigval
        #self.eigen_vectors, self.eigen_values = self.calc_eigen_decomposition()
        self._eigenvec = None
        self._eigenval = None

    #---------------------------------------------
    # Lazy calculation of the eigen-values/vectors, as we may not always want to calculate them
    # For instance, when defining hybrid correlations
    @property
    def eigen_vectors(self):
        if self._eigenvec is None: 
            self.calc_eigen_decomposition()
        return self._eigenvec
    
    @property
    def eigen_values(self):
        if self._eigenval is None:
            self.calc_eigen_decomposition()
        return self._eigenval
        
    #---------------------------------------------
    @classmethod
    @debug.trace_args('cortype', 'corlen')
    def from_pars(cls, lons : NDArray, lats : NDArray, cortype : str, corlen : float, cache_dir : Path | None = None, stretch_ratio : float = 1., min_corr : float = 1.e-7):
        """

        Args:
            lons (NDArray): Longitude of the points (center of the grid cells) 
            lats (NDArray): Latitude of the points (center of the grid cells)
            cortype (str): Type of function used to generate the correlation: e (exponential), g (gaussian) or h (hyperbolic)
            corlen (float): Correlation length
            cache_dir (Path | None, optional): Since the calculation of the eigen-value decomposition can be quite lengthy, the code will store the result in a file in the "cache_dir" directory. Subsequent runs will be able to re-use this. The name of the file is based on the hash of the correlation matrix, and is not human readable.
            stretch_ratio (float, optional): optionally, the correlation length can be set differently according to lat and lon. This is achieved by "stretching" the distances (e.g. a "stretch_ratio" of 2 will lead to distances along the longitude axis to be computed as twice what they really are for computing the correlations, therefore they will drop faster in the longitude axis than in the latitude axis.
            min_corr (float): minimum correlation value allowed
        """
        
        #npt = len(lats)
        distmat = calc_dist_matrix(lats, lons, stretch_ratio = stretch_ratio)
        match cortype.lower() :
            case "g" | "gaussian" : 
                mat = exp( - (distmat / corlen) ** 2)
            case "e" | "exponential" :
                mat = exp( - (distmat / corlen))
            case "h" | "hyperbolic" :
                mat = 1 / (1 + distmat / corlen)
        mat[mat < min_corr] = 0.
        return cls(
            mat=mat, 
            cache_dir=cache_dir, 
            cortype=cortype,
            corlen=corlen,
            lats=lats, 
            lons=lons, 
            min_corr=min_corr, 
            stretch_ratio=stretch_ratio
        )
    
    @classmethod
    @debug.timer
    @debug.trace_args('filename')
    def from_file(cls, filename : str | Path, lats : NDArray, lons : NDArray, cache_dir : Path | None = None):
        ds = xr.open_dataset(filename)
        index_file = ds[['lon_points', 'lat_points']].to_dataframe().reset_index().set_index(['lon_points', 'lat_points'])
        points = index_file.loc[zip(lons, lats), :].point.values
        ds['hc'] = xr.DataArray(ds.horizontal_correlations.values, dims=('point', 'other_point'))
        ds['other_point'] = xr.DataArray(ds.point.values, dims=('other_point'))
        mat = ds['hc'][{'point':points, 'other_point':points}].fillna(0).values
        return cls(mat=mat, cortype='file', corfile=filename, lats=lats, lons=lons, cache_dir=cache_dir)

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
                    self._eigenvec = fid['eigen_vectors'][:]
                    self._eigenval = fid['eigen_values'][:] ** .5
                return self._eigenvec, self._eigenval
                
        lam, p = linalg.eigh(self.mat)
        
        # Make positive semidefinite
        if self.min_eigval > 1.e-10 :
            min_eigval = self.min_eigval * min((1, lam.max()))
        else :
            min_eigval = self.min_eigval

        n_neg = sum(lam < min_eigval)
        n_neg2 = sum(abs(lam) < min_eigval)
        lam[lam < min_eigval] = min_eigval
        #lam[abs(lam) < min_eigval] = min_eigval
        logger.debug(f"Maximum eigenvalue = {lam.max():10.3e}, minimum eigenvalue = {lam.min():10.3e}")
        if n_neg != n_neg2 :
            logger.warning(f"{n_neg - n_neg2} large negative eigen values set to 0. Maybe it's a bug?")
            print(lam[lam < 0])
        if n_neg2 > 0 :
            logger.debug(f"Set {n_neg2} eigenvalues to {min_eigval:15.11f}")
            
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
                if self.cortype == 'file':
                    fid.attrs['corfile'] = self.corfile
                else:
                    fid.attrs['corlen'] = self.corlen
                    fid.attrs['cortype'] = self.cortype
                fid.attrs['min_eigval'] = self.min_eigval
                fid.attrs['stretch_ratio'] = self.stretch_ratio
                fid.attrs['min_corr'] = self.min_corr

        self._eigenvec = p
        self._eigenval = lam ** .5
        return p, lam**.5

    @property
    def L(self) -> NDArray:
        return self.eigen_vectors * self.eigen_values 

    @property
    def B(self) -> NDArray:
        return self.mat


#@dataclass(kw_only=True)
class TemporalCorrelation:
    # cortype : str 
    # corlen : float
    # dt : float
    # n : int
    # min_corr : float = 0
    
    def __init__(self, B : NDArray):
        self.B = B
        self._eigenvec = None
        self._eigenval = None
        
    @property
    def eigen_vectors(self):
        if self._eigenvec is None:
            self.calc_eigen_decomposition()
        return self._eigenvec
    
    @property
    def eigen_values(self):
        if self._eigenval is None:
            self.calc_eigen_decomposition()
        return self._eigenval

    @debug.timer
    def calc_eigen_decomposition(self) -> Tuple[NDArray, NDArray]:
        lam, evec = linalg.eigh(self.B)
        lam[lam <= 0] = lam[lam > 0].min()
        sort_order = flipud(argsort(lam))
        lam = lam[sort_order]
        evec = evec[:, sort_order]
        lam_sqrt = diag(sqrt(lam))
        # Make sure that the elements in the top row of P are non-negative
        col_sign = where(evec[0] < 0.0, -1.0, 1.0)
        ev = evec * col_sign
        self._eigenvec = ev
        self._eigenval = lam_sqrt
        return ev, lam_sqrt

    @property
    def L(self) -> NDArray:
        # TODO: check why eigen_values is not just a vector here (instead of a diagonal matrix).
        return self.eigen_vectors @ self.eigen_values
    
    @classmethod
    def from_file(cls, filename : str | Path, times : DatetimeIndex):
        B = xr.open_dataset(filename).temporal_correlations
        times_file = DatetimeIndex(B.time.values)
        assert all(times_file == times)
        return TemporalCorrelation(B.values)
        
    @classmethod
    def from_params(cls, corlen : float, dt : float, n : int, min_corr : float = 0):
        if corlen < 1.e-20:
            B = eye(n)
        else :
            t1, t2 = meshgrid(range(n), range(n))
            B = exp(- abs(t1 - t2) * dt / corlen)
        B[B < min_corr] = 0.
        return TemporalCorrelation(B)


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
    

# @debug.timer
# def calc_temporal_correlation(
#         corlen: DateOffset,
#         dt: DateOffset, 
#         sigmas: DataFrame) -> TemporalCorrelation:
#     assert dt.base == corlen.base

#     # Number of time steps :
#     times = sigmas.loc[:, 'time'].drop_duplicates()
#     nt = times.shape[0]

#     return TemporalCorrelation(corlen=corlen.n / dt.n, dt=1., n=nt)

    
@debug.timer
def calc_temporal_correlation(
    cat : DictConfig,
    #cortype : str, 
    #corstr : DateOffset | str | Path,    # Should be either an offset (e.g. "30D") or a file path.
    #dt : DateOffset | None = None,
    sigmas : DataFrame | None = None) -> TemporalCorrelation :
    
    times = DatetimeIndex(sigmas.loc[:, 'time'].drop_duplicates())
    match cat.temporal_correlation.type:
        case "file":
            return TemporalCorrelation.from_file(cat.temporal_correlation.file, times)
        case "e" | "exp":
            corlen = to_offset(cat.temporal_correlation.correlation_length)
            dt = to_offset(cat.optimization_interval)
            assert dt.base == corlen.base
            nt = times.shape[0]
            return TemporalCorrelation.from_params(corlen=corlen.n / dt.n, dt=1., n=nt)
        case 'hybrid':
            fcorr = TemporalCorrelation.from_file(cat.temporal_correlation.file, times)
            corlen = to_offset(cat.temporal_correlation.correlation_length)
            dt = to_offset(cat.optimization_interval)
            assert dt.base == corlen.base
            nt = times.shape[0]
            dcorr = TemporalCorrelation.from_params(corlen=corlen.n / dt.n, dt=1., n=nt)
            return TemporalCorrelation(fcorr.B * dcorr.B)


@debug.timer
def calc_horizontal_correlation(
        cat : DictConfig,
        sigmas: DataFrame, 
        cache_dir : Path = None) -> SpatialCorrelation:

    logger.warning("Fix might be needed if two categories from two different tracers have the same name")

    #try :
    #    vec = sigmas.loc[(sigmas.category == cat.name)]
    #except :
    #    import pdb; pdb.set_trace()
    vec = sigmas.loc[sigmas.time == sigmas.iloc[0].time]
    match cat.horizontal_correlation.type:
        case "file":
            return SpatialCorrelation.from_file(
                filename=cat.horizontal_correlation.file, 
                lats=vec.lat.values, 
                lons=vec.lon.values, 
                cache_dir=cache_dir
            )
        case "g" | "h" | "e" | "gaussian" | "hyperbolic" | "exponential":
            # Two categories with the same name can exist, in different tracers ...
            # It would be better to have unique categories that have cat name and cat tracer as properties
            return SpatialCorrelation.from_pars(
                corlen=Quantity(cat.horizontal_correlation.correlation_length).to('km').m, 
                cortype=cat.horizontal_correlation.type,
                lats=vec.lat.values, 
                lons=vec.lon.values, 
                cache_dir=cache_dir
            )
        case "hybrid":
            # Combine a file-based correlation with a correlation-length based one
            # For that, calculate first a file-based correlation matrix (fcorr), and then a distance-based
            # one (dcorr). Then combine them using corr = (fcorr ** .5) * (dcorr ** .5)
            fcorr = SpatialCorrelation.from_file(
                filename = cat.horizontal_correlation.file,
                lats = vec.lat.values,
                lons = vec.lon.values,
                cache_dir = cache_dir)
            dcorr = SpatialCorrelation.from_pars(
                corlen = Quantity(cat.horizontal_correlation.correlation_length).to('km').m,
                cortype = cat.horizontal_correlation.correlation_type,
                lats = vec.lat.values,
                lons = vec.lon.values,
                cache_dir = cache_dir)
            return SpatialCorrelation(
                mat = fcorr.B * dcorr.B,
                cortype = 'hybrid',
                lats = vec.lat.values,
                lons = vec.lon.values,
                corlen = cat.horizontal_correlation,
                cache_dir = cache_dir
            )
