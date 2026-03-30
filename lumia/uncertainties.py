#!/usr/bin/env python

from loguru import logger
from copy import deepcopy
from multiprocessing import Pool
from tqdm import tqdm
from numpy import zeros, exp, linalg, eye, meshgrid, dot, pi, sin, cos, arcsin, flipud, argsort, sqrt, where, diag, unique, log, linspace, array
from scipy.stats import norm
from pdb import set_trace
import numpy as np
# from scipy.sparse import coo_matrix, csr_matrix
# from scipy.spatial import cKDTree


common = {}


def _aggregate_uncertainty(it1):
    itimes = common['itimes']
    sig1 = array(common['sigmas'][itimes == it1])
    Ct = common['Ct']
    Ch = common['Ch']
    nt = len(unique(itimes))
    err = 0
    for it2 in range(nt):
        sig2 = array(common['sigmas'][itimes == it2])
        err += (Ct[it1, it2] * Ch * sig1[None, :] * sig2[:, None]).sum()

    return err


def calc_dist(lon1, lat1, lon2, lat2, ae=6.371e6, stretch_ratio=1.):
    """ 
    Computes distance between two points on the globe
    The "stretch_ratio" optional argument can be used to "stretch" (stretch_ratio > 1) 
    the distances along the longitude axis (or to compress them if stretch_ratio < 1)
    """
    x1 = lon1*pi/180
    y1 = lat1*pi/180
    x2 = lon2*pi/180
    y2 = lat2*pi/180
    dy2 = (sin(0.5*(y2-y1)))**2
    dx2 = cos(y1)*cos(y2)*(sin(0.5*(x2-x1)))**2
    dd = 2*arcsin((dx2*stretch_ratio+dy2)**.5)
    ddg = dd*180/pi
    return (ddg*2*pi*0.001*ae)/360


def calc_dist_vector(iloc, stretch_ratio=1.):
    lons = common['lons']
    lats = common['lats']
    stretch_ratio = common.get('stretch_ratio', stretch_ratio)
    reflon = lons[iloc]
    reflat = lats[iloc]
    V = zeros(iloc+1)
    for ii, (lon, lat) in enumerate(zip(lons[:iloc+1], lats[:iloc+1])):
        V[ii] = calc_dist(reflon, reflat, lon, lat, stretch_ratio=1.)
    return V


def calc_dist_matrix(lats, lons, stretch_ratio=1.):
    M = zeros((len(lats), len(lons)))
    common['lons'] = lons
    common['lats'] = lats
    common['stretch_ratio'] = stretch_ratio
    with Pool() as pp :
        res = pp.map(calc_dist_vector, tqdm(range(len(lons))))
    for i, v in tqdm(enumerate(res), desc="Computing spatial distance matrix", total=len(lats)):
        M[:i+1, i] = v
        M[i, :i+1] = v
    logger.debug(f"Distance matrix computed with shape {M.shape} and stretch ratio {stretch_ratio}.")
    del common['lons'], common['lats']
    logger.debug("Latitudes and longitudes removed from common context.")
    return M


class HorCor:
    def __init__(self, corlen, cortype, lats, lons, min_eigval=0.00001):
        self.corlen = int(corlen)             # in km
        self.cortype = cortype.lower()          # 'e','g','h'
        self.lats = np.asarray(lats, dtype=np.float32)
        self.lons = np.asarray(lons, dtype=np.float32)
        self.n = len(self.lats)
        self.min_eigval = min_eigval
        if cortype == 'g' :
            self.genCovarMat = self.genGaussianCovarMat
            logger.info("Using Gaussian covariances for horizontal correlations.")
        elif cortype == 'h' :
            self.genCovarMat = self.genHyperbolicCovariances
            logger.info("Using hyperbolic covariances for horizontal correlations.")
        elif cortype == 'e' :
            self.genCovarMat = self.genExponentialCovariances
            logger.info("Using exponential covariances for horizontal correlations.")

    def _project_xy_km(self):
        R = 6371.0
        latr = np.deg2rad(self.lats)
        lonr = np.deg2rad(self.lons)
        x = lonr * R * np.cos(latr)
        y = latr * R
        return np.column_stack([x, y])
    
    # def genSparseCov(self, minw=1e-7, radius_mult=3.0):
    #     """Sparse horizontal correlation CSR (n x n)."""
    #     XY = self._project_xy_km()
    #     tree = cKDTree(XY)
    #     r = radius_mult * self.corlen
    #     sparse matrix of pairwise distances within radius r
    #     D = tree.sparse_distance_matrix(tree, max_distance=r, output_type='coo_matrix')

    #     d = D.data.astype(np.float32, copy=False)
    #     if self.cortype == 'e':
    #         w = np.exp(-d / self.corlen, dtype=np.float32)
    #     elif self.cortype == 'g':
    #         w = np.exp(-(d / self.corlen) ** 2, dtype=np.float32)
    #     else:  # 'h' hyperbolic
    #         w = 1.0 / (1.0 + d / self.corlen)

    #     keep = w >= minw
    #     W = coo_matrix((w[keep], (D.row[keep], D.col[keep])), shape=(self.n, self.n)).tocsr()
    #     W.setdiag(1.0)
    #     W.eliminate_zeros()
    #     self.mat = W                               # store CSR directly
    #     logger.debug(f"Sparse Hcor: n={self.n}, nnz={W.nnz}")
    #     return W

    # def __call__(self, sparse=True):
    #     if sparse:
    #         return self.genSparseCov()
    #     # fallback to your original dense+eigendecomp path if ever needed
    #     self.mat = self.genCovarMat()
    #     p, lam = self.eigenDecompose(self.mat)
    #     return p * lam

    def __call__(self):
        self.mat = self.genCovarMat()
        p, lam = self.eigenDecompose(self.mat)
        return p*lam 
    
    def genGaussianCovarMat(self, minv=1.e-7):
        # Get a matrix of distances
        distmat = calc_dist_matrix(self.lats, self.lons)

        # Calculate the correlations based on it
        corrmat = exp(-(distmat/self.corlen)**2)   # Gaussian covariances only for now
        corrmat[corrmat < minv] = 0.
        return corrmat

    def genExponentialCovariances(self, minv=1.e-7):
        distmat = calc_dist_matrix(self.lats, self.lons)

        corrmat = exp(-(distmat/self.corlen))
        corrmat[corrmat < minv] = 0.
        return corrmat

    def genHyperbolicCovariances(self, minv=1.e-7, stretch_ratio=2.):
        # Get a "stretched" matrix of distances
        distmat = calc_dist_matrix(self.lats, self.lons, stretch_ratio=stretch_ratio)

        # Calculate the correlations based on it
        corrmat = 1/(1+distmat/self.corlen)
        corrmat[corrmat < minv] = 0.
        return corrmat

    def eigenDecompose(self, mat):
        # Eigen value decomposition
        lam, p = linalg.eigh(mat)

        # Make positive semidefinite
        if self.min_eigval > 1.e-10 :
            min_eigval = self.min_eigval * min((1, lam.max()))
        else :
            min_eigval = self.min_eigval

        n_neg = sum(lam < min_eigval)
        lam[lam < min_eigval] = min_eigval
        logger.info(f"Maximum eigenvalue = {lam.max():10.3e}, minimum eigenvalue = {lam.min():10.3e}")
        if n_neg > 0 :
            logger.info(f"Set {n_neg} eigenvalues to {min_eigval:15.11f}")

        return p, lam**.5


class TempCor:
    def __init__(self, corlen, dt, n):
        self.corlen = corlen
        self.dt = dt
        self.n = n

    def __call__(self):
        if self.corlen < 1.e-20 :
            self.mat = eye(self.n)
            return self.mat
        self.mat = self.calcMatrix()
        P, D = self.eigenDecompose(self.mat)
        return dot(P, D)

    def calcMatrix(self):
        if self.corlen < 1.e-20 :
            return eye(self.n)
        else :
            dummy_X, dummy_Y = meshgrid(range(self.n), range(self.n))
            A = exp(-abs(dummy_X-dummy_Y)*self.dt/self.corlen)
        return A

    def eigenDecompose(self, mat):
        lam, P = linalg.eigh(mat)
        sort_order = flipud(argsort(lam))
        lam = lam[sort_order]
        P = P[:, sort_order]
        D = diag(sqrt(lam))
        # Make sure that the elements in the top row of P are non-negative
        col_sign = where(P[0]<0.0, -1.0, 1.0)
        P = P*col_sign
        return P, D


class Uncertainties:
    def __init__(self, interface, horcor=HorCor, tempcor=TempCor):
        self.interface = interface
        self.corrfile = None
        self.HorCor = horcor
        self.TempCor = tempcor

        self.dict = {
            'prior_uncertainty':None,
            'Hcor':{},
            'Tcor':{}
        }
        self.Ct = {}
        self.Ch = {}

        for tr in self.interface.tracers.list:
            self.dict['Hcor'][tr] = {}
            self.dict['Tcor'][tr] = {}
            self.Ct[tr] = {}
            self.Ch[tr] = {}
            for cat in self.interface.tracers[tr].categories:
                if cat.optimize:
                    self.dict['Hcor'][tr][cat.name] = {}
                    self.dict['Tcor'][tr][cat.name] = {}
                    self.Ct[tr][cat.name] = {}
                    self.Ch[tr][cat.name] = {}

        self.CalcUncertaintyStructure()
        logger.info("Uncertainties structure initialized.")
        self.setup_Hcor()
        logger.info("Horizontal correlations set up.")
        self.setup_Tcor()
        logger.info("Temporal correlations set up.")
        self.ScaleUncertainty()
        logger.info("Uncertainties scaled to the desired values.")

    def errStructToVec(self, errstruct):
        data = self.interface.StructToVec(errstruct)
        data.loc[:, 'prior_uncertainty'] = data.loc[:, 'value']
        return data.drop(columns=['value'])

    def setup_Hcor(self):
        for tr in self.interface.tracers.list:
            for cat in self.interface.tracers[tr].categories:
                if cat.optimize :
                    if cat.horizontal_correlation not in self.dict['Hcor'][tr][cat.name] :
                        corlen, cortype = cat.horizontal_correlation.split('-')
                        corlen = int(corlen)
                        vec = self.data.loc[(self.data.category == cat)]
                        vec = vec.loc[vec.time == vec.iloc[0].time]
                        logger.debug(f"Setting up horizontal correlation for tracer {tr}, category {cat.name}, correlation type {cortype} with length {corlen}.")
                        corr = self.HorCor(corlen, cortype, vec.lat.values, vec.lon.values)
                        logger.debug(f"Horizontal correlation matrix generated.")
                        self.dict['Hcor'][tr][cat.name][cat.horizontal_correlation] = corr()
                        self.Ch[tr][cat.name][cat.horizontal_correlation] = corr
                        logger.debug(f"Horizontal correlation matrix stored for tracer {tr}, category {cat.name}, correlation type {cortype} with length {corlen}.")

    # def setup_Hcor(self):
    #     for tr in self.interface.tracers.list:
    #         for cat in self.interface.tracers[tr].categories:
    #             if not cat.optimize: 
    #                 continue
    #             key = cat.horizontal_correlation
    #             if key in self.dict['Hcor'][tr][cat.name]:
    #                 continue
    #             corlen, cortype = key.split('-')
    #             corlen = int(corlen)
    #             vec = self.data.loc[(self.data.category == cat)]
    #             vec = vec.loc[vec.time == vec.iloc[0].time]  # one snapshot for coords
    #             corr = self.HorCor(corlen, cortype, vec.lat.values, vec.lon.values)
    #             Hcsr = corr(sparse=True)                     # <<< sparse!
    #             self.dict['Hcor'][tr][cat.name][key] = Hcsr  # store CSR directly
    #             self.Ch[tr][cat.name][key] = corr            # keep object if needed
    #             logger.debug(f"Hcor[{tr}/{cat.name}] CSR nnz={Hcsr.nnz}")


    def setup_Tcor(self):
        for tr in self.interface.tracers.list:
            for cat in self.interface.tracers[tr].categories:
                if cat.optimize :
                    if cat.temporal_correlation not in self.dict['Tcor'][tr][cat.name] :
                        
                        temp_corlen = float(cat.temporal_correlation.split('-')[0])

                        # Time interval of the optimization
                        dt = cat.optimization_interval.months + 12*cat.optimization_interval.years + cat.optimization_interval.days/30. + cat.optimization_interval.hours/30/24

                        # Number of time steps :
                        times = self.data.loc[self.data.category == cat, 'time'].drop_duplicates()
                        nt = times.shape[0]

                        corr = self.TempCor(temp_corlen, dt, nt)
                        self.dict['Tcor'][tr][cat.name][cat.temporal_correlation] = corr()
                        self.Ct[tr][cat.name][cat.temporal_correlation] = corr

    # def _total_uncertainty_fast(self, tr, cat):
    #     """
    #     Compute sqrt( trace( Ct · ( V · Ch · V^T ) ) )
    #     Inputs:
    #     tr  : tracer name (str)
    #     cat : Category object (has .name, .horizontal_correlation, .temporal_correlation)
    #     """
    #     import numpy as np
    #     from scipy.sparse import csr_matrix

    #     # 1) Keys from the Category object
    #     key_h = cat.horizontal_correlation           # e.g. "500-e"
    #     key_t = cat.temporal_correlation             # e.g. "6" (months) etc.

    #     # 2) Correlation matrices
    #     # Ch: CSR (nv x nv) stored in dict['Hcor']
    #     Ch = self.dict['Hcor'][tr][cat.name][key_h]
    #     if not isinstance(Ch, csr_matrix):
    #         Ch = csr_matrix(Ch, dtype=np.float32)
    #     # Ct: dense (nt x nt) stored in dict['Tcor']
    #     Ct = self.dict['Tcor'][tr][cat.name][key_t].astype(np.float32, copy=False)

    #     # 3) Build V (nt x nv) from the DataFrame rows for THIS category
    #     df = self.data.loc[self.data['category'] == cat.name].sort_values(['itime', 'iloc'], kind='mergesort')
    #     it = df['itime'].to_numpy(np.int32, copy=False)
    #     il = df['iloc'].to_numpy(np.int32, copy=False)
    #     nt = int(it.max()) + 1
    #     nv = int(il.max()) + 1

    #     sig = df['prior_uncertainty'].to_numpy(np.float32, copy=False)
    #     V = np.zeros((nt, nv), dtype=np.float32)
    #     V[it, il] = sig

    #     # 4) Z = V · (Ch · V^T), computed in column chunks of V to cap memory
    #     Z = np.zeros((nt, nt), dtype=np.float64)     # accumulate in f64 for accuracy
    #     chunk = 2000 if nv >= 10000 else nv

    #     for j0 in range(0, nv, chunk):
    #         j1 = min(nv, j0 + chunk)
    #         Vblk = V[:, j0:j1]           # (nt x k)
    #         W    = Ch[:, j0:j1]          # (nv x k) CSR
    #         T    = V @ W                 # (nt x k), sparse-dense matmul
    #         Z   += T @ Vblk.T            # (nt x nt)

    #     var_total = float(np.trace(Ct @ Z))
    #     return np.sqrt(var_total)


    # def calcTotalUncertainty(self):
    #     errtot = {}
    #     for tr in self.interface.tracers.list:
    #         errtot[tr] = {}
    #         for cat in self.interface.tracers[tr].categories:
    #             if not cat.optimize:
    #                 continue

    #             # units: scale sigmas temporarily (keep float32 inside)
    #             unitconv = dict(PgC=12.e-21, TgCH4=16.e-21)[cat.unit]
    #             sel = (self.data['category'] == cat.name)    # <-- use cat.name (string)
    #             bak = self.data.loc[sel, 'prior_uncertainty'].to_numpy(copy=True)
    #             self.data.loc[sel, 'prior_uncertainty'] = bak.astype(np.float32) * unitconv

    #             std = self._total_uncertainty_fast(tr, cat)
    #             errtot[tr][cat.name] = std

    #             # restore
    #             self.data.loc[sel, 'prior_uncertainty'] = bak
    #             logger.debug(f"Total original uncertainty [{tr}/{cat.name}] = {std:.3e} {cat.unit}")
    #     return errtot


    def calcTotalUncertainty(self): 
        errtot = {}
        for tr in self.interface.tracers.list:
            errtot[tr] = {}
            for cat in self.interface.tracers[tr].categories:
                unitconv = dict(PgC=12.e-21, TgCH4=16.e-21)[cat.unit]
                if cat.optimize :
                    #sig = (self.vectors.prior_uncertainty.values)
                    common['Ch'] = self.Ch[tr][cat.name][cat.horizontal_correlation].mat
                    common['Ct'] = self.Ct[tr][cat.name][cat.temporal_correlation].mat
                    common['sigmas'] = self.data.loc[self.data.category == cat].prior_uncertainty * unitconv
                    common['itimes'] = self.data.loc[self.data.category == cat].itime.values

                    nt = len(unique(common['itimes']))

                    # import pdb; pdb.set_trace()

                    with Pool() as pp :
                        errm = pp.imap(_aggregate_uncertainty, range(nt))
                        # err = [e for e in tqdm(errm, total=nt)]
                        err = sum(tqdm(errm, total=nt))

                    # here "err" is the variance, in units of [flux_unit]^2. We want something in [flux_unit] so take the square root.
                    errtot[tr][cat.name] = sqrt(err)

                    for key in ['Ch', 'Ct', 'sigmas', 'itimes'] :
                        del common[key]
                    # logger.debug(f"Total original uncertainty for category {cat}: {sum(errtot[tr][cat.name]):.3f} {cat.unit}")
                    logger.debug(f"Total original uncertainty for category {cat}: {errtot[tr][cat.name]:.3f} {cat.unit}")
        return errtot

    def CalcUncertaintyStructure(self):
        """
        Uncertainties set to a specified value (in PgC)
        """

        # The code belows first sets the standard deviations (sig_i) of the flux in each model grid cell i.
        # The standard deviation sig_x of the control vector element x that aggregates n grid cells is then given by:
        # sig_x = sqrt(\sum_i^n \sum_j^n sig_i*sig_j*corr_i_j)
        # with corr_i_j the correlation coefficient between i and j.
        # Here, since we optimize the aggregated pixels together, the correlation coefficients are by definition 1, and therefore sig_x = \sum_i^n sig_i

        # Calculate the spatio-temporal structure of the uncertainty
        data = deepcopy(self.interface.ancilliary_data)
        for tr in self.interface.tracers.list :
            for cat in self.interface.tracers[tr].categories :
                if cat.optimize :
                    # In the following code, we set the variances of the fluxes at the transport scale
                    if cat.error_structure == 'linear':
                        data[tr][cat.name]['emis'] = data[tr][cat.name]['emis']**2
                    elif cat.error_structure == 'log':
                        em = data[tr][cat.name]['emis'] ** 2
                        em = em.reshape(-1, 24, em.shape[1], em.shape[2])
                        daily_tot = em.sum((1,2,3))
                        em = (em.swapaxes(0, -1) * log(daily_tot) / daily_tot).swapaxes(0, -1)
                        data[tr][cat.name]['emis'] = em.reshape(-1, em.shape[2], em.shape[3])
                    elif cat.error_structure == 'norm':
                        em = data[tr][cat.name]['emis']**2
                        hourly_tot = em.sum((1,2))
                        hourly_frac = em / hourly_tot[:, None, None]
                        mean = hourly_tot.mean()
                        std = hourly_tot.std()
                        x = linspace(hourly_tot.min(), hourly_tot.max(), len(hourly_tot))
                        y = norm.pdf(x, mean, std*2)
                        data[tr][cat.name]['emis'] = hourly_frac * y[:, None, None]
                    elif cat.error_structure == 'abs':
                        data[tr][cat.name]['emis'] = abs(data[tr][cat.name]['emis'])
                    elif cat.error_structure == 'sqrt':
                        data[tr][cat.name]['emis'] = abs(data[tr][cat.name]['emis'])**.5
                    elif cat.error_structure == 'flat':
                        data[tr][cat.name]['emis'][:] = self.interface.region.area
                    elif cat.error_structure == 'model':
                        data[tr][cat.name]['emis'][:] = abs(data['emis_unc'][tr][cat.name]['emis'] - data[tr][cat.name]['emis'])
        # Aggregate the variances into a control vector
        self.data = self.interface.StructToVec(data, store_ancilliary=False)

        # Store the square root of this (standard deviations). They are re-converted to variances later
        self.data.loc[:, 'prior_uncertainty'] = self.data.loc[:, 'value']
        self.data.drop(columns=['value'], inplace=True)

    def ScaleUncertainty(self):
        # Scale the whole array to reach the desired total uncertainty value:
        errtot = self.calcTotalUncertainty()

        # Divide by the simulation length:
        nsec = (self.interface.time.end - self.interface.time.start).total_seconds()
        nsec_year = 365*86400.

        for tr in self.interface.tracers.list:
            for cat in self.interface.tracers[tr].categories:
                if cat.optimize :
                    scalef = cat.uncertainty / errtot[tr][cat.name] * nsec / nsec_year 
                    self.data.loc[self.data.category == cat, 'prior_uncertainty'] *= scalef
                    logger.info(f"Uncertainty for category {cat.name} set to {cat.uncertainty} {cat.unit} (standard deviations scaled by {scalef = })")

        _ = self.calcTotalUncertainty()
        for tr in self.interface.tracers.list:
            for cat in self.interface.tracers[tr].categories:
                if cat.optimize :
                    logger.info(f"{_[tr][cat.name] = }")
        
        self.dict['prior_uncertainty'] = self.data.prior_uncertainty