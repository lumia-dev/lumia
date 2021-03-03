#!/usr/bin/env python

import logging
from copy import deepcopy
from multiprocessing import Pool
from tqdm import tqdm
from numpy import zeros, exp, linalg, eye, meshgrid, dot, pi, sin, cos, arcsin, flipud, argsort, sqrt, where, diag, unique

logger = logging.getLogger(__name__)

common = {}


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
    del common['lons'], common['lats']
    return M


class HorCor:
    def __init__(self, corlen, cortype, lats, lons, min_eigval=0.00001):
        self.corlen = corlen
        self.cortype = cortype
        self.lats = lats
        self.lons = lons
        self.n = len(self.lats)
        self.min_eigval = min_eigval
        if cortype == 'g' :
            self.genCovarMat = self.genGaussianCovarMat
        elif cortype == 'h' :
            self.genCovarMat = self.genHyperbolicCovariances

    def __call__(self):
        self.mat = self.genCovarMat()
        p, lam = self.eigenDecompose(self.mat)
        return p*lam   # TODO: check why this is not a dot product
    
    def genGaussianCovarMat(self, minv=1.e-7):
        # Get a matrix of distances
        distmat = calc_dist_matrix(self.lats, self.lons)

        # Calculate the correlations based on it
#        import pdb; pdb.set_trace()
        corrmat = exp(-(distmat/self.corlen)**2)   # Gaussian covariances only for now
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


def aggregate_uncertainty(itime):
    err = 0.
    for ivar in range(len(common['std'])):
        std1 = common['std'][ivar]
        corr_t = common['Bt'][common['itime'][ivar], itime]
        std2 = common['std'][common['itime'] == itime]
        corr_h = common['Bh'][common['iloc'][ivar], :]
        err += sum(std1*corr_t*(corr_h*std2))
    return err


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

        self.calcPriorUncertainties()
        self.setup_Hcor()
        self.setup_Tcor()

    def errStructToVec(self, errstruct):
        data = self.interface.StructToVec(errstruct)
        data.loc[:, 'prior_uncertainty'] = data.loc[:, 'value']
        return data.drop(columns=['value'])

    def calcPriorUncertainties(self):
        """
        Uncertainties set to a percentage of the prior control vector
        """
        data = deepcopy(self.interface.ancilliary_data)
        data = self.errStructToVec(data)
        for cat in self.interface.categories :
            if cat.optimize :
                errfact = cat.uncertainty*0.01
                errcat = abs(data.loc[data.category == cat, 'prior_uncertainty'].values)*errfact
                errcat[(errcat < 0.01*errcat.max())*(data.loc[:, 'land_fraction']>0)] = errcat.max()/100
                data.loc[data.category == cat, 'prior_uncertainty'] = errcat
        self.data = data
        self.dict['prior_uncertainty'] = data.prior_uncertainty

    def setup_Hcor(self):
        for cat in self.interface.categories :
            if cat.optimize :
                if cat.horizontal_correlation not in self.dict['Hcor'] :

                    corlen, cortype = cat.horizontal_correlation.split('-')
                    corlen = int(corlen)
                    vec = self.data.loc[(self.data.category == cat)]
                    vec = vec.loc[vec.time == vec.iloc[0].time]

                    corr = self.HorCor(corlen, cortype, vec.lat.values, vec.lon.values)
                    self.dict['Hcor'][cat.horizontal_correlation] = corr()
                    self.hcov = corr.mat

    def setup_Tcor(self):
        for cat in self.interface.categories :
            if cat.optimize :
                if cat.temporal_correlation not in self.dict['Tcor'] :
                    temp_corlen = float(cat.temporal_correlation[:3].strip())

                    # Time interval of the optimization
                    dt = cat.optimization_interval.months + 12*cat.optimization_interval.years + cat.optimization_interval.days/30. + cat.optimization_interval.hours/30/24

                    # Number of time steps :
                    times = self.data.loc[self.data.category == cat, 'time'].drop_duplicates()
                    nt = times.shape[0]
                    
                    corr = self.TempCor(temp_corlen, dt, nt)
                    self.dict['Tcor'][cat.temporal_correlation] = corr()
                    self.tcov = corr.mat

    def calcTotalError(self):
        common['std'] = self.dict['prior_uncertainty'].values
        common['Bh'] = self.hcov
        common['Bt'] = self.tcov
        common['itime'] = self.data.loc[:, 'itime'].values
        common['iloc'] = self.data.loc[:, 'iloc'].values

        with Pool() as pp :
            err = [e for e in tqdm(pp.imap(aggregate_uncertainty, unique(common['itime'])), total=self.tcov.shape[0])]

        for key in ['std','Bh','Bt','itime','iloc'] :
            del common[key]

        return sum(err)