#!/usr/bin/env python

from numpy import arange, ones_like
from lumia import tqdm

class Categories:
    def __init__(self, rcf=None):
        self.list = []
        if rcf is not None :
            self.setup(rcf)

    def add(self, name):
        if not name in self.list :
            setattr(self, name, Category(name))
            self.list.append(name)
        else :
            raise RuntimeError("Category %s already exists!"%name)

    def __getitem__(self, item):
        if item in self.list :
            return getattr(self, item)
        else :
            raise KeyError("Category %s not initialized yet!"%item)

    def __iter__(self):
        for item in self.list :
            yield getattr(self, item)

    def setup(self, rcf):
        catlist = rcf.get('emissions.categories')
        for cat in catlist :
            self.add(cat)
            self[cat].optimize = rcf.get('emissions.%s.optimize'%cat, totype=bool)
            self[cat].is_ocean = rcf.get('emissions.%s.is_ocean'%cat, totype=bool, default=False)
            if self[cat].optimize :
                self[cat].uncertainty = rcf.get('emissions.%s.error'%cat)
#                self[cat].uncertainty_type = rcf.get('emissions.%s.error_type'%cat, default='tot')
                self[cat].min_uncertainty = rcf.get('emissions.%s.error_min'%cat, default=0)
                self[cat].horizontal_correlation = rcf.get('emissions.%s.corr'%cat)
                self[cat].temporal_correlation = rcf.get('emissions.%s.tcorr'%cat)
                self[cat].optimization_interval = rcf.get('optimization.interval')

    def __len__(self):
        return len(self.list)


class Category:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        return self.name == other
    

class costFunction:
    def __init__(self, bg=0, obs=None):
        self.J_obs = obs
        self.J_bg = bg

    def __getattr__(self, item):
        if item in ['J_obs', 'obs']:
            return self.J_obs
        if item in ['bg', 'J_bg','b']:
            return self.J_bg
        elif item in ['J', 'J_tot', 'tot']:
            try :
                return self.J_bg+self.J_obs
            except TypeError :
                raise RuntimeError("J_tot cannot be computed because J_obs hasn't been evaluated")
        else :
            raise AttributeError("CostFunction attributes can only be J, tot, J_tot, J_bg or J_obs")

    def __setattr__(self, key, value):
        # Modify __dict__ directly to avoid infinite recursion loops
        if key in ['obs', 'J_obs']:
            self.__dict__['J_obs'] = value
        elif key in ['bg', 'J_bg']:
            self.__dict__['J_bg'] = value
        else :
            raise AttributeError("Attribute %s not permitted"%key)
        
        
class Cluster:
    def __init__(self, data, indices=None, mask=None):
        self.data = data
        self.shape = self.data.shape
        self.ny, self.nx = data.shape
        self.size = self.nx*self.ny
        if indices is None :
            self.ind = arange(self.size).reshape((self.ny, self.nx))
        else :
            self.ind = indices
        if mask is None :
            mask = ones_like(data, dtype=bool)
        self.mask=mask
        self.rank = abs(self.data.sum())
        if self.size == 1 : self.rank = -1

    def splitx(self):
        self.transpose()
        c1, c2 = self.splity()
        c1.transpose()
        c2.transpose()
        return c1, c2

    def splity(self):
        npt = self.data.shape[0]
        isplit = int(npt/2)
        if npt%2 == 1 :
            d1 = abs(self.data[:isplit,:].sum()-self.data[isplit:,:].sum())
            d2 = abs(self.data[:isplit+1,:].sum()-self.data[isplit+1:,:].sum())
            if d2 < d1 : isplit = isplit+1
        c1 = Cluster(self.data[:isplit,:], indices=self.ind[:isplit,:], mask=self.mask[:isplit,:])
        c2 = Cluster(self.data[isplit:,:], indices=self.ind[isplit:,:], mask=self.mask[isplit:,:])
        return c1, c2

    def transpose(self):
        self.data = self.data.transpose()
        self.ind = self.ind.transpose()
        self.mask = self.mask.transpose()
        self.ny, self.nx = self.data.shape

    def split(self):
        if self.ny > self.nx :
            c1, c2 = self.splity()
        elif self.nx > self.ny :
            c1, c2 = self.splitx()
        else :
            c11, c21 = self.splity()
            d1 = abs(c11.data.sum()-c21.data.sum())
            c12, c22 = self.splitx()
            d2 = abs(c12.data.sum()-c22.data.sum())
            if d1 > d2 :
                c1, c2 = c12, c22
            else :
                c1, c2 = c11, c21
        return c1, c2


def clusterize(field, nmax, mask=None):
    clusters = [Cluster(field, mask=mask)]
    sizes = [c.size for c in clusters]
    clusters_final = []   # Offload the clusters that cannot be further divided to speed up the calculations
    with tqdm(total=min(nmax, (clusters[0].mask > 0).sum()), desc="spatial aggregation") as pbar:
        while len(clusters) < nmax and len(sizes) < sum(sizes):
            ranks = [c.rank for c in clusters]
            ind = ranks.index(max(ranks))
            cl1, cl2 = clusters[ind].split()
            clusters.pop(ind)
            increment = 0
            if cl1.mask.any() :
                if cl1.size == 1 :
                    clusters_final.append(cl1)
                else :
                    clusters.append(cl1)
                increment = 1
            if cl2.mask.any() :
                if cl2.size == 1 :
                    clusters_final.append(cl2)
                else :
                    clusters.append(cl2)
                increment = 1
            sizes = [c.size for c in clusters+clusters_final]
            pbar.update(increment)
    return clusters+clusters_final
