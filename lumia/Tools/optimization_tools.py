#!/usr/bin/env python

import re
from dateutil.relativedelta import relativedelta
from numpy import arange, ones_like, array, cumsum
from lumia import tqdm


class Categories:
    def __init__(self, rcf=None):
        self.list = []
        if rcf is not None :
            self.setup(rcf)

    def add(self, name):
        if name not in self.list :
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
            self[cat].optimize = rcf.get('emissions.%s.optimize'%cat, totype=bool, default=False)
            self[cat].is_ocean = rcf.get('emissions.%s.is_ocean'%cat, totype=bool, default=False)
            if self[cat].optimize :
                self[cat].uncertainty = rcf.get('emissions.%s.error'%cat)
#                self[cat].uncertainty_type = rcf.get('emissions.%s.error_type'%cat, default='tot')
                self[cat].min_uncertainty = rcf.get('emissions.%s.error_min'%cat, default=0)
                self[cat].horizontal_correlation = rcf.get('emissions.%s.corr'%cat)
                self[cat].temporal_correlation = rcf.get('emissions.%s.tcorr'%cat)
                optint = rcf.get('optimization.interval')
                if re.match('\d*y', optint):
                    n = re.split('d', optint)
                    u = relativedelta(years=1)
                elif re.match('\d*m', optint):
                    n = re.split('m', optint)
                    u = relativedelta(months=1)
                elif re.match('\d*d', optint):
                    n = re.split('d', optint)
                    u = relativedelta(days=1)
                elif re.match('\d*h', optint):
                    n = re.split('h', optint)
                    u = relativedelta(hours=1)
                n = int(n) if n != '' else 1
                self[cat].optimization_interval = n*u 
                #self[cat].optimization_interval = rcf.get('optimization.interval')

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
    def __init__(self, data, indices=None, mask=None, dy=None, crop=True):
        self.data = data
        self.shape = self.data.shape
        self.ny, self.nx = data.shape
        self.size = self.nx*self.ny
        self.dy = self.nx if dy is None else dy
        if indices is None :
            self.ind = arange(self.size).reshape((self.ny, self.nx))
        else :
            self.ind = indices
        if mask is None :
            mask = ones_like(data, dtype=bool)
        self.mask=mask
        self.rank = abs(self.data.sum())
        if self.size == 1 : 
            self.rank = -1
        if crop: 
            self.crop()

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
            if d2 < d1 : 
                isplit = isplit+1
        c1 = Cluster(self.data[:isplit,:], indices=self.ind[:isplit,:], mask=self.mask[:isplit,:], dy=self.dy)
        c2 = Cluster(self.data[isplit:,:], indices=self.ind[isplit:,:], mask=self.mask[isplit:,:], dy=self.dy)
        return [c1, c2]

    def transpose(self):
        self.data = self.data.transpose()
        self.ind = self.ind.transpose()
        self.mask = self.mask.transpose()
        self.ny, self.nx = self.data.shape
        self.shape = self.data.shape

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
        return [c1, c2]

    def crop(self):
        """
        This crops the edge row/columns if they are completely masked
        """
        mask_r = self.mask.sum(0) > 0
        mask_c = self.mask.sum(1) > 0
        mask_r = cumsum(mask_r, dtype=bool)*cumsum(mask_r[::-1], dtype=bool)[::-1]
        mask_c = cumsum(mask_c, dtype=bool)*cumsum(mask_c[::-1], dtype=bool)[::-1]
        return Cluster(
            self.data[mask_c, :][:, mask_r], 
            indices=self.ind[mask_c, :][:, mask_r], 
            mask=self.mask[mask_c,:][:, mask_r], 
            dy=self.dy,
            crop=False
        )

    def splitByMask(self):
        indices = self.ind[self.mask > 0]
        new_clusters = []
        while len(indices) > 0 :
            cl2 = self._walk(indices[0])
            newmask = array([c in cl2 for c in self.ind.reshape(-1)]).reshape(self.shape)
            newcl = Cluster(
                self.data, 
                self.ind, 
                mask=newmask, 
                dy=self.dy
            )
            new_clusters.append(newcl)
            indices = [ii for ii in indices if ii not in cl2]
        return new_clusters

    def _find_neighbours(self, ind):
        neighbours = [ind-1, ind+1, ind-self.dy, ind+self.dy]
        return [n for n in neighbours if n in self.ind[self.mask>0]]

    def _walk(self, n0, neighbours=[]):
        if len(neighbours) == 0 : 
            neighbours = [n0]
        new_neigbours = self._find_neighbours(n0)
        new_neigbours = [n for n in new_neigbours if n not in neighbours]
        neighbours.extend(new_neigbours)
        for nb in new_neigbours:
            neighbours = self._walk(nb, neighbours)
        return neighbours


def clusterize(field, nmax, mask=None):
    clusters = [Cluster(field, mask=mask, crop=False)]
    clusters_final = []   # Offload the clusters that cannot be further divided to speed up the calculations
    nclmax = min(nmax, (clusters[0].mask > 0).sum())
    with tqdm(total=nclmax, desc="spatial aggregation") as pbar:
        ncl = len(clusters+clusters_final)
        while ncl < nclmax :  # and len(Cluster) > 0 :
            ranks = [c.rank for c in clusters]
            ind = ranks.index(max(ranks))
            new_clusters = clusters[ind].split()
            clusters.pop(ind)
            for cl in new_clusters :
                if cl.mask.any() :
                    if cl.size == 1 :
                        clusters_final.append(cl)
                    else :
                        if mask is not None :
                            clusters.extend(cl.splitByMask())
                        else :
                            clusters.append(cl)
            inc = len(clusters+clusters_final)-ncl
            pbar.update(inc)
            ncl += inc
    return clusters+clusters_final
