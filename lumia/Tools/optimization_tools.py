#!/usr/bin/env python

from numpy import shape, zeros, dot
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
                self[cat].uncertainty_type = rcf.get('emissions.%s.error_type'%cat, default='tot')
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
        
        
#def g_to_gc(G_state, Temp_Lt, Hor_Lt, g, ipos, dummy):
#    n_state = len(G_state)
#    nt = shape(Temp_Lt)[0]
#    nhor = shape(Hor_Lt)[0]
#    g_c = zeros([n_state])
#    for i in tqdm(range(nt), desc='preconditioning gradient', leave=False):
#        for j in range(nt):
#            g_c[ipos+i*nhor:ipos+(i+1)*nhor] += dot(Temp_Lt[i,j]*Hor_Lt, G_state[ipos+j*nhor:ipos+(j+1)*nhor] * g[ipos+j*nhor:ipos+(j+1)*nhor])
#    return g_c

def xc_to_x(G_state, Temp_L, Hor_L, x_c, ipos, dummy):
    n_state = len(G_state)
    nt = shape(Temp_L)[0]
    nhor = shape(Hor_L)[0]

    x = zeros(n_state)
    for i in tqdm(range(nt), desc='xc_to_x', leave=True):
        for j in tqdm(range(nt), desc='step %i/%i'%(i, nt), leave=False):
            x[ipos+i*nhor:ipos+(i+1)*nhor] += G_state[ipos+i*nhor:ipos+(i+1)*nhor]* dot(Temp_L[i,j]*Hor_L, x_c[ipos+j*nhor:ipos+(j+1)*nhor])
    return x
