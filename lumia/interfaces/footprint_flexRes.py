#!/usr/bin/env python
import logging
from datetime import datetime
from copy import deepcopy
from numpy import zeros, meshgrid, average, float64, array, nan, unique, float32, dot, array_equal
from pandas import DataFrame
from lumia.Tools import Region, Tracers
from lumia.Tools.optimization_tools import clusterize
from lumia.Tools.time_tools import tinterv
from lumia import tqdm
from lumia.formatters.structure import Struct
from lumia.control import flexRes
from lumia.uncertainties import Uncertainties as unc 

logger = logging.getLogger(__name__)

obsoperator = 'lagrange'
invcontrol = 'flexRes'

data = {}


class Interface :

    def __init__(self, rcf, ancilliary=None, emis=None):
        self.rcf = rcf
        self.tracers = Tracers(rcf)
        self.region = Region(rcf)
        self.ancilliary_data = ancilliary
        self.data = flexRes.Control(rcf)
        if emis is not None :
            self.SetupPrior(emis)
            self.SetupUncertainties()

    def SetupPrior(self, emis):
        # Calculate the initial control vector
        vec = self.StructToVec(emis)
        self.data.setupPrior(vec)

    def SetupUncertainties(self, errclass=unc):
        err = errclass(self)
        self.data.setupUncertainties(err.dict)

    def Coarsen(self, struct):
        trac, categ, statevec, ipos, itime = [], [], [], [], []

        for tr in self.temporal_mapping.keys():
            for cat in self.temporal_mapping[tr].keys():
                tmap = self.temporal_mapping[tr][cat]['map']
                nt = tmap.shape[0]
                # Temporal coarsening
                emcat = zeros((nt, self.region.nlat, self.region.nlon))
                for it in range(nt):
                    emcat[it, :, :] = struct[tr][cat]['emis'][tmap[it, :], :, :].sum(0)

                # Spatial coarsening
                emvec = zeros((nt, self.spatial_mapping['stv'].shape[0]))
                for it in range(nt):
                    emvec[it, :] = dot(self.spatial_mapping['stv'], emcat[it, :].reshape(-1))
                    ipos.extend([cl.ipos for cl in self.spatial_mapping['cluster_specs']])
                    itime.extend([it]*emvec[it,:].size)

                # Store
                statevec.extend(emvec.reshape(-1))
                categ.extend([cat]*emvec.size)
                trac.extend([tr]*emvec.size)

        return trac, categ, statevec, ipos, itime

    def calcCoarsening(self, struct, minxsize=1, minysize=1, lsm_from_file=False):
        # Calculate spatio/temporal coarsening
        if not hasattr(self, 'spatial_mapping'):
            self.temporal_mapping = self.calc_temporal_coarsening(struct)
            self.spatial_mapping = self.calc_spatial_coarsening(minxsize=minxsize, minysize=minysize, lsm_from_file=lsm_from_file)
            self.calc_transition_matrices(self.spatial_mapping['cluster_specs'])

    def StructToVec(self, struct, lsm_from_file=False, minxsize=1, minysize=1):

        # 1. Calculate coarsening parameters
        self.calcCoarsening(struct, minxsize=minxsize, minysize=minysize, lsm_from_file=lsm_from_file)

        # 2. Apply the coarsening
        trac, categ, statevec, ipos, itime = self.Coarsen(struct)

        # 3. Store the vectors
        vec = DataFrame(columns=['tracer', 'category', 'value', 'iloc', 'time'])
        vec.loc[:, 'tracer'] = array(trac, dtype=str)
        vec.loc[:, 'category'] = array(categ, dtype=str)
        vec.loc[:, 'value'] = array(statevec, dtype=float64)
        vec.loc[:, 'iloc'] = array(ipos, dtype=int)
        vec.loc[:, 'itime'] = array(itime, dtype=int)

        # 4. Add coordinates
        for ipos in unique(vec.loc[:, 'iloc']):
            vec.loc[vec.loc[:, 'iloc'] == ipos, 'lat'] = self.spatial_mapping['cluster_specs'][ipos].mean_lat
            vec.loc[vec.loc[:, 'iloc'] == ipos, 'lon'] = self.spatial_mapping['cluster_specs'][ipos].mean_lon
            vec.loc[vec.loc[:, 'iloc'] == ipos, 'land_fraction'] = self.spatial_mapping['cluster_specs'][ipos].land_fraction
        
        # cat = categ[0] # TODO: need to fix this line specifically to allow optimization of multiple categories (there are probably more lines to fix)
        
        for tr in self.temporal_mapping.keys():
            for cat in self.temporal_mapping[tr].keys():
                for itopt, topt in enumerate(self.temporal_mapping[tr][cat]['times_optim']):
                    vec.loc[vec.itime == itopt, 'time'] = topt

        # 4. Store ancilliary data (needed for the reverse operation)
        self.ancilliary_data['vec2struct'] = vec.loc[:, ['tracer', 'category', 'iloc', 'itime']]
        self.ancilliary_data['vec2struct'].loc[:, 'prior'] = vec.loc[:, 'value']

        for tr in self.temporal_mapping.keys():
            self.ancilliary_data[tr] = {}
            for cat in struct[tr].keys():
                self.ancilliary_data[tr][cat] = struct[tr][cat]
        return vec

    def VecToStruct(self, vector):

        # 1. Create container structure, same as ancilliary data but with optimized cats set to zero 
        struct = Struct()
        for tr in self.tracers.list:
            struct[tr] = {}
            for cat in self.tracers[tr].categories:
                struct[tr][cat.name] = deepcopy(self.ancilliary_data[tr][cat.name])
                if cat.optimize:
                    struct[tr][cat.name]['emis'][:] = 0.

        # 2. Retrieve the prior control vector
        prior = self.ancilliary_data['vec2struct'].prior.values

        # 3. Disaggregate
        for tr in self.temporal_mapping.keys():
            for cat in self.temporal_mapping[tr].keys():
                tmap = self.temporal_mapping[tr][cat]['map']
                nt = tmap.shape[0]
                for it in range(nt):
                    sel = (self.ancilliary_data['vec2struct'].tracer == tr) & (self.ancilliary_data['vec2struct'].category == cat) & (self.ancilliary_data['vec2struct'].itime == it)
                    dx = (vector-prior).loc[sel].values
                    emcat = dot(dx, self.spatial_mapping['vts']).reshape((self.region.nlat, self.region.nlon)) #TODO: check this
                    
                    # switch to model temporal resolution
                    struct[tr][cat]['emis'][tmap[it, :], :, :] = self.ancilliary_data[tr][cat]['emis'][tmap[it, :], :, :] + emcat

        # 4. Convert to umol/m2/s
        if self.rcf.get('optim.unit.convert', default=False):
            struct.to_intensive()
        return struct

    def VecToStruct_adj(self, adjstruct):
        # 1. Convert adj to umol (from umol/m2/s)
        if self.rcf.get('optim.unit.convert', default=False):
            adjstruct.to_intensive()

        # 2. Aggregate
        adjvec = []
        for tr in self.temporal_mapping.keys():
            for cat in self.temporal_mapping[tr].keys():
                tmap = self.temporal_mapping[tr][cat]['map']
                nt = tmap.shape[0]
                for it in range(nt):
                    emcat = adjstruct[tr][cat]['emis'][tmap[it, :], :, :].mean(0).reshape(-1)
                    emcat = dot(self.spatial_mapping['vts'], emcat)
                    adjvec.extend(emcat)
        return adjvec

    def calc_spatial_coarsening(self, minxsize=1, minysize=1, lsm_from_file=None):
        lsm = self.region.get_land_mask(refine_factor=2, from_file=lsm_from_file)

        clusters = clusterize(
            self.ancilliary_data['sensi_map'],
            self.rcf.get('optimize.ngridpoints'),
            mask = lsm,
            minxsize=minxsize,
            minysize=minysize
        )
        mapping = {
            'clusters_map': zeros((self.region.nlat, self.region.nlon))+nan,
            'cluster_specs': []
        }
        lons, lats = meshgrid(self.region.lons, self.region.lats)
        ilons, ilats = meshgrid(range(self.region.nlon), range(self.region.nlat))
        lats, lons, ilats, ilons = lats.reshape(-1), lons.reshape(-1), ilats.reshape(-1), ilons.reshape(-1)
        area = self.region.area.reshape(-1)
        lsm = lsm.reshape(-1)
        for icl, cl in enumerate(tqdm(clusters)) :
            #indices = cl.ind.reshape(-1)
            indices = cl.ind[cl.mask]
            mapping['clusters_map'].reshape(-1)[indices] = icl
            #cl.ind = icl
            cl.indices = indices
            cl.ipos = icl
            cl.lats = lats[indices]
            cl.lons = lons[indices]
            cl.ilats = ilats[indices]
            cl.ilons = ilons[indices]
            cl.area = area[indices]
            cl.mean_lat = average(cl.lats, weights=cl.area)
            cl.mean_lon = average(cl.lons, weights=cl.area)
            cl.area_tot = cl.area.sum()
            cl.land_fraction = average(lsm[indices], weights=cl.area)
            cl.size = len(indices)
            mapping['cluster_specs'].append(cl)
        return mapping

    def calc_transition_matrices(self, clusters):
        # stv
        nm = self.region.nlat * self.region.nlon  # number of model grid points
        nv = len(clusters)
        stv_matrix = zeros((nv, nm), dtype=bool)  # bool to save space ...
        for cluster in clusters :
            stv_matrix[cluster.ipos, cluster.indices] = 1

        # vts
        vts_matrix = stv_matrix.transpose()/stv_matrix.sum(1).astype(float32)

        self.spatial_mapping['vts'] = vts_matrix.transpose()
        self.spatial_mapping['stv'] = stv_matrix

    def calc_temporal_coarsening(self, struct):
        mapping = {}

        for tr in self.tracers.list:
            for cat in self.tracers[tr].categories:
                if cat.optimize:
                    # Model times
                    times_s_model = struct[tr][cat.name]['time_interval']['time_start']
                    times_e_model = struct[tr][cat.name]['time_interval']['time_end']
                    times_model = [tinterv(s, e) for (s, e) in zip(times_s_model, times_e_model)]

                    # Find initial time 
                    t0 = datetime(times_model[0].start.year, 1, 1)
                    dt = cat.optimization_interval 

                    while t0 < times_s_model[0]:
                        t0 += dt
                    
                    # Optimization times
                    times_optim = []
                    while t0 < times_s_model[-1]:
                        times_optim.append(tinterv(t0, t0+dt))
                        t0 += dt

                    # Mapping:
                    nt_optim = len(times_optim)
                    nt_model = len(times_model)
                    mapping[tr] = {}
                    mapping[tr][cat.name] = {'map':zeros((nt_optim, nt_model)), 'times_model':times_model, 'times_optim':times_optim}
                    for imod, tmod in enumerate(times_model):
                        for iopt, topt in enumerate(times_optim):
                            mapping[tr][cat.name]['map'][iopt, imod] = tmod.overlap_percent(topt)

                    # Make sure we don't split model time steps
                    assert array_equal(mapping[tr][cat.name]['map'], mapping[tr][cat.name]['map'].astype(bool)), "Splitting model time steps it technically possible but not implemented"
                    mapping[tr][cat.name]['map'] = mapping[tr][cat.name]['map'].astype(bool)

        return mapping            