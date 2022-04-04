#!/usr/bin/env python
import logging
import pdb
from datetime import datetime
from copy import deepcopy
from numpy import zeros, meshgrid, average, float64, array, nan, unique, float32, dot, array_equal
from pandas import DataFrame
from lumia.Tools import Region, Categories
from lumia.Tools.optimization_tools import clusterize
from lumia.Tools.time_tools import tinterv
from lumia import tqdm
from lumia.formatters.lagrange import Struct
from lumia.control import flexRes
from lumia.uncertainties import Uncertainties as unc
from types import SimpleNamespace

logger = logging.getLogger(__name__)

obsoperator = 'lagrange'
invcontrol = 'flexRes'

data = {}


class Interface :

    def __init__(self, rcf, ancilliary={}, emis=None, **kwargs):
        self.rcf = rcf
        self.categories = Categories(rcf)
        self.region = Region(rcf)
        self.ancilliary_data = ancilliary
        self.data = flexRes.Control(rcf)
        if emis is not None :
            self.SetupPrior(emis)
            self.time = SimpleNamespace(start=self.data.start, end=self.data.end)
            self.SetupUncertainties(**kwargs)

    def SetupPrior(self, emis):
        if self.rcf.get('optim.unit.convert', default=True):
            emis.to_extensive()  # Convert to umol
        # Calculate the initial control vector
        vec = self.StructToVec(emis)
        self.data.setupPrior(vec)

    def SetupUncertainties(self, errclass=unc):
        self.err = errclass(self)
        self.data.setupUncertainties(self.err.dict)

    def Coarsen(self, struct):
        categ, statevec, ipos, itime = [], [], [], []
        for cat in self.temporal_mapping :
            tmap = self.temporal_mapping[cat]['map']
            nt = tmap.shape[0]
            # Temporal coarsening
            emcat = zeros((nt, self.region.nlat, self.region.nlon))
            for it in range(nt):
                emcat[it, :, :] = struct[cat]['emis'][tmap[it, :], :, :].sum(0)

            # Spatial coarsening
            emvec = zeros((nt, self.spatial_mapping['stv'].shape[0]))
            for it in range(nt):
                emvec[it, :] = dot(self.spatial_mapping['stv'], emcat[it, :].reshape(-1))
                ipos.extend([cl.ipos for cl in self.spatial_mapping['cluster_specs']])
                itime.extend([it]*emvec[it,:].size)

            # Store  
            statevec.extend(emvec.reshape(-1))
            categ.extend([cat]*emvec.size)
        return categ, statevec, ipos, itime

    def calcCoarsening(self, struct, minxsize=1, minysize=1, lsm_from_file=False):
        # Calculate spatio/temporal coarsening
        if not hasattr(self, 'spatial_mapping'):
            self.temporal_mapping = self.calc_temporal_coarsening(struct)
            self.spatial_mapping = self.calc_spatial_coarsening(minxsize=minxsize, minysize=minysize, lsm_from_file=lsm_from_file)
            self.calc_transition_matrices(self.spatial_mapping['cluster_specs'])

    def StructToVec(self, struct, lsm_from_file=False, minxsize=1, minysize=1, store_ancilliary=True):

        # 1. Calculate coarsening parameters
        self.calcCoarsening(struct, minxsize=minxsize, minysize=minysize, lsm_from_file=lsm_from_file)

        # 2. Apply the coarsening
        categ, statevec, ipos, itime = self.Coarsen(struct)

        # 3. Store the vectors
        vec = DataFrame(columns=['category', 'value', 'iloc', 'time'])
        vec.loc[:, 'category'] = array(categ, dtype=str)
        vec.loc[:, 'value'] = array(statevec, dtype=float64)
        vec.loc[:, 'iloc'] = array(ipos, dtype=int)
        vec.loc[:, 'itime'] = array(itime, dtype=int)

        # 4. Add coordinates
        for ipos in unique(vec.loc[:, 'iloc']):
            vec.loc[vec.loc[:, 'iloc'] == ipos, 'lat'] = self.spatial_mapping['cluster_specs'][ipos].mean_lat
            vec.loc[vec.loc[:, 'iloc'] == ipos, 'lon'] = self.spatial_mapping['cluster_specs'][ipos].mean_lon
            vec.loc[vec.loc[:, 'iloc'] == ipos, 'land_fraction'] = self.spatial_mapping['cluster_specs'][ipos].land_fraction
#            vec.loc[vec.loc[:, 'iloc'] == ipos, 'area'] = self.spatial_mapping['cluster_specs'][ipos].area

        cat = categ[0] # TODO: need to fix this line specifically to allow optimization of multiple categories (there are probably more lines to fix)
        for itopt, topt in enumerate(self.temporal_mapping[cat]['times_optim']):
            vec.loc[vec.itime == itopt, 'time'] = topt

        # 4. Store ancilliary data (needed for the reverse operation)
        if store_ancilliary :
            self.ancilliary_data['vec2struct'] = vec.loc[:, ['category', 'iloc', 'itime']]
            self.ancilliary_data['vec2struct'].loc[:, 'prior'] = vec.loc[:, 'value']
            for cat in struct.keys():
                self.ancilliary_data[cat] = struct[cat]
        return vec

    # def VecToStruct_(self, vector):
    #
    #     # 1. Create container structure, same as ancilliary data but with optimized cats set to zero
    #     struct = Struct()
    #     for cat in self.categories:
    #         struct[cat.name] = deepcopy(self.ancilliary_data[cat.name])
    #         if cat.optimize :
    #             struct[cat.name]['emis'][:] = 0.
    #
    #     # 2. Retrieve the prior control vector
    #     prior = self.ancilliary_data['vec2struct'].prior.values
    #
    #     # 3. Disaggregate
    #     for cat in self.temporal_mapping :
    #         tmap = self.temporal_mapping[cat]['map']
    #         nt = tmap.shape[0]
    #         for it in range(nt):
    #             sel = (self.ancilliary_data['vec2struct'].category == cat) & (self.ancilliary_data['vec2struct'].itime == it)
    #             dx = (vector-prior).loc[sel].values
    #             emcat = dot(dx, self.spatial_mapping['vts']).reshape((self.region.nlat, self.region.nlon))
    #
    #             # switch to model temporal resolution
    #             struct[cat]['emis'][tmap[it, :], :, :] = self.ancilliary_data[cat]['emis'][tmap[it, :], :, :] + emcat
    #
    #     # 4. Convert to umol/m2/s
    #     if self.rcf.get('optim.unit.convert', default=True):
    #         struct.to_intensive()
    #     return struct

    def VecToStruct(self, vector):
        """
        Converts a state vector to flux array(s). The conversion follows the steps:
        0. The input state vector contains fluxes in umol, for each space/time cluster
        1. substract the prior value from the input vectors to get differences vs. the prior
        2. for each category, redistribute the fluxes from the optimization time step to the transport time step (distribflux_time)
        3. further distribute the fluxes from the spatial clusters to the transport model grid (distribflux_space)
        4. add the prior value to this difference (or just take a copy of the prior values for the non optimized categories).
        5. Convert the flux to umol/m2/s
        """
        struct = Struct()
        info = self.ancilliary_data['vec2struct']
        emdiff = vector - self.ancilliary_data['vec2struct'].prior.values
        for cat in self.categories :
            vec = emdiff[info.category == cat.name].values
            struct[cat.name] = deepcopy(self.ancilliary_data[cat.name])
            if cat.optimize :
                dem = self.distribflux_time(vec, cat.name)
                dem = self.distribflux_space(dem)
                struct[cat.name]['emis'] = struct[cat.name]['emis'] + dem

        if self.rcf.get('optim.unit.convert', default=True):
            struct.to_intensive()
        return struct

    # def VecToStruct_adj_(self, adjstruct):
    #     # 1. Convert adj to umol (from umol/m2/s)
    #     if self.rcf.get('optim.unit.convert', default=True):
    #         adjstruct.to_intensive()
    #
    #     # 2. Aggregate
    #     adjvec = []
    #     for cat in self.temporal_mapping :
    #         tmap = self.temporal_mapping[cat]['map']
    #         nt = tmap.shape[0]
    #         for it in range(nt):
    #             emcat = adjstruct[cat]['emis'][tmap[it, :], :, :].sum(0).reshape(-1)
    #             emcat = dot(self.spatial_mapping['vts'], emcat)
    #             adjvec.extend(emcat)
    #     return adjvec

    def VecToStruct_adj(self, adjstruct):
        if self.rcf.get('optim.unit.convert', default=True):
            adjstruct.to_intensive()

        adjvec = []
        for cat in self.categories :
            if cat.optimize :
                emcoarse_adj = self.distribflux_space_adj(adjstruct[cat.name]['emis'])
                emcoarse_adj = self.distribflux_time_adj(emcoarse_adj, cat.name)
                adjvec.extend(emcoarse_adj)
        return array(adjvec)

    def distribflux_space(self, emcoarse):
        """
        Distribute the fluxes from the spatial clusters used in the optimization to the model grid.
        Input:
            - emcoarse: (nt, np) matrix, with np the number of spatial clusters
        Output:
            - emfine: (nt, nlat, nlon) matrix, with gridded fluxes
        """

        # 1) Select the transition matrix:
        T = self.spatial_mapping['vts']

        # 2) distribute the fluxes to a (nt, nlat*nlon) matrix: emfine = emcoarse * T
        emfine = dot(emcoarse, T)

        # 3) reshape as a (nt, nlat, nlon) array and return:
        return emfine.reshape((-1, self.region.nlat, self.region.nlon))

    def distribflux_space_adj(self, emcoarse_adj):
        """
        Adjoint of distribflux_space.
        Inputs:
            - emcoarse_adj: a (nt_mod, nlat, nlon) adjoint field
        Returns:
            - emfine_adj: a (nt_mod, npoints) adjoint field
        """

        # 1) Reshape as a (nt_mod, nlat*nlon) matrix:
        emcoarse_adj = emcoarse_adj.reshape(emcoarse_adj.shape[0], -1)

        # 2) Select the spatial transition matrix:
        T = self.spatial_mapping['vts']

        # 3) Regrid by matrix product: emfine = emcoarse * T^t
        emfine_adj = dot(emcoarse_adj, T.transpose())
        return emfine_adj

    def distribflux_time(self, emcoarse, cat):
        """
        Distribute the fluxes from the optimization time steps to the model time steps
        inputs:
        emcoarse: state vector (nx,), with nx = nt_optim * npoints
        cat: category name

        returns:
        emfine: (nt_model, npoints) matrix, with nt_model the numnber of model time steps
        """

        # 1) Select the transition matrix
        T = self.temporal_mapping[cat]['map']

        # Reshape the vector to a (nt_optim, npoints) array
        emcoarse = emcoarse.reshape(T.shape[0], -1)

        # Remap by matrix product (emfine = Tmap^t * emcoarse)
        return dot(T.transpose(), emcoarse)

    def distribflux_time_adj(self, emcoarse_adj, cat):
        """
        Adjoint of distribuflux_time.
        Inputs:
        - emcoarse_adj: a (nt_model, npoints) matrix, containing adjoint fluxes
        - cat: the category name
        Returns:
        - emfine_adj: a (nt_optim * npoints,) vector
        """

        # 1) Select the transition matrix
        T = self.temporal_mapping[cat]['map']

        # 2) Aggregate by matrix product (emcoarse_adj = T * emfine^adj)
        emcoarse_adj = dot(T, emcoarse_adj)

        # 3) Reshape as a vector and return
        return emcoarse_adj.reshape(-1)

    def calc_spatial_coarsening(self, minxsize=1, minysize=1, lsm_from_file=None):
        lsm = self.region.get_land_mask(refine_factor=2, from_file=lsm_from_file)

        if 'sensi_map' not in self.ancilliary_data :
            self.ancilliary_data['sensi_map'] = zeros((self.region.nlat, self.region.nlon))

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
        for cat in [x for x in self.categories if x.optimize]:
            # Model times
            times_s_model = struct[cat.name]['time_interval']['time_start']
            times_e_model = struct[cat.name]['time_interval']['time_end']
            times_model = [tinterv(s, e) for (s, e) in zip(times_s_model, times_e_model)]

            # Find initial time 
            t0 = datetime(times_model[0].start.year, 1, 1)
            #dt = unit*factor
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
            mapping[cat.name] = {'map':zeros((nt_optim, nt_model)), 'times_model':times_model, 'times_optim':times_optim}
            for imod, tmod in enumerate(times_model):
                for iopt, topt in enumerate(times_optim):
                    mapping[cat.name]['map'][iopt, imod] = tmod.overlap_percent(topt)

            # Make sure we don't split model time steps
            assert array_equal(mapping[cat.name]['map'], mapping[cat.name]['map'].astype(bool)), "Splitting model time steps it technically possible but not implemented"
            mapping[cat.name]['map'] = mapping[cat.name]['map'].astype(bool)

        return mapping            