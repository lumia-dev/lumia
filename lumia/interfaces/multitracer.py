#!/usr/bin/env python

from dataclasses import dataclass, field
from lumia.Tools.optimization_tools import clusterize
from numpy import meshgrid, zeros, float32, average, dot, ndarray, array
from xarray import DataArray, Dataset
from lumia.Tools.time_tools import interval_range
from pandas import DataFrame, Timestamp, concat
from tqdm import tqdm
from lumia.control.flexRes import Control
from loguru import logger
from lumia.units import units_registry as ureg
from lumia.tracers import species
from rctools import RcFile
import typing


@dataclass
class Interface:
    rcf  : RcFile
    model_data : typing.Any  # TODO: need an abstract class for this
    optim_data : DataFrame = None
    sensi_map : ndarray = None
    
    def __post_init__(self):
        self.setup_optimization()
        self.setup_coarsening(self.sensi_map)
        self.setup_prior()

    @property
    def optimized_categories(self):
        for cat in self.model_data.optimized_categories :
            yield cat

    @property
    def tracers(self):
        for tr in self.model_data.tracers :
            yield tr

    def setup_optimization(self):
        """
        Read the optimization parameters (tracers, categories, uncertaintes, etc.)
        """
        for tracer in self.tracers :
            for cat in self.model_data[tracer].categories :
                optimize_cat = self.rcf.get(f'emissions.{tracer}.{cat}.optimize', default=False)
                self.model_data[tracer][cat].attrs['optimized'] = optimize_cat
                if optimize_cat :
                    self.model_data[tracer][cat].attrs['optimization_interval'] = self.rcf.get(f'emissions.{tracer}.{cat}.optimization_interval', fallback='optimization.interval')
                    self.model_data[tracer][cat].attrs['apply_lsm'] = self.rcf.get(f'emissions.{tracer}.{cat}.apply_lsm', default=True)
                    self.model_data[tracer][cat].attrs['is_ocean'] = self.rcf.get(f'emissions.{tracer}.{cat}.is_ocean', default=False)
                    self.model_data[tracer][cat].attrs['n_optim_points'] = self.rcf.get(f'optimize.{tracer}.{cat}.npoints', fallback='optimize.npoints')
                    self.model_data[tracer][cat].attrs['horizontal_correlation'] = self.rcf.get(f'emissions.{tracer}.{cat}.corr', fallback=f'emissions.{cat}.corr')
                    self.model_data[tracer][cat].attrs['temporal_correlation'] = self.rcf.get(f'emissions.{tracer}.{cat}.tcorr', fallback=f'emissions.{cat}.tcorr')
                    err = ureg(self.rcf.get(f'emissions.{tracer}.{cat}.total_uncertainty'))
                    scf = ((1 * err.units) / species[tracer].unit_budget).m
                    self.model_data[tracer][cat].attrs['total_uncertainty'] = err.m * scf

    def setup_coarsening(self, sensi_map=None):
        """
        Calculate spatial and temporal coarsening matrices
        """
        for tracer in self.model_data.tracers :
            self.model_data[tracer].temporal_mapping = self.calc_temporal_coarsening(tracer)
            self.model_data[tracer].spatial_mapping = self.calc_spatial_coarsening(tracer, sensi_map=sensi_map[tracer])

    def calc_temporal_coarsening(self, tracer:str) -> Dataset :
        mapping = Dataset()
        for cat in self.model_data[tracer].optimized_categories :
            # Model times :
            times_model = self.model_data[tracer].intervals

            # Determine the optimization intervals: 
            # We could just use "times_model[0]" as initial time, but we want to have interval definitions
            # that don't depend on the actual date of the inversion. I.e., if an inversion that solves for 7D 
            # fluxesy, the first interval will be 1st to 7th of January, regardless of whether the inversion
            # starts on 1st january or 5th. If it solves for weekly fluxes, then the first interval should be 
            # the first partially covered calendar week.
            t0 = Timestamp(self.model_data[tracer].start.year, 1, 1)
            while t0 < self.model_data[tracer].start :
                t0 += cat.optimization_interval 
            times_optim = interval_range(t0, self.model_data[tracer].end, freq=cat.optimization_interval)

            # Mapping:
            nt_optim = len(times_optim)
            nt_model = len(times_model)
            catmap = DataArray(
                zeros((nt_optim, nt_model), dtype=float32),
                dims = [f'time_optim_{cat.optimization_interval}', 'time_model'],
                coords = {f'time_optim_{cat.optimization_interval}': [t.start for t in times_optim], 'time_model': [t.start for t in times_model]},
                attrs = {'interval_optim': cat.optimization_interval}
            )

            for imod, tmod in enumerate(times_model):
                for iopt, topt in enumerate(times_optim):
                    catmap.data[iopt, imod] = tmod.overlap_percent(topt)
            catmap.data = (catmap.data.transpose()/catmap.data.sum(1)).transpose()
            mapping[cat.name] = catmap
            mapping[f'time_optim_{cat.name}'] = DataArray([t.start for t in times_optim], dims=[f'time_optim_{cat.optimization_interval}'])
            mapping[f'timestep_{cat.name}'] = DataArray([t.dt for t in times_optim], dims=[f'time_optim_{cat.optimization_interval}'])
        
        return mapping

    def calc_spatial_coarsening(self, tracer:str, lsm_from_file=False, sensi_map=None) -> Dataset :
        mapping = Dataset()
        grid = self.model_data[tracer].grid
        for cat in self.model_data[tracer].optimized_categories :

            # Determine if we want to use a land-sea mask (and construct it!)
            lsm = None
            if cat.apply_lsm :
                lsm = grid.get_land_mask(refine_factor=2, from_file=lsm_from_file)
                if cat.is_ocean :
                    lsm = 1 - lsm

            if sensi_map is None :
                sensi_map = grid.area

            # Calculate the clusters
            indices = grid.indices.reshape(grid.shape)
            clusters = clusterize(sensi_map, cat.n_optim_points, mask=lsm, cat=cat.name, indices=indices)

            lons, lats = grid.mesh(reshape=-1)
            area = grid.area.reshape(-1)
            indices = indices.reshape(-1)

            for icl, cl in enumerate(tqdm(clusters)):
                indices = cl.ind[cl.mask]
                cl.indices = indices
                cl.ipos = icl
                cl.mean_lat = average(lats[indices], weights=area[indices])
                cl.mean_lon = average(lons[indices], weights=area[indices])
                cl.area_tot = area[indices].sum()
                if lsm is not None :
                    cl.land_fraction = average(lsm.reshape(-1)[indices], weights=area[indices])
                else :
                    cl.land_fraction = None

            # Calculate transition matrices:
            nm = grid.nlat * grid.nlon
            nv = len(clusters)
            stv_matrix = zeros((nv, nm), dtype=bool)
            for cluster in clusters :
                stv_matrix[cluster.ipos, cluster.indices] = True
            vts_matrix = stv_matrix.transpose()/stv_matrix.sum(1).astype(float32)
            mapping[cat.name] = DataArray(
                vts_matrix, 
                dims=['points_model', f'points_optim_{cat.name}'],
                coords={'points_model': grid.indices, f'points_optim_{cat.name}': [cl.ipos for cl in clusters]}
            )
            mapping[f'lat_{cat.name}'] = DataArray([c.mean_lat for c in clusters], dims=[f'points_optim_{cat.name}'])
            mapping[f'lon_{cat.name}'] = DataArray([c.mean_lon for c in clusters], dims=[f'points_optim_{cat.name}'])
            mapping[f'area_{cat.name}'] = DataArray([c.area_tot for c in clusters], dims=[f'points_optim_{cat.name}'])
            mapping[f'landfraction_{cat.name}'] = DataArray([c.land_fraction for c in clusters], dims=[f'points_optim_{cat.name}'])
            
        return mapping
    
    def control_vector(self) -> Control :
        control = Control(optimized_categories=self.optimized_categories)

        # 1) Make sure we are using the good unit:
        self.model_data.to_extensive()

        # 2) Coarsen each tracer and category:
        cv = []
        for cat in self.model_data.optimized_categories :
            cv.append(self.coarsen_cat(cat))
        control.vectors = concat(cv, ignore_index=True)

        # 3) Initialize the remaining fields:
        control.vectors.loc[:, 'start_prior_preco'] = 0.

        return control

    def setup_prior(self) -> None :
        control = self.control_vector().vectors
        self.optim_data = control.loc[:, ['category', 'tracer', 'state_prior']]

    def coarsen_cat(self, cat, data=None, value_field='state_prior') -> DataFrame:

        if data is None :
            data = self.model_data[cat.tracer][cat.name].data            

        tmap = self.model_data[cat.tracer].temporal_mapping[cat.name].astype(bool).data
        hmap = self.model_data[cat.tracer].spatial_mapping[cat.name].astype(bool).data
        field = dot(tmap, data.reshape(tmap.shape[1], -1))
        emv = dot(field, hmap).reshape(-1)
        
        nh = hmap.shape[1] 
        nt = tmap.shape[0]

        ipos, itime = meshgrid(range(nh), range(nt))
        ipos = ipos.reshape(-1)
        itime = itime.reshape(-1)

        # Return as a vector:
        vec = DataFrame(columns=['category', 'tracer', 'ipos', 'itime'])
        vec.loc[:, value_field] = emv
        vec.loc[:, 'category'] = cat.name
        vec.loc[:, 'tracer'] = cat.tracer
        vec.loc[:, 'ipos'] = ipos
        vec.loc[:, 'itime'] = itime
        vec.loc[:, 'lon'] = [self.model_data[cat.tracer].spatial_mapping[f'lon_{cat.name}'].values[i] for i in ipos]
        vec.loc[:, 'lat'] = [self.model_data[cat.tracer].spatial_mapping[f'lat_{cat.name}'].values[i] for i in ipos]
        vec.loc[:, 'area'] = [self.model_data[cat.tracer].spatial_mapping[f'area_{cat.name}'].values[i] for i in ipos]
        vec.loc[:, 'land_fraction'] = [self.model_data[cat.tracer].spatial_mapping[f'landfraction_{cat.name}'].values[i] for i in ipos]
        vec.loc[:, 'time'] = [self.model_data[cat.tracer].temporal_mapping[f'time_optim_{cat.name}'].values[t] for t in itime]
        vec.loc[:, 'dt'] = [self.model_data[cat.tracer].temporal_mapping[f'timestep_{cat.name}'].values[t] for t in itime]

        vec.loc[:, 'state_prior_preco'] = 0.

        return vec

    def VecToStruct(self, vector: ndarray, field : str = 'state'):
        """
        Converts a state vector to flux array(s). The conversion follows the steps:
        0. The input state vector contains fluxes in umol, for each space/time cluster
        1. substract the prior value from the input vectors to get differences vs. the prior
        2. for each category, redistribute the fluxes from the optimization time step to the transport time step (distribflux_time)
        3. further distribute the fluxes from the spatial clusters to the transport model grid (distribflux_space)
        4. add the prior value to this difference (or just take a copy of the prior values for the non optimized categories).
        5. Convert the flux to umol/m2/s
        """

        struct = self.model_data.new(copy_emis=True)
        emdiff = (vector - self.optim_data.state_prior).values
        tracer = self.optim_data.tracer
        categ = self.optim_data.category

        for cat in self.optimized_categories :
            vec = emdiff[(categ == cat.name) & (tracer == cat.tracer)]
            dem = self.distribflux_time(vec, cat)
            dem = self.distribflux_space(dem, cat)
            struct[cat.tracer][cat.name].data += dem

        struct.to_intensive()
        return struct

    def VecToStruct_adj(self, adjstruct):
        adjstruct.to_intensive_adj()

        adjvec = []
        for cat in self.optimized_categories :
            emcoarse_adj = self.distribflux_space_adj(adjstruct[cat.tracer][cat.name].data, cat)
            emcoarse_adj = self.distribflux_time_adj(emcoarse_adj, cat)
            adjvec.extend(emcoarse_adj)
        return array(adjvec)

    def distribflux_time(self, emcoarse, cat):
        """
        Distribute the fluxes from the optimization time steps to the model time steps
        inputs:
        emcoarse: state vector (nx,), with nx = nt_optim * npoints
        cat: category name

        returns:
        emfine: (nt_model, npoints) matrix, with nt_model the numnber of model time steps
        """

        # 1) Select the transition matrix (nt_optim, nt_model)
        tmap = self.model_data[cat.tracer].temporal_mapping
        ntopt = tmap[f'time_optim_{cat.name}'].size   # TODO: create simpler accessors for this
        T = tmap[cat.name].data

        # Reshape the vector to a (nt_optim, npoints) array
        emcoarse = emcoarse.reshape(ntopt, -1)

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

        # 1) Select the transition matrix (n_optim, n_model)
        T = self.model_data[cat.tracer].temporal_mapping[cat.name].data

        # 2) Aggregate by matrix product (emcoarse_adj = T * emfine^adj)
        emcoarse_adj = dot(T, emcoarse_adj)

        # 3) Reshape as a vector and return
        return emcoarse_adj.reshape(-1)

    def distribflux_space(self, emcoarse, cat):
        """
        Distribute the fluxes from the spatial clusters used in the optimization to the model grid.
        Input:
            - emcoarse: (nt, np) matrix, with np the number of spatial clusters
        Output:
            - emfine: (nt, nlat, nlon) matrix, with gridded fluxes
        """

        # 1) Select the transition matrix (np, nlat*nlon) and transpose it:
        T = self.model_data[cat.tracer].spatial_mapping[cat.name].data

        # 2) distribute the fluxes to a (nt, nlat*nlon) matrix
        emfine = dot(emcoarse, T.transpose())

        # 3) reshape as a (nt, nlat, nlon) array and return:
        return emfine.reshape(self.model_data[cat.tracer].shape)
        #return emfine.reshape((-1, self.model_data[cat.tracer].grid.nlat, self.model_data[cat.tracer].grid.nlon))

    def distribflux_space_adj(self, emcoarse_adj, cat):
        """
        Adjoint of distribflux_space.
        Inputs:
            - emcoarse_adj: a (nt_mod, nlat, nlon) adjoint field
        Returns:
            - emfine_adj: a (nt_mod, npoints) adjoint field
        """

        # 1) Reshape as a (nt_mod, nlat*nlon) matrix:
        emcoarse_adj = emcoarse_adj.reshape(emcoarse_adj.shape[0], -1)

        # 2) Select the spatial transition matrix (np, nlat*nlon):
        T = self.model_data[cat.tracer].spatial_mapping[cat.name].data

        # 3) Regrid by matrix product: emfine = emcoarse * T^t -> (nt, np)
        emfine_adj = dot(emcoarse_adj, T)
        return emfine_adj