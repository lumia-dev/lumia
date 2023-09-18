#!/usr/bin/env python

from xarray import DataArray, Dataset
from omegaconf import DictConfig
from dataclasses import dataclass, field
from loguru import logger
from pandas import Timedelta, Timestamp, DataFrame, concat
from numpy import float32, zeros, average, meshgrid, array, eye
from tqdm import tqdm
from lumia.models.footprints import Data
from lumia.utils.units import units_registry as ureg
from lumia.utils.tracers import species
from lumia.utils.clusters import clusterize
from lumia.utils.time_utils import overlap_percent, interval_range
from lumia.optimizer.categories import Category
from numpy.typing import NDArray
from typing import Dict
from collections.abc import Iterable
from lumia.utils import debug


@dataclass(kw_only=True)
class Mapping:
    model_data : Data
    dconf : DictConfig = None
    sensi_map : NDArray = None
    optim_data : DataFrame = None
    spatial_mapping : Dict[Category, Dataset] = field(default_factory=dict)
    temporal_mapping : Dict[Category, Dataset] = field(default_factory=dict)

    @classmethod
    def init(cls, dconf: DictConfig, emis: Data, sensi_map: NDArray = None) -> "Mapping":
        mapping = cls(model_data=emis, dconf=dconf, sensi_map=sensi_map)
        mapping.setup_optimization()
        mapping.setup_coarsening(mapping.sensi_map)
        mapping.setup_prior()
        return mapping

    def vec_to_struct(self, vector: NDArray) -> Data:
        """
        Converts a state vector to flux array(s). The conversion follows the steps:
        0. The input state vector contains fluxes in umol, for each space/time cluster
        1. substract the prior value from the input vectors to get differences vs. the prior
        2. for each category, redistribute the fluxes from the optimization time step to the transport time step (distribflux_time)
        3. further distribute the fluxes from the spatial clusters to the transport model grid (distribflux_space)
        4. add the prior value to this difference (or just take a copy of the prior values for the non optimized categories).
        5. Convert the flux to umol/m2/s
        """

        struct = self.model_data.copy(copy_attrs=True)
        struct.resolve_metacats()
        tracer = self.optim_data.tracer
        categ = self.optim_data.category

        for cat in self.optimized_categories :
            vec = vector[(categ == cat.name) & (tracer == cat.tracer)]
            dem = self.distribflux_time(vec, cat)
            dem = self.distribflux_space(dem, cat)
            struct[cat.tracer][cat.name].data += dem

        struct.to_intensive()
        return struct

    def vec_to_struct_adj(self, adjemis : Data) -> NDArray:
        adjemis.to_intensive_adj()

        adjvec = []
        for cat in self.optimized_categories :
            emcoarse_adj = self.distribflux_space_adj(adjemis[cat.tracer][cat.name].data, cat)
            emcoarse_adj = self.distribflux_time_adj(emcoarse_adj, cat)
            adjvec.extend(emcoarse_adj)
        return array(adjvec)

    def distribflux_time(self, emcoarse: NDArray, cat: Category) -> NDArray:
        """
        Distribute the fluxes from the optimization time steps to the model time steps
        inputs:
        emcoarse: state vector (nx,), with nx = nt_optim * npoints
        cat: category name

        returns:
        emfine: (nt_model, npoints) matrix, with nt_model the numnber of model time steps
        """

        # 1) Select the transition matrix (nt_optim, nt_model)
        tmap = self.temporal_mapping[cat].overlap_fraction
        ntopt = tmap.time_optim.size
        disaggregation_matrix = tmap.data

        # Reshape the vector to a (nt_optim, npoints) array
        emcoarse = emcoarse.reshape(ntopt, -1)

        # Remap by matrix product (emfine = Tmap^t * emcoarse)
        return disaggregation_matrix.transpose() @ emcoarse

    def distribflux_time_adj(self, emcoarse_adj: NDArray, cat: Category):
        """
        Adjoint of distribuflux_time.
        Inputs:
        - emcoarse_adj: a (nt_model, npoints) matrix, containing adjoint fluxes
        - cat: the category name
        Returns:
        - emfine_adj: a (nt_optim * npoints,) vector
        """

        # 1) Select the transition matrix (n_optim, n_model)
        disaggregation_matrix = self.temporal_mapping[cat].overlap_fraction.data

        # 2) Aggregate by matrix product (emcoarse_adj = T * emfine^adj)
        emcoarse_adj = disaggregation_matrix @ emcoarse_adj

        # 3) Reshape as a vector and return
        return emcoarse_adj.reshape(-1)

    def distribflux_space(self, emcoarse : NDArray, cat: Category) -> NDArray:
        """
        Distribute the fluxes from the spatial clusters used in the optimization to the model grid.
        Input:
            - emcoarse: (nt, np) matrix, with np the number of spatial clusters
        Output:
            - emfine: (nt, nlat, nlon) matrix, with gridded fluxes
        """

        # 1) Select the transition matrix (np, nlat*nlon) and transpose it:
        disaggregation_matrix = self.spatial_mapping[cat].overlap_fraction.data

        # 2) distribute the fluxes to a (nt, nlat*nlon) matrix
        emfine = emcoarse @ disaggregation_matrix.transpose()

        # 3) reshape as a (nt, nlat, nlon) array and return:
        return emfine.reshape(self.model_data[cat.tracer].shape)
        # return emfine.reshape((-1, self.model_data[cat.tracer].grid.nlat, self.model_data[cat.tracer].grid.nlon))

    def distribflux_space_adj(self, emcoarse_adj: NDArray, cat: Category) -> NDArray:
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
        disaggregation_matrix = self.spatial_mapping[cat].overlap_fraction.data

        # 3) Regrid by matrix product: emfine = emcoarse * T^t -> (nt, np)
        emfine_adj = emcoarse_adj @ disaggregation_matrix
        return emfine_adj

    @property
    def tracers(self):
        for tr in self.model_data.tracers :
            yield tr

    @property
    def optimized_categories(self) -> Iterable[Category]:
        for cat in self.model_data.optimized_categories :
            yield cat

    def setup_optimization(self) -> None:
        """
        Read the optimization parameters (tracers, categories, uncertaintes, etc.)
        """

        for tracer in self.model_data.tracers:
            # Add meta-categories (if any!)
            for k, v in self.dconf.emissions[tracer].get('metacategories', {}).items():
                self.model_data[tracer].add_metacat(k, v)

        for cat in self.model_data.categories :
            optim_pars = self.dconf.optimize.emissions[cat.tracer].get(cat.name)
            attrs = {'optimized': optim_pars is not None}
            if optim_pars is not None:
                logger.info(f'Category {cat.name} of tracer {cat.tracer} will be optimized')
                attrs.update({
                    'optimization_interval': optim_pars.optimization_interval,
                    'apply_lsm': optim_pars.get('apply_lsm', True),
                    'is_ocean': optim_pars.get('is_ocean', False),
                    'n_optim_points': optim_pars.get('npoints', None),
                    'horizontal_correlation': optim_pars.spatial_correlation,
                    'temporal_correlation': optim_pars.temporal_correlation
                })
                err = ureg(optim_pars.annual_uncertainty)
                scf = ((1 * err.units) / species[cat.tracer].unit_budget).m
                attrs['total_uncertainty'] = err * scf
            else :
                logger.info(f'Category {cat.name} of tracer {cat.tracer} will NOT be optimized')
            self.model_data[cat.tracer].variables[cat.name].attrs.update(attrs)

    def setup_coarsening(self, sensi_map : Dict | None = None):
        """
        Calculate spatial and temporal coarsening matrices
        """
        for cat in self.optimized_categories :
            self.temporal_mapping[cat] = self.calc_temporal_coarsening(cat)
            smap = sensi_map[cat.tracer] if sensi_map else None
            self.spatial_mapping[cat] = self.calc_spatial_coarsening(cat, sensi_map=smap)

    @debug.trace_args()
    def calc_temporal_coarsening(self, cat: Category) -> Dataset :
        mapping = Dataset()

        # Model times :
        times_model = self.model_data[cat.tracer].intervals

        # Determine the optimization intervals:
        # We could just use "times_model[0]" as initial time, but we want to have interval definitions
        # that don't depend on the actual date of the inversion. I.e., if an inversion that solves for 7D
        # fluxesy, the first interval will be 1st to 7th of January, regardless of whether the inversion
        # starts on 1st january or 5th. If it solves for weekly fluxes, then the first interval should be
        # the first partially covered calendar week.
        t0 = Timestamp(self.model_data[cat.tracer].start.year, 1, 1)
        while t0 < self.model_data[cat.tracer].start :
            t0 += Timedelta(cat.optimization_interval)
        times_optim = interval_range(t0, self.model_data[cat.tracer].end, freq=cat.optimization_interval)

        # Mapping:
        nt_optim = len(times_optim)
        nt_model = len(times_model)
        catmap = DataArray(
            zeros((nt_optim, nt_model), dtype=float32),
            dims = [f'time_optim', 'time_model'],
            coords = {f'time_optim': [t.left for t in times_optim], 'time_model': [t.left for t in times_model]},
            attrs = {'interval_optim': cat.optimization_interval}
        )

        for imod, tmod in enumerate(times_model):
            for iopt, topt in enumerate(times_optim):
                catmap.data[iopt, imod] = overlap_percent(tmod, topt)
        catmap.data = (catmap.data.transpose() / catmap.data.sum(1)).transpose()
        mapping["overlap_fraction"] = catmap
        # mapping[f'time_optim_{cat.name}'] = DataArray([t.left for t in times_optim], dims=[f'time_optim_{cat.optimization_interval}'])
        mapping['timestep'] = DataArray([t.length for t in times_optim], dims=[f'time_optim_{cat.optimization_interval}'])

        return mapping

    @debug.trace_args()
    def calc_spatial_coarsening(
        self, 
        cat: Category, 
        sensi_map : NDArray = None,
        aggregate_lon : int = 1,
        aggregate_lat : int = 1,
        lsm_from_file : bool = False,
        ) -> Dataset :
        """
        Determine if (and how) the gridded emissions should be coarsened.
        Arguments :
        - cat: category descriptor (i.e. an instance of lumia.optimizer.categories.Category)
        Optional arguments :
        - sensi_map : a map of the (average) sensitivity of the network to the emissions (or whatever, really), used to determine how pixels should be clustered.
        - aggregate_lon : whether pixels should be aggregated in the longitude dimension (e.g. 2 means that the optimization will be done at half the resolution compared to the transport, i.e. at 0.5° if the transport is at 0.25°).
        - aggregate_lat : whether pixels should be aggregated in the latgitude dimension (e.g. 4 means that the optimization will be done at half the resolution compared to the transport, i.e. at 1° if the transport is at 0.25°).
        - lsm_from_file : whether a land-sea mask should be read from a file (this should only be needed to repeat inversions that have been computed with an older land-sea mask. Normally, it gets generated automatically.
        
        If a "sensi_map" is provided, then the aggregation will be done using it (i.e. on a non-regular grid). Otherwise, if either of "aggregate_lon" or "aggregate_lat" is larger than 1, a regular aggregation will be preformed (i.e. optimization on a regular grid, coarser than that of the transport). Finally, if neither of the three are provided, the optimization will be done on the same grid as the transport.
        In either case, a land-sea mask will be applied, unless specifically disabled by the category description (cat).
        
        Output:
        The method returns an xarray Dataset with five data variables :
        - overlap_fraction: This is a 2D dataset (npoint_model, npoint_optim), which tells how much a given pixel in the model space contributes to a flux component in the optimization space
        - lat, lon, area, landfraction: these are 1D datasets (npoint_optim), which provide the center coordinates, area and land-fraction of the optimized flux components.
        """

        # Determine land-sea mask (if needed):
        grid = self.model_data[cat.tracer].grid
        lsm = None
        if cat.apply_lsm :   # This is the default behaviour
            lsm = grid.get_land_mask(refine_factor=2, from_file = lsm_from_file)
            if cat.is_ocean : # This is not the default behaviour
                lsm = 1 - lsm

        if sensi_map is not None :
            return self.aggregate_in_spatial_clusters(cat, sensi_map, lsm)
        elif aggregate_lon > 1 or aggregate_lat > 1:
            return self.reduce_resolution(cat, aggregate_lon, aggregate_lat, lsm)
        else :
            return self.optimize_at_native_spatial_resolution(cat, lsm)

    @debug.trace_args()
    def reduce_resolution(self, cat: Category, aggregate_lat : int, aggregate_lon : int, lsm : None | NDArray) -> Dataset:
        raise NotImplementedError
    
    @debug.trace_args()
    def aggregate_in_spatial_clusters(self, cat: Category, sensi_map: NDArray, lsm : None | NDArray) -> Dataset:
        grid = self.model_data[cat.tracer].grid

        # Determine if we want to use a land-sea mask (and construct it!)
        # Calculate the clusters
        indices = grid.indices.reshape(grid.shape)
        clusters = clusterize(sensi_map, cat.n_optim_points, mask=lsm, cat=cat.name, indices=indices)

        lons, lats = grid.mesh(reshape=-1)
        area = grid.area.reshape(-1)

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
        
        mapping = Dataset()
        mapping["overlap_fraction"] = DataArray(
            vts_matrix,
            dims=['points_model', f'points_optim_{cat.name}'],
            coords={'points_model': grid.indices, f'points_optim_{cat.name}': [cl.ipos for cl in clusters]}
        )
        mapping[f'lat'] = DataArray([c.mean_lat for c in clusters], dims=[f'points_optim_{cat.name}'])
        mapping[f'lon'] = DataArray([c.mean_lon for c in clusters], dims=[f'points_optim_{cat.name}'])
        mapping[f'area'] = DataArray([c.area_tot for c in clusters], dims=[f'points_optim_{cat.name}'])
        mapping[f'landfraction'] = DataArray([c.land_fraction for c in clusters], dims=[f'points_optim_{cat.name}'])

        return mapping

    @debug.trace_args()
    def optimize_at_native_spatial_resolution(self, cat, lsm: None | NDArray) -> Dataset:
        grid = self.model_data[cat.tracer].grid

        # Select only the pixels where lsm is > 0:
        sel = lsm.reshape(-1) > 0
        
        # Calculate the transition matrix itself
        nm = grid.nlat * grid.nlon
        nv = (lsm > 0).sum()
        vts_matrix = eye(nm, dtype=bool)[:, sel]
        
        # Calculate the coordinates:
        lons, lats = grid.mesh()
        lons = lons.reshape(-1)[sel]
        lats = lats.reshape(-1)[sel]
        areas = grid.area.reshape(-1)[sel]
        land_fractions = lsm.reshape(-1)[sel]
        
        mapping = Dataset()
        mapping['overlap_fraction'] = DataArray(
            vts_matrix,
            dims = ['points_model', f'points_optim_{cat.name}'],
            coords={'points_model': grid.indices, f'points_optim_{cat.name}': range(nv)}
        )
        mapping[f'lat'] = DataArray(lons, dims=[f'points_optim_{cat.name}'])
        mapping[f'lon'] = DataArray(lats, dims=[f'points_optim_{cat.name}'])
        mapping[f'area'] = DataArray(areas, dims=[f'points_optim_{cat.name}'])
        mapping[f'landfraction'] = DataArray(land_fractions, dims=[f'points_optim_{cat.name}'])
        
        return mapping

    @debug.trace_args()
    def setup_prior(self) -> None :
        self.optim_data = self.control_vector.loc[:, ['category', 'tracer', 'state_prior']]

    @debug.trace_args()
    @debug.timer
    def coarsen_cat(self, cat : Category, data : NDArray = None, value_field : str = 'state_prior') -> DataFrame:
        if data is None :
            data = self.model_data[cat.tracer][cat.name].data

        tmap = self.temporal_mapping[cat].overlap_fraction.data.astype(bool)
        hmap = self.spatial_mapping[cat].overlap_fraction.data.astype(bool)
        field = tmap @ data.reshape(tmap.shape[1], -1)
        emv = (field @ hmap).reshape(-1)

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
        vec.loc[:, 'lon'] = self.spatial_mapping[cat].lon.values[ipos]
        vec.loc[:, 'lat'] = self.spatial_mapping[cat].lat.values[ipos]
        vec.loc[:, 'area'] = self.spatial_mapping[cat].area.values[ipos]
        vec.loc[:, 'land_fraction'] = self.spatial_mapping[cat].landfraction.values[ipos]
        vec.loc[:, 'time'] = self.temporal_mapping[cat].time_optim.values[itime]
        vec.loc[:, 'dt'] = self.temporal_mapping[cat].timestep.values[itime]
        vec.loc[:, 'state_prior_preco'] = 0.

        return vec

    @property
    def control_vector(self) -> DataFrame :

        # 1) Make sure we are using the good unit:
        self.model_data.to_extensive()

        # 2) Coarsen each tracer and category:
        cv = []
        for cat in self.model_data.optimized_categories :
            cv.append(self.coarsen_cat(cat))
        vectors = concat(cv, ignore_index=True)

        # 3) Initialize the remaining fields:
        vectors.loc[:, 'start_prior_preco'] = 0.

        return vectors
