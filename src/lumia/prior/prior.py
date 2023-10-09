#!/usr/bin/env python

from dataclasses import dataclass
from typing import Dict, List
from pandas import DataFrame, concat
from pandas.tseries.frequencies import to_offset
from .protocols import Mapping
from loguru import logger
from .uncertainties import calc_temporal_correlation, calc_horizontal_correlation, calc_total_uncertainty
from numpy import zeros
from lumia.optimizer.categories import Category
from pathlib import Path
from lumia.utils import debug
from omegaconf import DictConfig
from .uncertainties import TemporalCorrelation, SpatialCorrelation, CategoryCorrelation
from numpy.typing import NDArray


@dataclass
class PriorConstraints:
    temporal_correlations: Dict[str, TemporalCorrelation]
    horizontal_correlations: Dict[str, SpatialCorrelation]
    sigmas: Dict[str, NDArray]
    vectors: DataFrame
    category_correlations : Dict[str, CategoryCorrelation] | None = None

    def __post_init__(self):
        self.state_preco = zeros(self.size)

    @property
    def coordinates(self) -> DataFrame:
        return self.vectors.loc[:, ['category', 'tracer']]

    @property
    def size(self) -> int:
        return self.vectors.shape[0]

    @classmethod
    @debug.trace_args()
    def setup(cls, dconf: Dict | DictConfig, mapping: Mapping) -> "PriorConstraints":
        vectors = []
        sigmas, corr_t, corr_h = {}, {}, {}
        for cat in mapping.optimized_categories:
            # Error, in the model space, is proportional to the absolute value of the flux
            errmap = abs(mapping.model_data[cat.tracer][cat.name])

            # Error, in the optim space, is obtained by aggregating model-space errors following the same approach as for aggregating fluxes themselves
            errvec = mapping.coarsen_cat(cat, data=errmap.data, value_field='prior_uncertainty')

            # Calculate (square root of the inverse of) the covariance matrices
            corr_t[cat] = calc_temporal_correlation(
                to_offset(cat.temporal_correlation),
                to_offset(cat.optimization_interval),
                nt = len(errvec.itimes.drop_duplicates())
            )
            corr_h[cat] = calc_horizontal_correlation(
                cat.horizontal_correlation, 
                coords=errvec.loc[errvec.itime == 0, ['lat', 'lon']],
                cache_dir=dconf.get('cache_dir', None)
            )

            # Calculate the current total uncertainty for the category:
            errtot = calc_total_uncertainty(errvec, corr_t[cat].B, corr_h[cat].B, cat.unit_optim, cat.total_uncertainty.units)
            logger.debug(f"Original uncertainty for category {cat.name}: {errtot:.3f} {cat.total_uncertainty.units}")

            # Deduce a scaling factor for the prior_uncertainty column:
            # Scale also by the simulation length, as uncertainty is provided in units/year
            nsec = errvec.loc[:, ['itime', 'dt']].drop_duplicates().dt.sum().total_seconds()
            scalef = (cat.total_uncertainty / errtot) * (nsec / (365.25 * 86400))

            errvec.loc[:, 'prior_uncertainty'] *= scalef
            logger.info(
                f"Uncertainty for category {cat.name} set to {cat.total_uncertainty.magnitude} {cat.total_uncertainty.units} (standard deviations scaled by {scalef = })")

            # Store the results
            sigmas[cat] = errvec.prior_uncertainty.values
            vectors.append(errvec)

        vectors = concat(vectors)

        return cls(sigmas=sigmas, temporal_correlations=corr_t, horizontal_correlations=corr_h, vectors=vectors)

    @classmethod
    @debug.trace_args()
    def setup_(cls, dconf: Dict | DictConfig, mapping: Mapping) -> "PriorConstraints":
        vectors = []
        sigmas, corr_t, corr_h, corr_c = {}, {}, {}, {}
        for optim_group, optim_pars in dconf.groups.items():
            vecs = []
            
            # Setup the cross-category correlations
            corr_c[optim_group] = CategoryCorrelation(optim_pars.annual_uncertainty.keys())
            for cats, corr in optim_pars.cross_correlations.items():
                cat1, cat2 = cats.split(',')
                corr_c[optim_group].correlate(cat1.strip(), cat2.strip(), corr)
            
            # Loop over the categories in the group and setup their uncertainties:
            for catname in optim_pars.annual_uncertainty.keys():
                
                cat = mapping.categories[catname]
                
                # Error, in the model space, is proportional to the absolute value of the flux
                errmap = abs(mapping.model_data[cat.tracer][cat.name])
                
                # Error, in the optim space, is obtained by aggregating model-space errors following the same approach as for aggregating fluxes themselves
                errvec = mapping.coarsen_cat(cat, data=errmap.data, value_field='prior_uncertainty')
                
                errvec.loc[:, 'optim_group'] = optim_group
                
                # Just setup the correlations for the first category, the others are the same:
                if optim_group not in corr_t:
                    corr_t[optim_group] = calc_temporal_correlation(
                        to_offset(optim_pars.temporal_correlation),
                        to_offset(optim_pars.optimization_interval),
                        nt = len(errvec.itimes.drop_duplicates())
                    )
                
                    corr_h[optim_group] = calc_horizontal_correlation(
                        optim_pars.horizontal_correlation, 
                        coords=errvec.loc[errvec.itime == 0, ['lat', 'lon']],
                        cache_dir=dconf.get('cache_dir', None)
                    )

                # Calculate the current total uncertainty for the category:
                errtot = calc_total_uncertainty(errvec, corr_t[optim_group].B, corr_h[optim_group].B, cat.unit_optim, cat.total_uncertainty.units)
                logger.debug(f"Original uncertainty for category {cat.name}: {errtot:.3f} {cat.total_uncertainty.units}")

                # Deduce a scaling factor for the prior_uncertainty column:
                # Scale also by the simulation length, as uncertainty is provided in units/year
                nsec = errvec.loc[:, ['itime', 'dt']].drop_duplicates().dt.sum().total_seconds()
                scalef = (cat.total_uncertainty / errtot) * (nsec / (365.25 * 86400))
                
                errvec.loc[:, 'prior_uncertainty'] *= scalef
                logger.info(
                    f"Uncertainty for category {cat.name} set to {cat.total_uncertainty.magnitude} {cat.total_uncertainty.units} (standard deviations scaled by {scalef = })")
                
                # Store the results
                vecs.append(errvec)
            
            vecs = concat(vecs)
            
            sigmas[optim_group] = vecs.prior_uncertainty.values
            vectors.append(vecs)

        vectors = concat(vectors)
        return cls(
            sigmas=sigmas, 
            temporal_correlations=corr_t, 
            horizontal_correlations=corr_h, 
            category_correlations=corr_c, 
            vectors=vectors
            )

    @property
    def categories(self) -> List[Category]:
        return list(self.sigmas.keys())

    def save(self, dest: Path):
        for cat in self.categories:
            tbl = self.vectors.loc[(self.vectors.tracer == cat.tracer) & (self.vectors.category == cat.name)].to_xarray()
            tbl.attrs = cat.ncattrs
            tbl.to_netcdf(dest, group=f'{cat.tracer}.{cat.name}')
