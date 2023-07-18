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


@dataclass
class PriorConstraints:
    temporal_correlations: Dict
    horizontal_correlations: Dict
    sigmas: Dict
    vectors: DataFrame

    def __post_init__(self):
        self.state_preco = zeros(self.size)

    @property
    def coordinates(self) -> DataFrame:
        return self.vectors.loc[:, ['category', 'tracer']]

    @property
    def size(self) -> int:
        return self.vectors.shape[0]

    @classmethod
    @debug.trace_call
    def setup(cls, mapping: Mapping) -> "PriorConstraints":
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
                errvec
            )
            corr_h[cat] = calc_horizontal_correlation(
                cat.name, cat.horizontal_correlation, errvec
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

    @property
    def categories(self) -> List[Category]:
        return list(self.sigmas.keys())

    def save(self, dest: Path):
        for cat in self.categories:
            tbl = self.vectors.loc[(self.vectors.tracer == cat.tracer) & (self.vectors.category == cat.name)].to_xarray()
            tbl.attrs = cat.ncattrs
            tbl.to_netcdf(dest, group=f'{cat.tracer}.{cat.name}')
