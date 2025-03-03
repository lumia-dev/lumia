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
from numpy import sqrt, array, ones, eye, kron
import numpy as np
from scipy.sparse.linalg import LinearOperator
import xarray as xr


@dataclass
class PriorConstraints:
    temporal_correlations: Dict
    horizontal_correlations: Dict
    sigmas: Dict
    vectors: DataFrame
    category_correlations: np.ndarray  # field for the categorical correlations

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
    def setup(cls, dconf: dict | DictConfig, mapping: Mapping) -> "PriorConstraints":
        vectors = []
        sigmas, corr_t, corr_h = {}, {}, {}
        # We'll store the temporal and horizontal correlation matrices for each category
        # so we can later combine them with the categorical correlation.
        full_cov_data = {}

        # Convert the generator to a list so we can index and iterate multiple times.
        categories = list(mapping.optimized_categories)

        for cat in categories:
            # Error in the model space is proportional to the absolute value of the flux.
            errmap = abs(mapping.model_data[cat.tracer][cat.name])
    
            # Aggregate model-space errors into the optimization space.
            errvec = mapping.coarsen_cat(cat, data=errmap.data, value_field='prior_uncertainty')
    
            # Calculate the covariance matrices (each .B attribute holds the underlying NumPy array).
            corr_t[cat] = calc_temporal_correlation(
                to_offset(cat.temporal_correlation),
                to_offset(cat.optimization_interval),
                errvec
            )
            corr_h[cat] = calc_horizontal_correlation(
                cat.name, 
                cat.horizontal_correlation, 
                errvec,
                cache_dir=dconf.get('cache_dir', None)
            )
    
            # Calculate the current total uncertainty for the category.
            errtot = calc_total_uncertainty(errvec, corr_t[cat].B, corr_h[cat].B, cat.unit_optim, cat.total_uncertainty.units)
            logger.info(f"Original uncertainty for category {cat.name}: {errtot:.3f} {cat.total_uncertainty.units}")
    
            # Deduce a scaling factor for the prior_uncertainty column (scale by simulation length).
            nsec = errvec.loc[:, ['itime', 'dt']].drop_duplicates().dt.sum().total_seconds()
            scalef = (cat.total_uncertainty.m / errtot) * (nsec / (365.25 * 86400))
            errvec.loc[:, 'prior_uncertainty'] *= scalef
            logger.info(
                f"Uncertainty for category {cat.name} set to {cat.total_uncertainty.magnitude} {cat.total_uncertainty.units} "
                f"(standard deviations scaled by {scalef = })"
            )
    
            # Store the per-category uncertainty vector and the error vector.
            sigmas[cat] = errvec.prior_uncertainty.values
            vectors.append(errvec)
    
            # Instead of forming the full (temporal ⊗ horizontal) matrix now, save the pair for later combination.
            full_cov_data[cat] = (corr_t[cat].B, corr_h[cat].B)
    
        vectors = concat(vectors)

        # Replace print statements with logger calls
        logger.info("Before adding the global factor, prior uncertainty: {}", vectors.prior_uncertainty)
        unitconv = 12.01 * 1e-6 * 1e-15 # umol to PgC
        uncertainty_before = np.sqrt(np.sum(vectors.prior_uncertainty**2)) * unitconv
        logger.info("Uncertainty before applying global factor: {:.6f} {}", uncertainty_before, 'PgC')
    
        # --- Combine cross-category correlation ---
                
        n_cat = len(categories)
        if n_cat > 0:
            # Define the categorical correlation matrix, Cc.
            # Off-diagonals are set via configuration (default 0.5).

            rho_cat = dconf.get("category_correlation", 0.0)
            Cc = np.full((n_cat, n_cat), rho_cat)
            np.fill_diagonal(Cc, 1.0)
            
    
            # Use a representative category for the temporal and horizontal matrices.
            rep_cat = categories[0]
            Ct = corr_t[rep_cat].B    # shape: (n_time, n_time)
            Ch = corr_h[rep_cat].B    # shape: (n_h, n_h)
    
            # Combine categorical and temporal correlations: Cct = Cc ⊗ Ct.
            Cct = np.kron(Cc, Ct)
            logger.info(f"Combined time-category matrix (Cct) shape: {Cct.shape}")

            # Create an xarray DataArray from Cc with coordinates.
            cat_names = [cat.name for cat in categories]
            Cc_da = xr.DataArray(
                Cc,
                dims=["category_from", "category_to"],
                coords={"category_from": cat_names, "category_to": cat_names},
                name="category_correlations"
            )
          
            # --- Create a LinearOperator for the Full Covariance Including Cross-Category ---
            # Full covariance: C_full = Cct ⊗ Ch.
            # We use a matrix–free approach so as not to allocate huge arrays.

            def kron_ct_ch_matvec(x, Cct, Ch):
                """
                Computes y = (Cct ⊗ Ch)x without forming the full matrix.
                Reshape x into a matrix of shape (n_h, n_cat*n_time) using Fortran order,
                then compute Y = Ch @ X @ Cct^T, and flatten Y back.
                """
                n_cat_nt = Cct.shape[0]
                n_h = Ch.shape[0]
                X = x.reshape((n_h, n_cat_nt), order='F')
                Y = Ch @ X @ Cct.T
                return Y.ravel(order='F')
    
            full_shape = (n_cat * Ct.shape[0] * Ch.shape[0], n_cat * Ct.shape[0] * Ch.shape[0])
            logger.info("Full covariance operator shape: {}", full_shape)
            
            full_cov_operator = LinearOperator(shape=full_shape,
                                             matvec=lambda x: kron_ct_ch_matvec(x, Cct, Ch))

            logger.info("Combined full covariance operator shape: {}", full_cov_operator.shape)
        else:
            full_cov_operator = None

        # --- Adjust sigmas to maintain overall uncertainty ---
        # Assemble the full sigma vector in the same order as used for the Kronecker products.
        sigma_full = np.concatenate([sigmas[cat] for cat in categories])
        # Compute the effective variance via the operator:
        effective_variance = np.dot(sigma_full, full_cov_operator.matvec(sigma_full))
        effective_uncertainty = np.sqrt(effective_variance)
        logger.debug("Effective uncertainty before sigma adjustment: {}", effective_uncertainty)
    
        # --- Use unit conversion for target uncertainty ---

        global_target = np.sqrt(sum((cat.total_uncertainty.m * (nsec / (365.25 * 86400)) * (1/unitconv))**2 for cat in categories))
    
        scaling_factor = global_target / effective_uncertainty
        logger.info(f"Effective uncertainty before sigma adjustment: {effective_uncertainty:.3f} {'umol'}")
        logger.info(f"Global target uncertainty: {global_target:.3f} {'umol'}")
        logger.info(f"Scaling factor to adjust sigmas: {scaling_factor:.3f}")
    
        # Adjust the full sigma vector.
        sigma_adjusted = scaling_factor * sigma_full
    
        # Update the per-category sigmas dictionary.
        start = 0
        for cat in categories:
            length = len(sigmas[cat])
            sigmas[cat] = sigma_adjusted[start:start+length]
            start += length

        # Optionally, reassemble sigma_full from the adjusted sigmas if needed.
        sigma_full_adjusted = np.concatenate([sigmas[cat] for cat in categories])
        # Update the 'prior_uncertainty' column in the vectors DataFrame.
        vectors.loc[:, 'prior_uncertainty'] = sigma_full_adjusted
        # Compute new effective uncertainty for diagnostics:
        new_effective_variance = np.dot(sigma_full_adjusted, full_cov_operator.matvec(sigma_full_adjusted))
        new_effective_uncertainty = np.sqrt(new_effective_variance)
        logger.info(f"Global uncertainty after sigma adjustment: {new_effective_uncertainty:.3f} {'umol'}")
    
       
        logger.debug("Adjusted sigmas in vectors: {}", vectors.prior_uncertainty)
        unitconv = 12.01 * 1e-6 * 1e-15 # umol to PgC
        uncertainty_after= np.sqrt(np.sum(vectors.prior_uncertainty**2)) * unitconv
        logger.info("Uncertainty after applying global factor: {:.6f} {}", uncertainty_after, ('PgC') )
        
    
        return cls(
            sigmas=sigmas,
            temporal_correlations=corr_t,
            horizontal_correlations=corr_h,
            vectors=vectors,
            category_correlations=Cc_da  # Save the DataArray with category coordinates
        )

    
    
    @property
    def categories(self) -> List[Category]:
        return list(self.sigmas.keys())

    def save(self, dest: Path):
        for cat in self.categories:
            tbl = self.vectors.loc[(self.vectors.tracer == cat.tracer) & (self.vectors.category == cat.name)].to_xarray()
            tbl.attrs = cat.ncattrs
            tbl.to_netcdf(dest, group=f'{cat.tracer}.{cat.name}')
