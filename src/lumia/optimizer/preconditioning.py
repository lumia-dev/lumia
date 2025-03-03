#!/usr/bin/env python

from numpy.typing import NDArray
from typing import Dict
from numpy import array, append
from pandas import DataFrame
from lumia.utils import debug
import numpy as np
import xarray as xr


#@debug.timer
#def xc_to_x(xc: NDArray, temporal_correlations: Dict, horizontal_correlations: Dict, sigmas: Dict,
#            coordinates: DataFrame) -> NDArray:
#    x = array(())
#    for cat in temporal_correlations.keys():
#        Lt = temporal_correlations[cat].L
#        Lh = horizontal_correlations[cat].L
#        mask = ((coordinates.category == cat.name) & (coordinates.tracer == cat.tracer)).values
#        w = xc[mask].reshape(Lt.shape[0], Lh.shape[0])
#        x = append(x, sigmas[cat] * (Lt @ w @ Lh.transpose()).reshape(-1))
#    return x

@debug.timer
def xc_to_x(xc: NDArray,
            temporal_correlations: Dict,
            horizontal_correlations: Dict,
            sigmas: Dict,
            coordinates: DataFrame,
            category_correlations: xr.DataArray) -> NDArray:
    """
    Converts xc to x while accounting for temporal, horizontal, and cross-category correlations.
    This aggregated implementation computes the preconditioned contribution for each "from" category
    and then sums over the contributions from all "to" categories.
    """

    # This list will hold the aggregated contributions for each "from" category.
    x_list = []
    
    # Loop over the "from" categories (the inner preconditioning step)
    for cat_from in temporal_correlations.keys():
        # Get the temporal and horizontal operators for the current category.
        Lt = temporal_correlations[cat_from].L
        Lh = horizontal_correlations[cat_from].L
        
        # Create a mask to select the relevant parts of xc.
        mask = ((coordinates.category == cat_from.name) &
                (coordinates.tracer == cat_from.tracer)).values
        w = xc[mask].reshape(Lt.shape[0], Lh.shape[0])
        
        # Compute the preconditioned component for cat_from.
        x_cat = sigmas[cat_from] * (Lt @ w @ Lh.transpose()).reshape(-1)
        
        # Aggregate contributions from all "to" categories.
        combined = 0
        for cat_to in temporal_correlations.keys():
            # Get the correlation factor between cat_from and cat_to.
            corr_factor = category_correlations.loc[cat_from.name, cat_to.name].item()
            
            # Weight the computed component with the correlation factor and sum.
            combined += corr_factor * x_cat
        x_list.append(combined)
    
    # Combine all the aggregated contributions into a single array.
    return np.concatenate(x_list)

#@debug.timer
#def g_to_gc(g: NDArray, temporal_correlations: Dict, horizontal_correlations: Dict, sigmas: DataFrame,
#            coordinates: DataFrame) -> NDArray:
#    gc = array(())
#    for cat in temporal_correlations.keys():
#        Lt = temporal_correlations[cat].L
#        Lh = horizontal_correlations[cat].L
#        mask = ((coordinates.category == cat.name) & (coordinates.tracer == cat.tracer)).values
#        q = (sigmas[cat] * g[mask]).reshape(Lt.shape[0], Lh.shape[0])
#        gc = append(gc, (Lt.transpose() @ q @ Lh).reshape(-1))
#    return gc

@debug.timer
def g_to_gc(g: NDArray,
            temporal_correlations: Dict,
            horizontal_correlations: Dict,
            sigmas: Dict,
            coordinates: DataFrame,
            category_correlations: xr.DataArray) -> NDArray:
    """
    Converts g to gc while accounting for temporal, horizontal, and cross-category correlations.
    This aggregated implementation computes the preconditioned contribution for each "from" category
    and then sums over the contributions from all "to" categories.
    """
    gc_list = []
    
    # Loop over the "from" categories
    for cat_from in temporal_correlations.keys():
        # Get the operators for the current category.
        Lt = temporal_correlations[cat_from].L
        Lh = horizontal_correlations[cat_from].L
        
        # Create a mask to select the relevant part of g for the current category.
        mask = ((coordinates.category == cat_from.name) &
                (coordinates.tracer == cat_from.tracer)).values
        # Compute q: apply sigma to the selected portion of g and reshape.
        q = (sigmas[cat_from] * g[mask]).reshape(Lt.shape[0], Lh.shape[0])
        
        # Compute the preconditioned component for cat_from.
        gc_cat = (Lt.transpose() @ q @ Lh).reshape(-1)
        
        # Aggregate contributions from all "to" categories.
        combined = 0
        for cat_to in temporal_correlations.keys():
            # Get the correlation factor between cat_from and cat_to.
            corr_factor = category_correlations.loc[cat_from.name, cat_to.name].item()
            combined += corr_factor * gc_cat
        gc_list.append(combined)
    
    # Combine all aggregated contributions into a single array.
    return np.concatenate(gc_list)
