#!/usr/bin/env python

from numpy.typing import NDArray
from typing import Dict
from numpy import array, append, kron
from pandas import DataFrame
from lumia.utils import debug


@debug.timer
def xc_to_x(xc: NDArray, temporal_correlations: Dict, horizontal_correlations: Dict, sigmas: Dict,
            coordinates: DataFrame) -> NDArray:
    x = array(())
    for cat in temporal_correlations.keys():
        Lt = temporal_correlations[cat].L
        Lh = horizontal_correlations[cat].L
        mask = ((coordinates.category == cat.name) & (coordinates.tracer == cat.tracer)).values
        w = xc[mask].reshape(Lt.shape[0], Lh.shape[0])
        x = append(x, sigmas[cat] * (Lt @ w @ Lh.transpose()).reshape(-1))
    return x


@debug.timer
def g_to_gc(g: NDArray, temporal_correlations: Dict, horizontal_correlations: Dict, sigmas: DataFrame,
            coordinates: DataFrame) -> NDArray:
    gc = array(())
    for cat in temporal_correlations.keys():
        Lt = temporal_correlations[cat].L
        Lh = horizontal_correlations[cat].L
        mask = ((coordinates.category == cat.name) & (coordinates.tracer == cat.tracer)).values
        q = (sigmas[cat] * g[mask]).reshape(Lt.shape[0], Lh.shape[0])
        gc = append(gc, (Lt.transpose() @ q @ Lh).reshape(-1))
    return gc


@debug.timer
def xc_to_x_multicat(
    xc: NDArray, 
    temporal_correlations: Dict[str, NDArray], 
    horizontal_correlations: Dict[str, NDArray], 
    category_correlations: Dict[str, NDArray], 
    sigmas: Dict[str, NDArray], 
    coordinates: DataFrame) -> NDArray:
    """
    Preconditioning with cross-category correlations. This requires the correlated categories to have same uncertainty specifications (correlation lengths, spatial and temporal resolution), so instead of iterating on categories, we iterate on "optimization groups":
    - The "temporal_correlations", "horizontal_correlations" and "sigmas" variables must have one item for each group instead of for each category.
    - An additional "category_correlations" variable is required, which contains the correlation matrix between the different categories.
    
    Within a correlation group, the correlation matrix is constructed as :
    C = Lc ⊗ Lt ⊗ Lh
    
    The preconditioning is done as:
    w = ((Lc ⊗ Lt) ⋅ χ ⋅ Lh.T).reshape(-1)
    
    with Lh.T the transport of Lh, and χ the control vector x reshaped as a (nt * nc, nh) matrix
    
    """
    x = array([])
    for grp in temporal_correlations.keys():
        Lt = temporal_correlations[grp]
        Lh = horizontal_correlations[grp]
        Lc = category_correlations[grp]
        mask = (coordinates.group == grp.name).values
        nt, nc, nh = Lt.shape[0], Lc.shape[0], Lh.shape[0]
        w = xc[mask].reshape(nt * nc, nh)
        x = append(x, sigmas[grp] * (kron(Lc, Lt) @ w @ Lh.T).reshape(-1))
    return x


@debug.timer
def g_to_gc_multicat(
    g : NDArray,
    temporal_correlations : Dict[str, NDArray],
    horizontal_correlations : Dict[str, NDArray],
    category_correlations : Dict[str, NDArray],
    sigmas : Dict[str, NDArray],
    coordinates : DataFrame
) -> NDArray:
    raise NotImplemented