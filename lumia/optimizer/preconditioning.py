#!/usr/bin/env python

from numpy.typing import NDArray
from typing import Dict
from numpy import array, append
from pandas import DataFrame


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
