#!/usr/bin/env python

from numpy.typing import NDArray
from typing import Dict
from numpy import array, append, concatenate
from pandas import DataFrame
from lumia.utils import debug

def _is_sparse_corr(obj) -> bool:
    return getattr(obj, "is_sparse", False) is True

@debug.timer
def xc_to_x(xc: NDArray, temporal_correlations: Dict, horizontal_correlations: Dict,
            sigmas: Dict, coordinates: DataFrame) -> NDArray:
    x = array(())
    for cat in temporal_correlations.keys():
        Lt = temporal_correlations[cat].L             # dense temporal
        H = horizontal_correlations[cat]
        mask = ((coordinates.category == cat.name) & (coordinates.tracer == cat.tracer)).values

        if _is_sparse_corr(H):
            # reshape, apply H^{1/2} on the right via apply_Lt (note orientation)
            w = xc[mask].reshape(Lt.shape[0], H.B.shape[0])           # (nt, np)
            # x_cat = diag(sigmas) * vec( Lt @ w @ Lh^T )
            # apply sqrt(B_h) on columns (right): w @ Lh^T == (apply_Lt on rows)
            W1 = Lt @ w                                              # (nt, np)
            W2 = H.apply_Lt(W1.T).T                                  # (nt, np)
            x = append(x, (sigmas[cat] * W2.reshape(-1)))
        else:
            # dense path
            Lh = H.L
            w = xc[mask].reshape(Lt.shape[0], Lh.shape[0])
            x = append(x, sigmas[cat] * (Lt @ w @ Lh.T).reshape(-1))
    return x

@debug.timer
def g_to_gc(g: NDArray, temporal_correlations: Dict, horizontal_correlations: Dict,
            sigmas: Dict, coordinates: DataFrame) -> NDArray:
    gc = array(())
    for cat in temporal_correlations.keys():
        Lt = temporal_correlations[cat].L
        H  = horizontal_correlations[cat]
        mask = ((coordinates.category == cat.name) & (coordinates.tracer == cat.tracer)).values

        if _is_sparse_corr(H):
            # q = (sigmas * g)[mask] reshaped to (nt, np)
            q = (sigmas[cat] * g[mask]).reshape(Lt.shape[0], H.B.shape[0])  # (nt, np)
            # gc_cat = vec( Lt^T @ q @ Lh )
            Q1 = Lt.T @ q
            Q2 = H.apply_L(Q1.T).T
            gc = append(gc, Q2.reshape(-1))
        else:
            Lh = H.L
            q = (sigmas[cat] * g[mask]).reshape(Lt.shape[0], Lh.shape[0])
            gc = append(gc, (Lt.T @ q @ Lh).reshape(-1))
    return gc


# @debug.timer
# def xc_to_x(xc: NDArray, temporal_correlations: Dict, horizontal_correlations: Dict, sigmas: Dict,
#             coordinates: DataFrame) -> NDArray:
#     x = array(())
#     for cat in temporal_correlations.keys():
#         Lt = temporal_correlations[cat].L
#         Lh = horizontal_correlations[cat].L
#         mask = ((coordinates.category == cat.name) & (coordinates.tracer == cat.tracer)).values
#         w = xc[mask].reshape(Lt.shape[0], Lh.shape[0])
#         x = append(x, sigmas[cat] * (Lt @ w @ Lh.transpose()).reshape(-1))
#     return x


# @debug.timer
# def g_to_gc(g: NDArray, temporal_correlations: Dict, horizontal_correlations: Dict, sigmas: DataFrame,
#             coordinates: DataFrame) -> NDArray:
#     gc = array(())
#     for cat in temporal_correlations.keys():
#         Lt = temporal_correlations[cat].L
#         Lh = horizontal_correlations[cat].L
#         mask = ((coordinates.category == cat.name) & (coordinates.tracer == cat.tracer)).values
#         q = (sigmas[cat] * g[mask]).reshape(Lt.shape[0], Lh.shape[0])
#         gc = append(gc, (Lt.transpose() @ q @ Lh).reshape(-1))
#     return gc
