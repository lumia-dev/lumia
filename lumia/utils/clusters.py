#!/usr/bin/env python

from numpy import arange, ones_like, cumsum, array
from numpy.typing import NDArray
from typing import Tuple, List
import sys
from tqdm.autonotebook import tqdm


class Cluster:
    minxsize : int = 1
    minysize : int = 1

    def __init__(self, data: NDArray, indices: NDArray | None = None, mask: NDArray | None = None, dy: int | None = None, crop: bool = True):
        self.data = data
        self.dy = self.nx if dy is None else dy
        if indices is None :
            self.ind = arange(self.size).reshape((self.ny, self.nx))
        else :
            self.ind = indices
        if mask is None :
            mask = ones_like(data, dtype=bool)
        self.mask = mask
        if crop:
            self.crop()

    @property
    def shape(self) -> Tuple:
        return self.data.shape

    @property
    def ny(self) -> int:
        return self.data.shape[0]

    @property
    def nx(self) -> int:
        return self.data.shape[1]

    @property
    def size(self) -> int:
        return self.nx * self.ny

    @property
    def rank(self) -> float:
        if self.size == 1 :
            return -1
        return abs(self.data.sum())

    def transpose(self) -> None:
        self.data = self.data.transpose()
        self.ind = self.ind.transpose()
        self.mask = self.mask.transpose()

    def splity(self) -> Tuple["Cluster", "Cluster"]:
        npt = self.data.shape[0]
        isplit = int(npt/2)
        if npt % 2 == 1 :
            d1 = abs(self.data[:isplit, :].sum()-self.data[isplit:, :].sum())
            d2 = abs(self.data[:isplit + 1, :].sum()-self.data[isplit + 1:, :].sum())
            if d2 < d1 :
                isplit = isplit+1
        c1 = Cluster(self.data[:isplit, :], indices=self.ind[:isplit, :], mask=self.mask[:isplit, :], dy=self.dy)
        c2 = Cluster(self.data[isplit:, :], indices=self.ind[isplit:, :], mask=self.mask[isplit:, :], dy=self.dy)
        return c1, c2

    def splitx(self) -> Tuple["Cluster", "Cluster"]:
        self.transpose()
        c1, c2 = self.splity()
        c1.transpose()
        c2.transpose()
        return c1, c2

    def split(self) -> Tuple["Cluster", "Cluster"]:
        if self.ny > self.nx or self.nx == self.minxsize :
            c1, c2 = self.splity()
        elif self.nx > self.ny or self.ny == self.minysize :
            c1, c2 = self.splitx()
        else :
            c11, c21 = self.splity()
            d1 = abs(c11.data.sum()-c21.data.sum())
            c12, c22 = self.splitx()
            d2 = abs(c12.data.sum()-c22.data.sum())
            if d1 > d2 :
                c1, c2 = c12, c22
            else :
                c1, c2 = c11, c21
        return c1, c2

    def crop(self) -> "Cluster":
        """
        This crops the edge row/columns if they are completely masked
        """
        mask_r = self.mask.sum(0) > 0
        mask_c = self.mask.sum(1) > 0
        mask_r = cumsum(mask_r, dtype=bool)*cumsum(mask_r[::-1], dtype=bool)[::-1]
        mask_c = cumsum(mask_c, dtype=bool)*cumsum(mask_c[::-1], dtype=bool)[::-1]
        return Cluster(
            self.data[mask_c, :][:, mask_r],
            indices=self.ind[mask_c, :][:, mask_r],
            mask=self.mask[mask_c, :][:, mask_r],
            dy=self.dy,
            crop=False
        )

    def _find_neighbours(self, ind: int) -> List[int]:
        neighbours = [ind - 1, ind + 1, ind - self.dy, ind + self.dy]
        return [n for n in neighbours if n in self.ind[self.mask > 0]]

    def _walk(self, n0: int, neighbours : List[int] = None) -> List[int]:
        if not neighbours:
            neighbours = [n0]
        new_neigbours = self._find_neighbours(n0)
        new_neigbours = [n for n in new_neigbours if n not in neighbours]
        neighbours.extend(new_neigbours)
        for nb in new_neigbours:
            neighbours = self._walk(nb, neighbours)
        return neighbours

    def splitByMask(self) -> List["Cluster"]:
        indices = self.ind[self.mask > 0]
        new_clusters = []
        while len(indices) > 0 :
            cl2 = self._walk(indices[0])
            newmask = array([c in cl2 for c in self.ind.reshape(-1)]).reshape(self.shape)
            newcl = Cluster(
                self.data,
                self.ind,
                mask=newmask,
                dy=self.dy
            )
            new_clusters.append(newcl)
            indices = [ii for ii in indices if ii not in cl2]
        return new_clusters


def clusterize(field, nmax: int, mask: NDArray = None, minxsize: int = 1, minysize: int = 1, cat: str = '', indices: NDArray = None):

    # Set general properties for all clusters:
    Cluster.minxsize = minxsize
    Cluster.minysize = minysize

    # Create one big cluster with everything in it
    clusters = [Cluster(field, mask=mask, crop=False, indices=indices)]

    # Increate the recursion limit temporarily
    rlim = sys.getrecursionlimit()
    sys.setrecursionlimit(clusters[0].size+1)

    # Container list to offload the clusters that cannot be further divided to speed up the calculations
    clusters_final = []

    # if coarsen is not None :
    #     clusters = clusters[0].reduce_res(coarsen)

    # Max number of clusters is the lowest of nmax and the number of non-masked pixels
    nclmax = min(nmax, (clusters[0].mask > 0).sum())

    # Loop until nclmax is reached:
    with tqdm(total=nclmax, desc=f"spatial aggregation {cat}") as pbar:
        ncl = len(clusters + clusters_final)
        while ncl < nclmax :  # and len(Cluster) > 0 :

            # Find the highest-ranked cluster (the one that will be split)
            ranks = [c.rank for c in clusters]
            ind = ranks.index(max(ranks))

            # Split it and remove it from the list of clusters
            new_clusters = clusters[ind].split()
            clusters.pop(ind)

            # Determine what to do with the (two) new clusters:
            for cl in new_clusters :

                # Not too sure what happens with the mask here ...
                if cl.mask.any() :

                    # If the cluster has reached its minimum size, store it in clusters_final
                    if cl.nx <= cl.minxsize and cl.ny <= cl.minysize :
                        clusters_final.append(cl)

                    # Otherwise, check if further mask-splitting is required (i.e. ensure that the pixels in the cluster are still contiguous).
                    # Whatever the result, append them to the working list of clusters.
                    else :
                        if mask is not None :
                            clusters.extend(cl.splitByMask())
                        else :
                            clusters.append(cl)

            # Update the progress-bar
            inc = len(clusters + clusters_final) - ncl
            pbar.update(inc)

            ncl += inc

    # Restore the original recursion limit
    sys.setrecursionlimit(rlim)

    # Return the list of remaining clusters + the ones that have reached the min size
    return clusters + clusters_final
