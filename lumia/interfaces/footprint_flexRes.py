#!/usr/bin/env python
import os, pickle
import logging
from datetime import datetime
from copy import deepcopy
from numpy import zeros, meshgrid, average, flatnonzero, float64, array, size, nan, unique, frombuffer
from pandas import DataFrame
from lumia.Tools import Region, Categories
from lumia.Tools.optimization_tools import clusterize
from lumia.Tools.time_tools import tinterv
from lumia import tqdm, timer
from multiprocessing import Pool

logger = logging.getLogger(__name__)

obsoperator = 'lagrange'
invcontrol = 'flexRes'

data = {}


def stv_worker(itopt):
    statevec, categ, ipos, itime = [], [], [], []
    time_indices = flatnonzero(data['temporal_mapping'][itopt,:])
    for cluster in data['clusters']:
        statevec.append(data['emis'][time_indices, :, :][:,cluster.ilats, cluster.ilons].sum())
        categ.append(data['cat'])
        ipos.append(cluster.ind)
        itime.append(itopt)
    return (statevec, categ, ipos, itime)


class Interface :

    def __init__(self, rcf, ancilliary=None):
        self.rcf = rcf
        self.categories = Categories(rcf)
        self.region = Region(rcf)
        self.ancilliary_data = ancilliary

    def StructToVec(self, struct, lsm_from_file=False):
        lsm = self.region.get_land_mask(refine_factor=2, from_file=lsm_from_file)
        self.temporal_mapping = self.calc_temporal_coarsening(struct)
        self.spatial_mapping = self.calc_spatial_coarsening(lsm=lsm)

        vec = DataFrame(columns=['category', 'value', 'iloc', 'time'])
        
        statevec, categ, ipos, itime = [], [], [], []
        timer.info()
        for cat in [x for x in self.categories if x.optimize]:

            nt = len(self.temporal_mapping[cat.name]['times_optim'])
            data['emis'] = struct[cat.name]['emis']
            data['cat'] = cat.name
            data['clusters'] = self.spatial_mapping['cluster_specs']
            data['temporal_mapping'] = self.temporal_mapping[cat.name]['map']
            with Pool() as pp :
                res = pp.map(stv_worker, tqdm(range(nt)))

            for rr in tqdm(res) :
                statevec.extend(rr[0])
                categ.extend(rr[1])
                ipos.extend(rr[2])
                itime.extend(rr[3])

        timer.info()
        vec.loc[:, 'category'] = array(categ, dtype=str)
        vec.loc[:, 'value'] = array(statevec, dtype=float64)
        vec.loc[:, 'iloc'] = array(ipos, dtype=int)
        vec.loc[:, 'itime'] = array(itime, dtype=int)
        for ipos in unique(vec.loc[:, 'iloc']):
            vec.loc[vec.loc[:, 'iloc'] == ipos, 'lat'] = self.spatial_mapping['cluster_specs'][ipos].mean_lat
            vec.loc[vec.loc[:, 'iloc'] == ipos, 'lon'] = self.spatial_mapping['cluster_specs'][ipos].mean_lon
            vec.loc[vec.loc[:, 'iloc'] == ipos, 'land_fraction'] = self.spatial_mapping['cluster_specs'][ipos].land_fraction

        for itopt, topt in enumerate(self.temporal_mapping[cat.name]['times_optim']):
            vec.loc[vec.itime == itopt, 'time'] = topt
        self.ancilliary_data['vec2struct'] = vec.loc[:, ['category', 'iloc', 'itime']]
        return vec

    def VecToStruct(self, vector):
        struct = {}
        for cat in self.categories:
            struct[cat.name] = deepcopy(self.ancilliary_data[cat.name])
            if cat.optimize :
                struct[cat.name]['emis'][:] = 0.
        for var in self.ancilliary_data['vec2struct'].itertuples(): 
        #for var in vector.itertuples():
            tind = flatnonzero(self.temporal_mapping[var.category]['map'][var.itime,:])
            cl = self.spatial_mapping['cluster_specs'][var.iloc]
            f0 = self.ancilliary_data[var.category]['emis'][tind, :, :][:,cl.ilats, cl.ilons]
            nv = size(f0)
            struct[var.category]['emis']
            for ipt in range(cl.size) :
                struct[var.category]['emis'][tind, cl.ilats[ipt], cl.ilons[ipt]] = f0[:, ipt] + (vector.loc[var.Index] - f0.sum()) / nv
        return struct

    def VecToStruct_adj(self, adjstruct):
        adjvec = []
        for var in self.ancilliary_data['vec2struct'].itertuples():
            tind = flatnonzero(self.temporal_mapping[var.category]['map'][var.itime,:])
            cl = self.spatial_mapping['cluster_specs'][var.iloc]
            adj = adjstruct[var.category]['emis'][tind, :, :][:, cl.ilats, cl.ilons]
            nv = size(adj)
            adjvec.append(adj.sum()/nv)
#            itmod = flatnonzero(self.temporal_mapping[var.category][var.itime,:])
#        for cat in [x for x in self.categories if x.optimize]:
#            for itopt in range(len(self.temporal_mapping[cat.name]['times_optim'])):
#                for cluster in self.spatial_mapping['cluster_specs']:
#                    itmod = flatnonzero(self.temporal_mapping[cat.name][itopt,:])
#                    adjf = adjstruct[cat.name]['emis'][itmod, cluster.ilats, cluster.ilons]
#                    nv = size(adjf)
#                    adjvec.append(adjf.sum()/nv)
        return adjvec

    def calc_spatial_coarsening(self, lsm=None):
        clusters = clusterize(
            self.ancilliary_data['sensi_map'],
            self.rcf.get('optimize.ngridpoints'),
            mask = lsm
        )
        mapping = {
            'clusters_map': zeros((self.region.nlat, self.region.nlon))+nan,
            'cluster_specs': []
        }
        lons, lats = meshgrid(self.region.lons, self.region.lats)
        ilons, ilats = meshgrid(range(self.region.nlon), range(self.region.nlat))
        lats, lons, ilats, ilons = lats.reshape(-1), lons.reshape(-1), ilats.reshape(-1), ilons.reshape(-1)
        area = self.region.area.reshape(-1)
        lsm = lsm.reshape(-1)
        for icl, cl in enumerate(tqdm(clusters)) :
            #indices = cl.ind.reshape(-1)
            indices = cl.ind[cl.mask]
            mapping['clusters_map'].reshape(-1)[indices] = icl
            cl.ind = icl
            cl.lats = lats[indices]
            cl.lons = lons[indices]
            cl.ilats = ilats[indices]
            cl.ilons = ilons[indices]
            cl.area = area[indices]
            cl.mean_lat = average(cl.lats, weights=cl.area)
            cl.mean_lon = average(cl.lons, weights=cl.area)
            cl.area_tot = cl.area.sum()
            cl.land_fraction = average(lsm[indices], weights=cl.area)
            cl.size = len(indices)
            mapping['cluster_specs'].append(cl)
        return mapping

    def calc_temporal_coarsening(self, struct):
        mapping = {}
        for cat in [x for x in self.categories if x.optimize]:
            # Model times
            times_s_model = struct[cat.name]['time_interval']['time_start']
            times_e_model = struct[cat.name]['time_interval']['time_end']
            times_model = [tinterv(s, e) for (s, e) in zip(times_s_model, times_e_model)]

            # Read optimization time settings
            # factor = 1
            # unit = {
            #     'y':relativedelta(years=1),
            #     'm':relativedelta(months=1),
            #     'd':relativedelta(days=1),
            #     'h':relativedelta(hours=1),
            # }[cat.optimization_interval]

            # Find initial time 
            t0 = datetime(times_model[0].start.year, 1, 1)
            #dt = unit*factor
            dt = cat.optimization_interval
            while t0 < times_s_model[0]:
                t0 += dt
            
            # Optimization times
            times_optim = []
            while t0 < times_s_model[-1]:
                times_optim.append(tinterv(t0, t0+dt))
                t0 += dt

            # Mapping:
            nt_optim = len(times_optim)
            nt_model = len(times_model)
            mapping[cat.name] = {'map':zeros((nt_optim, nt_model)), 'times_model':times_model, 'times_optim':times_optim}
            for imod, tmod in enumerate(times_model):
                for iopt, topt in enumerate(times_optim):
                    mapping[cat.name]['map'][iopt, imod] = tmod.overlap_percent(topt)

        return mapping            