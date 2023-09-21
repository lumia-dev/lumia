#!/usr/bin/env python

from json.encoder import py_encode_basestring_ascii
import os
import shutil
from datetime import datetime
from numpy import array_equal, nan, array, unique, nonzero, argsort, append, random, dot, finfo, zeros
from multiprocessing import Pool
from tqdm import tqdm
from netCDF4 import Dataset
from lumia.Tools import rctools, Tracers
from lumia import obsdb
from lumia.formatters.structure import Emissions, ReadStruct, WriteStruct, Struct, CreateStruct
from lumia.Tools import regions
import logging
import pickle
import pdb
from h5py import File
import tempfile

# Cleanup needed in the following ...
from lumia.Tools import Region, costFunction
from lumia.Tools.time_tools import time_interval

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


common = {}


def Observations(arg):
    """
    For now, this is just a wrapper around the lumia obsdb class, but since I don't
    like that class, I might change it. 
    """
    if isinstance(arg, obsdb):
        return arg
    elif arg is None :
        return obsdb()
    else :
        return obsdb(arg)

class Flux:
    """
    This class slighly adapts the "Struct" class from the lumia.formatters.lagrange module: it ensures that
    all the fields are on the exact same grid (including temporally), and stores some variables related to that
    grid, to make the conformity check with the footprints easier and allow adjusting the time indices, which
    don't have the same origin.
    TODO: this should be directly part of the "Struct" class itself ==> yes, but after the class is adapted for
          standard netCDF format
    """
    def __init__(self, fluxfile, tracers=None):
        if isinstance(fluxfile, Struct) :
            self.data = fluxfile
        else :
            self.data = ReadStruct(fluxfile, tracers=tracers)

        self.tracers = {}

        for tr in list(self.data.keys()):
            self.tracers[tr] = {}
            self.tracers[tr] = list(self.data[tr].keys())

        #TODO: when reading adjoint.nc check that it has data in it. Check before if there are not opt categories.

        # Make sure that all categories have the same coordinates (this is a requirement):
        for tr in self.tracers.keys():
            for cat in self.tracers[tr]:
                cat0 = self.tracers[tr][0]
                assert array_equal(self.data[tr][cat]['time_interval']['time_start'], self.data[tr][cat0]['time_interval']['time_start'])
                assert array_equal(self.data[tr][cat]['time_interval']['time_end'], self.data[tr][cat0]['time_interval']['time_end'])
                try :
                    assert array_equal(self.data[tr][cat]['lats'], self.data[tr][cat0]['lats'])
                    assert array_equal(self.data[tr][cat]['lons'], self.data[tr][cat0]['lons'])
                except AssertionError :
                    print(tr, cat)
                    print(self.data[tr][cat]['lats'])
                    print(self.data[tr][cat]['lons'])
                    print(self.data[tr][cat0]['lats'])
                    print(self.data[tr][cat0]['lons'])

        tstart = self.data[tr][cat0]['time_interval']['time_start']
        tend = self.data[tr][cat0]['time_interval']['time_end']
        self.tres = tstart[1]-tstart[0]
        self.nt = self.data[tr][cat0]['emis'].shape[0]

        # Retrieve the dimensions
        # TODO: for now, we assume that lat and lon conform with those of the footprints. Ideally, we'd need a check on this

        self.start = tstart[0]
        self.end = tend[-1]
        self.times_start = tstart
        self.times_end = tend
        self.nlat = self.data[tr][cat0]['emis'].shape[1]
        self.nlon = self.data[tr][cat0]['emis'].shape[0]
        self.region = regions.region(longitudes=self.data[tr][cat0]['lons'], latitudes=self.data[tr][cat0]['lats'])
        self.coordinates = SpatialCoordinates(obj=self.region)
        self.origin = self.start

    def write(self, outfile):
        WriteStruct(self.data, outfile)

    def between_times(self, start, end):
        Flux(self.data.between_times(start, end))

    def asvec(self):
        """
        Returns a vector representation of the data (primarily for adjoint test purpose)
        """
        vec = array(())
        for tr in self.tracers.keys():
            for cat in self.tracers[tr] :
                logger.debug(cat)
                vec = append(vec, self.data[tr][cat]['emis'].reshape(-1))
        return vec


class Delta:
    def __init__(self, deltafile, atmos_del=True):
        self.data = ReadStruct(deltafile, atmos_del)

    def write(self, outfile, atmos_del=True):
        WriteStruct(self.data, outfile, atmos_del=True)


class FootprintFile:
    def __init__(self, filename, silent=False, cache=False):
        filename = self.cache(filename, cache)
        self.filename = filename
        self.silent = silent
        self._initialized = False
        self.Footprint = Footprint

    def cache(self, filename, cache):
        """
        Copy the footprint file to a cache.
        This is meant to be used on clusters that have a local hard drive with fast I/O.
        Since different files may be processed on different nodes (and therefore may use different
        physical caches), the caching is done by the "FootprintFile" class itself.
        """
        if cache :
            source, fname = os.path.split(filename)
            dest = os.path.join(cache, fname)
            if not os.path.exists(dest):
                shutil.copy(filename, dest)
        else :
            return filename

    def run(self, obslist, emis, step, atmdel=None): 
        self.read()  # Read basic information from the file itself
        self.setup(emis.coordinates, emis.origin, emis.tres)
        # 1st, check which obsids are present in the file:
        footprint_valid = [o in self.footprints for o in obslist.obsid]
        #logger.debug(f"{sum(footprint_valid)} valid footprints of {obslist.shape[0]} in file {self.filename}")
        obslist = obslist.loc[footprint_valid].copy()
        if step == 'forward' :
            return self._runForward(obslist, emis, atmdel)
        elif step == 'adjoint' :
            return self._runAdjoint(obslist, emis, atmdel)

    def _runForward(self, obslist, emis, atmdel=None):
        """
        Compute the (foreground) concentration for a specific obs (given by its obsid) and a surface flux, provided as argument 
        """
        for iobs, obs in tqdm(obslist.iterrows(), desc=self.filename, total=obslist.shape[0], disable=self.silent):
            fpt = self.getFootprint(obs.obsid, origin=emis.origin)
            
            for cat in emis.data[obs.tracer].keys():
                # if cat == 'nuclear':
                #     pass
                # else:
                obslist.loc[iobs, f'{obs.tracer}_{cat}'] = fpt.to_conc(emis.data[obs.tracer][cat]['emis'])

            if obs.tracer == 'c14':
                #TODO: include co2 categories for unit conversion. Done
                obslist.loc[iobs, 'co2_ocean'] = fpt.to_conc(emis.data['co2']['ocean']['emis'])
                obslist.loc[iobs, 'co2_biosphere'] = fpt.to_conc(emis.data['co2']['biosphere']['emis'])
                obslist.loc[iobs, 'co2_fossil'] = fpt.to_conc(emis.data['co2']['fossil']['emis'])
                obslist.loc[iobs, f'{obs.tracer}_ocean'] = fpt.to_conc(emis.data['co2']['ocean']['emis'] * atmdel.data['Del_14C']['obs'][:, None, None])
                obslist.loc[iobs, f'{obs.tracer}_biosphere'] = fpt.to_conc(emis.data['co2']['biosphere']['emis'] * atmdel.data['Del_14C']['obs'][:, None, None])
                obslist.loc[iobs, f'{obs.tracer}_fossil'] = fpt.to_conc(emis.data['co2']['fossil']['emis'] * -1) #TODO: call fossil.D14C key from rc file
                # obslist.loc[iobs, f'{obs.tracer}_nuclear'] = fpt.to_conc(emis.data['c14']['nuclear']['emis'] * (975/(-8+1000))**2) 

        self.close()
        return obslist

    def _runAdjoint(self, obslist, adjstruct, atmdel=None):

        for iobs, obs in tqdm(obslist.iterrows(), desc=self.filename, total=obslist.shape[0], disable=self.silent):
            fpt = self.getFootprint(obs.obsid, origin=adjstruct.origin)
            if obs.tracer == 'c14':
                t = (atmdel.data['Del_14C']['time_interval']['time_start']<=obs.time) & (atmdel.data['Del_14C']['time_interval']['time_start']<obs.time)
                atmos_del = atmdel.data['Del_14C']['obs'][t].mean()
                if 'ocean' in adjstruct.data['co2'].keys():
                    adjstruct.data['co2']['ocean']['emis'] = fpt.to_adj(obs.dy * atmos_del, adjstruct.data['co2']['ocean']['emis'])
                if 'biosphere' in adjstruct.data['co2'].keys():
                    adjstruct.data['co2']['biosphere']['emis'] = fpt.to_adj(obs.dy * atmos_del, adjstruct.data['co2']['biosphere']['emis'])
                if 'fossil' in adjstruct.data['co2'].keys():
                    adjstruct.data['co2']['fossil']['emis'] = fpt.to_adj(obs.dy * -1, adjstruct.data['co2']['fossil']['emis']) #TODO: call fossil.D14C key from rc file
                
            if obs.tracer in adjstruct.data.keys():                    
                for cat in adjstruct.data[obs.tracer].keys():
                    adjstruct.data[obs.tracer][cat]['emis'] = fpt.to_adj(obs.dy, adjstruct.data[obs.tracer][cat]['emis'])#.copy())
                    
        self.close()
        return adjstruct


class SpatialCoordinates:
    def __init__(self, **kwargs):
        attrs = ['lons', 'lon0', 'dlon', 'nlon', 'lats', 'lat0', 'dlat', 'nlat']
        for attr in attrs :
            setattr(self, attr, kwargs.get(attr, None))

        # The following enables constructing a set of spatial coordinates from
        # an object that would already have some of the required attributes
        obj = kwargs.get('obj', False)
        if obj :
            for attr in attrs :
                if hasattr(obj, attr):
                    setattr(self, attr, getattr(obj, attr))

        self._autocomplete()

    def _autocomplete(self):
        """
        Tries to calculate missing values based on the ones that are present
        For now, dlon and dlat are deduced from lons and lats
        """
        # Fix lons
        if self.lons is None and None not in [self.lon0, self.dlon, self.nlon]:
            self.lons = [self.lon0 + i*self.dlon for i in range(self.nlon)]
        # Else, fix dlon/nlon
        elif self.lons is not None :
            if self.dlon is None :
                self.dlon = self.lons[1]-self.lons[0]
            if self.nlon is None :
                self.nlon = len(self.lons)

        # same for lats:
        if self.lats is None and None not in [self.lat0, self.dlat, self.nlon]:
            self.lats = [self.lat0 + i*self.dlat for i in range(self.nlat)]
        elif self.lats is not None :
            if self.dlat is None :
                self.dlat = self.lats[1]-self.lats[0]
            if self.nlat is None :
                self.nlat = len(self.lats)

    def __le__(self, other):
        res = all([l in other.lons for l in self.lons])
        res *= all([l in other.lats for l in self.lats])
        res *= (other.dlon == self.dlon) * (other.dlat == self.dlat)
        return res

    def __eq__(self, other):
        return array_equal(self.lats, other.lats) * array_equal(self.lons, other.lons)


class Footprint:
    # Allows setting some default class attributes (normally all footprints in a 
    # footprint file share these ...)
    coords = None 
    origin = None
    dt = None

    def __init__(self, origin=None):
        if origin is not None :
            self.origin = origin
        self.itims = []
        self.ilats = []
        self.ilons = []
        self.sensi = []

    def shift_origin(self, origin):
        shift_t = (self.origin-origin)/self.dt
        if shift_t - int(shift_t) != 0:
            pdb.set_trace()
        assert shift_t - int(shift_t) == 0
        self.itims += int(shift_t)
        self.origin = origin

    def to_conc(self, flux):
        # Here we assume that flux has the exact same shape and coordinates as the footprints
        # TODO: make this a bit more explicit ...
        s = (self.itims >= 0) & (self.itims < flux.shape[0])
        if s.shape[0] == 0 :
            return nan
        if self.itims.min() < 0 :
            return nan
        return (flux[self.itims[s], self.ilats[s], self.ilons[s]]*self.sensi[s]).sum()

    def to_adj(self, dy, adjfield):
        # TODO: this (and the forward) accept only one category ... maybe here is the place to implement more?
        s = (self.itims >= 0) & (self.itims < adjfield.shape[0])
        if s.shape[0] == 0 :
            return adjfield
        if self.itims.min() < 0 :
            return adjfield
        adjfield[self.itims[s], self.ilats[s], self.ilons[s]] += self.sensi[s]*dy
        return adjfield

    def itime_to_times(self, it):
        return self.origin+it*self.dt


def loop_forward(filename, silent=True):
    """
    """
    obslist = common['obslist'].loc[common['obslist'].footprint == filename, ['obsid', 'tracer']]#.copy()
    fpf = common['fpclass'](filename, silent=silent)
    res = fpf.run(obslist, common['emis'], 'forward', common['atmdel'])
    return res


def loop_adjoint(filename):
    obslist = common['obslist'].loc[common['obslist'].footprint == filename, ['obsid', 'dy', 'time', 'site', 'tracer']]
    adj = Flux(CreateStruct(common['tracers'], common['region'], common['start'], common['end'], common['tres']))
    fpf = common['fpclass'](filename, silent=True)
    res = fpf.run(obslist, adj, 'adjoint', common['atmdel'])

    with tempfile.NamedTemporaryFile(dir=common['tmpdir'], prefix='adjoint_', suffix='.h5') as fid :
        fname_out = fid.name
    # fname_out = os.path.join(common['tmpdir'], f'adj_{os.path.basename(fpf.filename)}.h5')
    with File(fname_out, 'w') as fid :
        for tr in res.data.keys():
            trgrp = fid.create_group(tr)
            for cat in res.data[tr].keys():
                adj_emis = res.data[tr][cat]['emis'].reshape(-1)
                nz = nonzero(adj_emis)[0]
                catgrp = trgrp.create_group(cat)
                catgrp['coords'] = nz
                catgrp['values'] = adj_emis[nz]
                # logger.critical(f'{filename} {tr} {cat}')
                # nzi = nonzero(res.data[tr][cat]['emis'].reshape(-1)[0])
                # nzv = res.data[tr][cat]['emis'].reshape(-1)[0][nzi]
                # cat = trgrp.create_group(cat)
                # cat['nzi'] = nzi
                # cat['nzv'] = nzv
                # logger.critical(f'{filename} {tr} {cat}')


    # fname_out = os.path.join(common['tmpdir'], f'adj_{os.path.basename(fpf.filename)}.pickle')
    # with open(fname_out, 'wb') as fid :
    #     compact = {}
    #     for tr in res.data.keys():
    #         compact[tr] = {}
    #         for cat in res.data[tr].keys():
    #             nzi = nonzero(res.data[tr][cat]['emis'])
    #             nzv = res.data[tr][cat]['emis'][nzi]
    #             compact[tr][cat] = (nzi, nzv)
    #     pickle.dump(compact, fid)
    return str(fname_out)


class FootprintTransport:
    def __init__(self, rcf, obs, emfile=None, atmdelfile=None, FootprintFileClass=FootprintFile, mp=False, checkfile=None, ncpus=None):
        self.rcf = rctools.rc(rcf)
        self.obs = Observations(obs)
        self.obs.checkIndex(reindex=True)
        self.emfile = emfile

        self.atmdelfile = atmdelfile
        self.fossil_del = self.rcf.get('fossil.D14C', totype=int, default=-1)

        #TODO: include Del_14C here

        # Just in case ...
        self.obsfile = obs
        self.rcfile = rcf

        # Internals
        self.executable = __file__
        self.ncpus = self.rcf.get('model.transport.split', default=os.cpu_count())
        self._set_parallelization(False)
        if mp :
            self._set_parallelization('ray')
        self.FootprintFileClass = FootprintFileClass

        if emfile is not None :
            self.tracers = Tracers(self.rcf)
        else :
            logger.warn("No emission files has been provided. Ignore this warning if it's on purpose")
        self.checkfile=checkfile

    def _set_parallelization(self, mode=False):
        if not mode :
            self._forward_loop = self._forward_loop_serial
            self._adjoint_loop = self._adjoint_loop_serial
        elif mode == 'ray' :
            self._forward_loop = self._forward_loop_mp
            self._adjoint_loop = self._adjoint_loop_mp

    def runForward(self, tracers=None):
        # Read the emissions:
        self.emis = Flux(self.emfile, tracers=tracers)
        if self.atmdelfile is not None:
            self.atmos_del = Delta(self.atmdelfile)
        else:
            self.atmos_del = None

        # Add the flux columns to the observations dataframe:
        for tr in self.emis.tracers.keys():
            for cat in self.emis.tracers[tr]:
                self.obs.observations.loc[:, f'mix_{tr}_{cat}'] = nan
            if tr == 'c14':
                self.obs.observations.loc[:, f'mix_{tr}_ocean'] = nan
                self.obs.observations.loc[:, f'mix_{tr}_biosphere'] = nan
                self.obs.observations.loc[:, f'mix_{tr}_fossil'] = nan

        filenames = self.obs.observations.footprint.dropna().drop_duplicates() 

        self._forward_loop(filenames)

        # Combine the flux components:
        try :
            self.obs.observations.loc[:, 'mix'] = self.obs.observations.mix_background.copy()
        except AttributeError :
            logger.warning("Missing background concentrations. Assuming mix_background=0")
            self.obs.observations.loc[:, 'mix'] = 0.

        for tr in self.emis.tracers.keys():
            self.obs.observations.loc[self.obs.observations.tracer == tr, 'mix'] += self.obs.observations.loc[self.obs.observations.tracer == tr].filter(regex=f'mix_{tr}').sum(axis=1)

            # for cat in self.emis.tracers[tr]:
            #     self.obs.observations.loc[self.obs.observations.tracer == tr, 'mix'] += self.obs.observations.loc[self.obs.observations.tracer == tr, f'mix_{tr}_{cat}'].fillna(0)
            # if tr == 'c14':
            #     self.obs.observations.loc[self.obs.observations.tracer == tr, 'mix'] += self.obs.observations.loc[self.obs.observations.tracer == tr, f'mix_{tr}_ocean'].fillna(0)
            #     self.obs.observations.loc[self.obs.observations.tracer == tr, 'mix'] += self.obs.observations.loc[self.obs.observations.tracer == tr, f'mix_{tr}_biosphere'].fillna(0)
            #     self.obs.observations.loc[self.obs.observations.tracer == tr, 'mix'] += self.obs.observations.loc[self.obs.observations.tracer == tr, f'mix_{tr}_fossil'].fillna(0)

        # self.obs.observations.loc[self.obs.observations.tracer == 'tr'].filter(regex=f'mix_{tr}').sum(axis=1) + self.obs.observations.loc[self.obs.observations.tracer == tr].mix_background - self.obs.observations.loc[self.obs.observations.tracer == tr].mix

        self.obs.save_tar(self.obsfile)

    def runAdjoint(self):

        # 1) Create an empty adjoint structure
        region = Region(self.rcf)

        if self.atmdelfile is not None:
            self.atmos_del = Delta(self.atmdelfile)
        else:
            self.atmos_del = None

        obs_tracers = self.rcf.get('obs.tracers') if isinstance(self.rcf.get('obs.tracers'), list) else [self.rcf.get('obs.tracers')]
        tracers = {}

        for tr in obs_tracers:
            tracers[tr] = []
            for cat in self.rcf.get(f'emissions.{tr}.categories'):
                if self.rcf.get(f'emissions.{tr}.{cat}.optimize', totype=bool, default=False):
                    tracers[tr].append(cat)
            if tracers[tr] == []:
                del tracers[tr]

        start = datetime(*self.rcf.get('time.start'))
        end = datetime(*self.rcf.get('time.end'))
        dt = time_interval(self.rcf.get('emissions.interval'))

        adj = Flux(CreateStruct(tracers, region, start, end, dt))

        # 2) Loop over the footprint files
        filenames = self.obs.observations.footprint.dropna().drop_duplicates()

        adj = self._adjoint_loop(filenames, adj) #TODO: include atmos_del, fossil_del

        # 3) Write the updated adjoint field
        WriteStruct(data=adj.data, path=self.emfile)

        # return adj 

    def adjoint_test(self):
        # 1) Get the list of categories to be optimized:
        self.obs.observations.loc[:, 'mix_background'] = 0.
        self.obs.observations.loc[:, 'mix_c14_nuclear production'] = 0. # Just in case

        tracers = {}
        for tr in self.rcf.get('obs.tracers'):
            tracers[tr] = []
            for cat in self.rcf.get(f'emissions.{tr}.categories'):
                if self.rcf.get(f'emissions.{tr}.{cat}.optimize', totype=bool, default=False):
                    tracers[tr].append(cat)

        # categories = [c for c in self.rcf.get('emissions.categories') if self.rcf.get(f'emissions.{c}.optimize', totype=bool, default=False) is True]
        self.runForward(tracers=tracers)
        x1 = self.emis.asvec()
        # x1 = random.rand(len(x1))
        self.obs.observations.dropna(subset=['mix'], inplace=True)
        y1 = self.obs.observations.loc[:, 'mix'].values
        y2 = random.randn(y1.shape[0])
        self.obs.observations.loc[:, 'dy'] = y2
        adj = self.runAdjoint()
        x2 = adj.asvec()
        adjtest = 1 - dot(x1, x2)/dot(y1, y2)
        logger.info(f"Adjoint test: {1 - dot(x1, x2)/dot(y1, y2) = }")
        if abs(adjtest) < finfo('float32').eps :
            logger.info("Success")
        else :
            logger.warning(f"Adjoint test failed")
        logger.info(f"Assumed machine precision of: {finfo('float32').eps = }")

    def _adjoint_loop_serial(self, filenames, adj):
        for filename in tqdm(filenames):
            obslist = self.obs.observations.loc[self.obs.observations.footprint == filename, ['obsid', 'dy', 'time', 'site', 'tracer']].copy() 
            fpf = self.get(filename)
            adj = fpf.run(obslist, adj, 'adjoint', self.atmos_del)
        return adj

    def _adjoint_loop_mp(self, filenames, adj):

        t0 = datetime.now()
        nobs = array([self.obs.observations.loc[self.obs.observations.footprint == f].shape[0] for f in filenames])
        filenames = [filenames.values[i] for i in argsort(nobs)[::-1]]

        # I use the common because these are not iterable, but it would probably be cleaner to use apply_asinc and pass these as arguments to loop_adjoint 
        common['tracers'] = adj.tracers
        common['region'] = adj.region
        common['start'] = adj.start
        common['end'] = adj.end
        common['tres'] = adj.tres
        common['tmpdir'] = self.rcf.get('path.temp')
        common['obslist'] = self.obs.observations
        common['fpclass'] = self.FootprintFileClass
        common['atmdel'] = self.atmos_del

        with Pool(processes=self.ncpus) as pool :
            res = list(tqdm(pool.imap(loop_adjoint, filenames, chunksize=1), total=len(nobs)))

        for w in res:
            with File(w, 'r') as fid :
                for tr in fid.keys():
                    for cat in fid[tr].keys():
                        # nzi = fid[tr][cat]['nzi'][:]
                        # nzv = fid[tr][cat]['nzv'][:]
                        # adj.data[tr][cat]['emis'].reshape(-1)[nzi] += nzv
                        coords = fid[tr][cat]['coords'][:]
                        values = fid[tr][cat]['values'][:]
                        adj.data[tr][cat]['emis'].reshape(-1)[coords] += values
            # Delete the temporary file 
            os.remove(w)

        # for w in res:
        #     with open(w, 'rb') as fid :
        #         compact = pickle.load(fid)
        #         for tr in compact.keys():
        #             for cat in compact[tr].keys():
        #                 nzi, nzv = compact[tr][cat]
        #                 adj.data[tr][cat]['emis'][nzi] += nzv

        print(datetime.now()-t0)
        return adj

    def _forward_loop_serial(self, filenames):
        common['emis'] = self.emis
        common['atmdel'] = self.atmos_del
        common['obslist'] = self.obs.observations
        common['fpclass'] = self.FootprintFileClass
        for filename in tqdm(filenames):
            obslist = loop_forward(filename, silent=False)
            for iobs, obs in obslist.iterrows():
                for cat in self.emis.data[obs.tracer].keys():
                    self.obs.observations.loc[iobs, f'mix_{obs.tracer}_{cat}'] = obs.loc[f'{obs.tracer}_{cat}']#.value
                if obs.tracer == 'c14':
                    self.obs.observations.loc[iobs, 'mix_co2_ocean'] = obs.loc['co2_ocean']#.value
                    self.obs.observations.loc[iobs, 'mix_co2_biosphere'] = obs.loc['co2_biosphere']#.value
                    self.obs.observations.loc[iobs, 'mix_co2_fossil'] = obs.loc['co2_fossil']#.value
                    self.obs.observations.loc[iobs, f'mix_{obs.tracer}_ocean'] = obs.loc[f'{obs.tracer}_ocean']#.value
                    self.obs.observations.loc[iobs, f'mix_{obs.tracer}_biosphere'] = obs.loc[f'{obs.tracer}_biosphere']#.value
                    self.obs.observations.loc[iobs, f'mix_{obs.tracer}_fossil'] = obs.loc[f'{obs.tracer}_fossil']#.value
            
            # for tr in self.emis.data.keys():
            #     for cat in self.emis.data[tr].keys():
            #         self.obs.observations.loc[obslist.index, f'mix_{tr}_{cat}'] = obslist.loc[:, f'{tr}_{cat}']
            #     if tr == 'c14':
            #         self.obs.observations.loc[obslist.index, f'mix_{tr}_ocean'] = obslist.loc[:, f'{tr}_ocean']
            #         self.obs.observations.loc[obslist.index, f'mix_{tr}_biosphere'] = obslist.loc[:, f'{tr}_biosphere']
            #         self.obs.observations.loc[obslist.index, f'mix_{tr}_fossil'] = obslist.loc[:, f'{tr}_fossil']

    def _forward_loop_mp(self, filenames):
        t0 = datetime.now()
        nobs = array([self.obs.observations.loc[self.obs.observations.footprint == f].shape[0] for f in filenames])
        common['emis'] = self.emis
        common['atmdel'] = self.atmos_del
        common['obslist'] = self.obs.observations
        common['fpclass'] = self.FootprintFileClass

        # To optimize CPU usage, process the largest files first
        filenames = [filenames.values[i] for i in argsort(nobs)[::-1]]

        with Pool(processes=self.ncpus) as pool :
            res = list(tqdm(pool.imap(loop_forward, filenames, chunksize=1), total=len(nobs))) #TODO: ValueError: time data 'JUE.120m.20180301-110000' does not match format '%Y%m%d%H%M%S'

        for filename, obslist in zip(filenames, res):
            for iobs, obs in obslist.iterrows():
                for cat in self.emis.data[obs.tracer].keys():
                    self.obs.observations.loc[iobs, f'mix_{obs.tracer}_{cat}'] = obs.loc[f'{obs.tracer}_{cat}']
                if obs.tracer == 'c14':
                    self.obs.observations.loc[iobs, 'mix_co2_ocean'] = obs.loc['co2_ocean']#.value
                    self.obs.observations.loc[iobs, 'mix_co2_biosphere'] = obs.loc['co2_biosphere']#.value
                    self.obs.observations.loc[iobs, 'mix_co2_fossil'] = obs.loc['co2_fossil']#.value
                    self.obs.observations.loc[iobs, f'mix_{obs.tracer}_ocean'] = obs.loc[f'{obs.tracer}_ocean']
                    self.obs.observations.loc[iobs, f'mix_{obs.tracer}_biosphere'] = obs.loc[f'{obs.tracer}_biosphere']
                    self.obs.observations.loc[iobs, f'mix_{obs.tracer}_fossil'] = obs.loc[f'{obs.tracer}_fossil']
        print(datetime.now()-t0)

    def get(self, filename, silent=None):
        return self.FootprintFileClass(filename, silent)

    def writeFootprints(self, destpath, destclass=None, silent=False):
        """
        Write the footprints from the obs database to new footprint files, in the "destpath" directory.
        Optionally, an alternative footprint class can be provided, using the "destclass" argument, to write
        the new footprints in the format implemented by that class.

        Example uses:

        # 1) simply write the footprints to a new directory:
        f = LumiaFootprintTransport(rcf, obs, None)
        f.writeFootprints(self, outpath)

        # 2) Concert LUMIA footprints to STILT footprints:
        from stilt import StiltFootprintTransport
        f = LumiaFootprintTransport(rcf, obs, None)
        f.writeFootprints(self, outpath, StiltFootprintTransport)

        # 3) Convert FLEXPART footprint files (grid_time files) to the LUMIA format:
        from flexpart import FlexpartFootprintTransport
        f = FlexpartFootprintTransport(rcf, obs, None)
        f.writeFootprints(self, outpath, LumiaFootprintTransport)
        """
        if destclass is None :
            destclass = self.__class__
        self.obs.observations = self.obs.observations.dropna(subset=['footprint'])
        dest = destclass(os.path.join(self.rcf.dirname, self.rcf.filename), self.obs, self.emfile)

        fnames_out = array([os.path.join(destpath, f) for f in dest.genFileNames()])
        fnames_in = self.obs.observations.footprint.dropna().values
        for file0 in tqdm(unique(fnames_in)):
            fpf0 = self.get(file0)
            destfiles = fnames_out[fnames_in == file0]
            for file1 in tqdm(unique(destfiles), desc=file0, leave=False, disable=silent):
                fpf1 = dest.get(file1)
                obslist = self.obs.observations.loc[(fnames_out == file1) & (fnames_in == file0)]
                for obs in tqdm(obslist.itertuples(), total=obslist.shape[0], desc=file1, leave=False, disable=silent) :
                    fp = fpf0.getFootprint(obs.obsid)
                    fpf1.writeFootprint(obs, fp)
            del fpf0
