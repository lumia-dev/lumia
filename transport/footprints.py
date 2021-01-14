#!/usr/bin/env python

import os
import shutil
from datetime import datetime
from numpy import array_equal, nan, array, unique, nonzero, argsort
import ray
from multiprocessing import Pool
from tqdm import tqdm
from lumia.Tools import rctools, Categories
from lumia import obsdb
from lumia.formatters.lagrange import ReadStruct, WriteStruct, Struct, CreateStruct
from lumia.Tools import regions
import logging
import pickle

# Cleanup needed in the following ...
from lumia.Tools import Region
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
    def __init__(self, fluxfile):
        if isinstance(fluxfile, Struct) :
            self.data = fluxfile
        else :
            self.data = ReadStruct(fluxfile)
        self.categories = list(self.data.keys())

        # Make sure that all categories have the same coordinates (this is a requirement):
        cat0 = self.categories[0]
        for cat in self.categories :
            assert array_equal(self.data[cat]['time_interval']['time_start'], self.data[cat0]['time_interval']['time_start'])
            assert array_equal(self.data[cat]['time_interval']['time_end'], self.data[cat0]['time_interval']['time_end'])
            assert array_equal(self.data[cat]['lats'], self.data[cat0]['lats'])
            assert array_equal(self.data[cat]['lons'], self.data[cat0]['lons'])

        # Retrieve the dimensions
        # TODO: for now, we assume that lat and lon conform with those of the footprints. Ideally, we'd need a check on this
        tstart = self.data[cat0]['time_interval']['time_start']
        tend = self.data[cat0]['time_interval']['time_end']
        self.start = tstart[0]
        self.end = tend[-1]
        self.times_start = tstart
        self.times_end = tend
        self.tres = tstart[1]-tstart[0]
        self.nt, self.nlat, self.nlon = self.data[cat0]['emis'].shape
        self.region = regions.region(longitudes=self.data[cat]['lons'], latitudes=self.data[cat]['lats'])
        self.coordinates = SpatialCoordinates(obj=self.region)
        self.origin = self.start

    def write(self, outfile):
        WriteStruct(self.data, outfile)

    def between_times(self, start, end):
        Flux(self.data.between_times(start, end))


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

    def run(self, obslist, emis, step):
        self.read()  # Read basic information from the file itself
        self.setup(emis.coordinates, emis.origin, emis.tres)

        # 1st, check which obsids are present in the file:
        footprint_valid = [o in self.footprints for o in obslist.obsid]
        #logger.debug(f"{sum(footprint_valid)} valid footprints of {obslist.shape[0]} in file {self.filename}")
        obslist = obslist.loc[footprint_valid].copy()
        if step == 'forward' :
            return self._runForward(obslist, emis)
        elif step == 'adjoint' :
            return self._runAdjoint(obslist, emis)

    def _runForward(self, obslist, emis):
        """
        Compute the (foreground) concentration for a specific obs (given by its obsid) and a surface flux, provided as argument
        """
        for iobs, obs in tqdm(obslist.iterrows(), desc=self.filename, total=obslist.shape[0], disable=self.silent):
            fpt = self.getFootprint(obs.obsid, origin=emis.origin)
            for cat in emis.categories :
                obslist.loc[iobs, cat] = fpt.to_conc(emis.data[cat]['emis'])
        self.close()
        return obslist.loc[:, emis.categories]

    def _runAdjoint(self, obslist, adjstruct):
        for iobs, obs in tqdm(obslist.iterrows(), desc=self.filename, total=obslist.shape[0], disable=self.silent):
            fpt = self.getFootprint(obs.obsid, origin=adjstruct.origin)
            for cat in adjstruct.categories :
                adjstruct.data[cat]['emis'] = fpt.to_adj(obs.dy, adjstruct.data[cat]['emis'])#.copy())
            #print(obs.site, obs.time, obs.dy, adjstruct.data['biosphere']['emis'].sum())
            #if obs.time.day == 21 :
            #    import pdb; pdb.set_trace()
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
            import pdb
            pdb.set_trace()
        assert shift_t - int(shift_t) == 0
        self.itims += int(shift_t)
        self.origin = origin

    def to_conc(self, flux):
        # Here we assume that flux has the exact same shape and coordinates as the footprints
        # TODO: make this a bit more explicit ... 
        s = (self.itims >= 0) * (self.itims < flux.shape[0])
        if s.shape[0] == 0 :
            return nan
        if self.itims.min() < 0 :
            return nan
        return (flux[self.itims[s], self.ilats[s], self.ilons[s]]*self.sensi[s]).sum()

    def to_adj(self, dy, adjfield):
        # TODO: this (and the forward) accept only one category ... maybe here is the place to implement more?
        s = (self.itims >= 0) * (self.itims < adjfield.shape[0])
        if s.shape[0] == 0 :
            return adjfield
        if self.itims.min() < 0 :
            return adjfield
        adjfield[self.itims[s], self.ilats[s], self.ilons[s]] += self.sensi[s]*dy
        return adjfield

    def itime_to_times(self, it):
        return self.origin+it*self.dt


@ray.remote
def ray_worker(filename, obslist, emis, FootprintFileClass, step='forward', tmpdir='.'):
    fpf = FootprintFileClass(filename, silent=True)
    res = fpf.run(obslist, emis, step)
    if step == 'forward':
        return res
    else :
        fname_out = os.path.join(tmpdir, f'adj_{os.path.basename(filename)}.pickle')
        logger.debug(f"writing adjoint file {fname_out}")
        with open(fname_out, 'wb') as fid :
            compact = {}
            for cat in res.data.keys():
                nzi = nonzero(res.data[cat]['emis'])
                nzv = res.data[cat]['emis'][nzi]
                compact[cat] = (nzi, nzv)
            pickle.dump(compact, fid)
    return fname_out


@ray.remote
def ray_worker_adjoint(filename, obslist, categories, region, start, end, dt, FootprintFileClass, tmpdir='.'):
    fpf = FootprintFileClass(filename, silent=True)
    adj = Flux(CreateStruct(categories, region, start, end, dt))
    res = fpf.run(obslist, adj, 'adjoint')
    fname_out = os.path.join(tmpdir, f'adj_{os.path.basename(filename)}.pickle')
    #logger.debug(f"writing adjoint file {fname_out}")
    with open(fname_out, 'wb') as fid :
        compact = {}
        for cat in res.data.keys():
            nzi = nonzero(res.data[cat]['emis'])
            nzv = res.data[cat]['emis'][nzi]
            compact[cat] = (nzi, nzv)
        pickle.dump(compact, fid)
    return fname_out


def loop_forward(filename):
    obslist = common['obslist'].loc[common['obslist'].footprint == filename, ['obsid']]#.copy()
    fpf = common['fpclass'](filename, silent=True)
    res = fpf.run(obslist, common['emis'], 'forward')
    return res


def loop_adjoint(filename):
    obslist = common['obslist'].loc[common['obslist'].footprint == filename, ['obsid', 'dy']]
    adj = Flux(CreateStruct(common['categories'], common['region'], common['start'], common['end'], common['tres']))
    fpf = common['fpclass'](filename, silent=True)
    res = fpf.run(obslist, adj, 'adjoint')

    fname_out = os.path.join(common['tmpdir'], f'adj_{os.path.basename(fpf.filename)}.pickle')
    with open(fname_out, 'wb') as fid :
        compact = {}
        for cat in res.data.keys():
            nzi = nonzero(res.data[cat]['emis'])
            nzv = res.data[cat]['emis'][nzi]
            compact[cat] = (nzi, nzv)
        pickle.dump(compact, fid)
    return fname_out


class FootprintTransport:
    def __init__(self, rcf, obs, emfile=None, FootprintFileClass=FootprintFile, mp=False, checkfile=None, ncpus=None):
        self.rcf = rctools.rc(rcf)
        self.obs = Observations(obs)
        self.obs.checkIndex(reindex=True)
        self.emfile = emfile

        # Just in case ...
        self.obsfile = obs
        self.rcfile = rcf

        # Internals
        self.executable = __file__
        self.ncpus = ncpus# self.rcf.get('model.transport.split', default=os.cpu_count())
        self._set_parallelization(False)
        if mp :
            self._set_parallelization('ray')
        self.FootprintFileClass = FootprintFileClass

        if emfile is not None :
            self.categories = Categories(self.rcf)
        else :
            logger.warn("No emission files has been provided. Ignore this warning if it's on purpose")
        self.checkfile=checkfile

    def _set_parallelization(self, mode=False):
        if not mode :
            self._forward_loop = self._forward_loop_mp
            self._adjoint_loop = self._adjoint_loop_mp
        elif mode == 'ray' :
            self._forward_loop = self._forward_loop_mp
            self._adjoint_loop = self._adjoint_loop_mp

    def runForward(self):
        # Read the emissions:
        self.emis = Flux(self.emfile)

        # Add the flux columns to the observations dataframe:
        for cat in self.emis.categories:
            self.obs.observations.loc[:, f'mix_{cat}'] = nan

        filenames = self.obs.observations.footprint.dropna().drop_duplicates()
        self._forward_loop(filenames)

        # Combine the flux components:
        self.obs.observations.loc[:, 'mix'] = self.obs.observations.mix_background.copy()
        for cat in self.emis.categories :
            self.obs.observations.mix += self.obs.observations.loc[:, f'mix_{cat}'].values
        
        self.obs.save_tar(self.obsfile)

    def runAdjoint(self):
        # 1) Create an empty adjoint structure
        region = Region(self.rcf)
        categories = [c for c in self.rcf.get('emissions.categories') if self.rcf.get(f'emissions.{c}.optimize', totype=bool, default=False) is True]
        start = datetime(*self.rcf.get('time.start'))
        end = datetime(*self.rcf.get('time.end'))
        dt = time_interval(self.rcf.get('emissions.interval'))
        adj = Flux(CreateStruct(categories, region, start, end, dt))

        # 2) Loop over the footprint files
        filenames = self.obs.observations.footprint.dropna().drop_duplicates()
        adj = self._adjoint_loop(filenames, adj)

        # 3) Write the updated adjoint field
        WriteStruct(adj.data, self.emfile)

    # def _adjoint_loop_serial(self, filenames, adj):
    #     for filename in tqdm(filenames):
    #         obslist = self.obs.observations.loc[self.obs.observations.footprint == filename, ['obsid', 'dy', 'time', 'site']].copy()
    #         adj = self.get(filename).run(obslist, adj, 'adjoint')
    #     return adj

    def _adjoint_loop_mp(self, filenames, adj):
        t0 = datetime.now()
        nobs = array([self.obs.observations.loc[self.obs.observations.footprint == f].shape[0] for f in filenames])
        filenames = [filenames.values[i] for i in argsort(nobs)[::-1]]

        # I use the common because these are not iterable, but it would probably be cleaner to use apply_asinc and pass these as arguments to loop_adjoint 
        common['categories'] = adj.categories
        common['region'] = adj.region
        common['start'] = adj.start
        common['end'] = adj.end
        common['tres'] = adj.tres
        common['tmpdir'] = self.rcf.get('path.run')
        common['obslist'] = self.obs.observations
        common['fpclass'] = self.FootprintFileClass

        with Pool(processes=self.ncpus) as pool :
            res = list(tqdm(pool.imap(loop_adjoint, filenames, chunksize=1), total=len(nobs)))

        for w in res:
            with open(w, 'rb') as fid :
                compact = pickle.load(fid)
                for cat in compact.keys():
                    nzi, nzv = compact[cat]
                    adj.data[cat]['emis'][nzi] += nzv
        
        print(datetime.now()-t0)
        return adj

    def _adjoint_loop_ray(self, filenames, adj):
        ray.init()
        workers = []
        for filename in filenames :
            obslist = self.obs.observations.loc[self.obs.observations.footprint == filename, ['obsid', 'dy']].copy()
            #workers.append(ray_worker.remote(filename, obslist, adj, self.FootprintFileClass, 'adjoint', tmpdir=self.rcf.get('path.run')))
            workers.append(ray_worker_adjoint.remote(
                filename, obslist, adj.categories, adj.region, adj.start, adj.end, adj.tres, self.FootprintFileClass, tmpdir=self.rcf.get('path.run')
            ))
        for w in tqdm(workers):
            with open(ray.get(w), 'rb') as fid :
                compact = pickle.load(fid)
                for cat in compact.keys():
                    nzi, nzv = compact[cat]
                    adj.data[cat]['emis'][nzi] += nzv
                #adj.data[nzi] += nzv #pickle.load(fid)
            #data = ReadStruct(ray.get(w))
            #adj += ray.get(data)
        ray.shutdown()
        return adj

    def _forward_loop_ray(self, filenames):
#        t0 = datetime.now()
        ray.init()
        emis_id = ray.put(self.emis)
        workers = []
        for filename in filenames:
            obslist = self.obs.observations.loc[self.obs.observations.footprint == filename, ['obsid']].copy()
            workers.append(ray_worker.remote(filename, obslist, emis_id, self.FootprintFileClass))
        
        for w in tqdm(workers) :
            obslist = ray.get(w)
            for field in obslist.columns :
                self.obs.observations.loc[obslist.index, f'mix_{field}'] = obslist.loc[:, field]
        ray.shutdown()
#        print(datetime.now()-t0)
        return

    def _forward_loop_mp(self, filenames):
        t0 = datetime.now()
        nobs = array([self.obs.observations.loc[self.obs.observations.footprint == f].shape[0] for f in filenames])
        common['emis'] = self.emis
        common['obslist'] = self.obs.observations
        common['fpclass'] = self.FootprintFileClass

        # To optimize CPU usage, process the largest files first
        filenames = [filenames.values[i] for i in argsort(nobs)[::-1]]

        with Pool(processes=self.ncpus) as pool :
            res = list(tqdm(pool.imap(loop_forward, filenames, chunksize=1), total=len(nobs)))

        for filename, obslist in zip(filenames, res):
            for field in obslist.columns :
                self.obs.observations.loc[obslist.index, f'mix_{field}'] = obslist.loc[:, field]
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
