#!/usr/bin/env python

import os
import shutil
from datetime import datetime
from types import SimpleNamespace
from numpy import array_equal, nan, array, unique, nonzero, argsort, append, dot, finfo
from multiprocessing import Pool
from tqdm import tqdm
#from lumia.Tools import Categories
import rctools
from lumia import obsdb
#from lumia.formatters.lagrange import ReadStruct, WriteStruct, Struct
from lumia.formatters.xr import CreateStruct_adj, TracerEmis, Data, ReadStruct, WriteStruct
#from lumia.Tools import regions
from lumia.units import units_registry as ureg
from loguru import logger
import pickle

from gridtools import grid_from_rc

# Cleanup needed in the following ...
#from lumia.Tools import Region
from lumia.Tools.time_tools import time_interval


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
    def __init__(self, fluxfile, categories=None):
        if isinstance(fluxfile, Data) :
            self.data = fluxfile
        else :
            self.data = ReadStruct(fluxfile, categories=categories)

        for tracer in self.data.tracers:
            self.data[tracer].attrs['coordinates'] = SpatialCoordinates(obj=self.data[tracer].grid)
            self.data[tracer].attrs['origin'] = self.data[tracer].start

    def write(self, outfile):
        for tracer in self.data.tracers :
            del self.data[tracer].attrs['coordinates']
            del self.data[tracer].attrs['origin']
        WriteStruct(self.data, outfile)

    @property
    def categories(self):
        for cat in self.data.categories : 
            yield cat

    @property
    def tracers(self):
        for tracer in self.data.tracers :
            yield tracer

    def __getitem__(self, name):
        return self.data[name]

    def between_times(self, start, end):
        Flux(self.data.between_times(start, end))

    def asvec(self):
        """
        Returns a vector representation of the data (primarily for adjoint test purpose)
        """
        vec = array(())
        for cat in sorted(self.categories) :
            logger.debug(cat)
            vec = append(vec, self.data[cat]['emis'].reshape(-1))
        return vec


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
        self.setup(emis.coordinates, emis.origin, emis.period)

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
                obslist.loc[iobs, cat] = fpt.to_conc(emis[cat].data)
        self.close()
        try :
            return obslist.loc[:, emis.categories]
        except KeyError :
            return None

    def _runAdjoint(self, obslist, adjstruct):
        for iobs, obs in tqdm(obslist.iterrows(), desc=self.filename, total=obslist.shape[0], disable=self.silent):
            fpt = self.getFootprint(obs.obsid, origin=adjstruct.origin)
            for cat in adjstruct.categories :
                adjstruct[cat].data = fpt.to_adj(obs.dy, adjstruct[cat].data)#.copy())
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
            import pdb; pdb.set_trace()
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


def loop_forward(filename, silent=True):
    obslist = common['obslist'].loc[common['obslist'].footprint == filename, ['obsid']]#.copy()
    fpf = common['fpclass'](filename, silent=silent)
    res = fpf.run(obslist, common['emis'], 'forward')
    return res


def loop_adjoint(filename, silent=True):
    compact = {}
    for tracer in common['obslist'].loc[:, 'tracer'].drop_duplicates():
        obslist = common['obslist'].loc[common['obslist'].footprint == filename, ['obsid', 'dy']]
        adj = Data()
        adj.add_tracer(CreateStruct_adj(tracer, common['categories'], common['region'], common['start'], common['end'], common['tres']))
        adj = Flux(adj).data[tracer]

        fpf = common['fpclass'](filename, silent)

        res = fpf.run(obslist, adj, 'adjoint')

        fname_out = os.path.join(common['tmpdir'], f'adj_{os.path.basename(fpf.filename)}.{tracer}.pickle')

        compact[tracer] = {}
        for cat in res.categories:
            nzi = nonzero(res[cat].data)
            compact[tracer][cat] = SimpleNamespace()
            compact[tracer][cat].indices = nzi
            compact[tracer][cat].values = res[cat].data[nzi]
    with open(fname_out, 'wb') as fid :
        pickle.dump(compact, fid)
    return fname_out


class FootprintTransport:
    def __init__(self, rcf, obs, emfile=None, FootprintFileClass=FootprintFile, mp=False, checkfile=None, ncpus=None):
        self.rcf = rctools.RcFile(rcf)
        self.obs = Observations(obs)
        self.obs.checkIndex(reindex=True)
        self.emfile = emfile
        self.tracers = self.rcf.rcfGet('tracers', tolist='force')

        # Just in case ...
        self.obsfile = obs
        self.rcfile = rcf

        # Internals
        self.executable = __file__
        self.ncpus = ncpus
        # self.ncpus = self.rcf.rcfGet('model.transport.split', default=os.cpu_count())
        self._set_parallelization(False)
        if mp :
            self._set_parallelization('ray')
        self.FootprintFileClass = FootprintFileClass

        #if emfile is not None :
        #    self.categories = Categories(self.rcf)
        #else :
        #    logger.warn("No emission files has been provided. Ignore this warning if it's on purpose")
        self.checkfile=checkfile

    def _set_parallelization(self, mode=False):
        if not mode :
            self._forward_loop = self._forward_loop_serial
            self._adjoint_loop = self._adjoint_loop_serial
        elif mode == 'ray' :
            self._forward_loop = self._forward_loop_mp
            self._adjoint_loop = self._adjoint_loop_mp
            # TODO: remove the next two lines. Only did so for debugging
        self._forward_loop = self._forward_loop_serial
        self._adjoint_loop = self._adjoint_loop_serial

    def runForward(self, categories=None):
        # Read the emissions:
        self.emis = Flux(self.emfile, categories=categories)
        print('footprints.runForward() self.emis=', flush=True)
        print(self.emis, flush=True)

        for tracer in self.emis.tracers :
            region = self.emis[tracer].grid

            # Add the flux columns to the observations dataframe:
            for cat in self.emis[tracer].categories:
                self.obs.observations.loc[self.obs.observations.tracer == tracer, f'mix_{cat}'] = nan

            filenames = self.obs.observations.footprint.loc[self.obs.observations.tracer == tracer]
            filenames = filenames.dropna().drop_duplicates()
            print('footprints.runForward() filenames=', flush=True)
            print(filenames, flush=True)

            common['emis'] = self.emis[tracer]
            common['obslist'] = self.obs.observations
            common['fpclass'] = self.FootprintFileClass

            # To optimize CPU usage in parallell simulations, process the largest files first
            nobs = array([self.obs.observations.loc[self.obs.observations.footprint == f].shape[0] for f in filenames])
            filenames = [filenames.values[i] for i in argsort(nobs)[::-1]]


            res = self._forward_loop(filenames)
            print('footprints.runForward() res=', flush=True)
            print(res, flush=True)
         

            # Combine the results:
            for obslist in res:
                if obslist is not None :
                    for field in obslist.columns :
                        self.obs.observations.loc[obslist.index, f'mix_{field}'] = obslist.loc[:, field]

            # Combine the flux components:
            try :
                self.obs.observations.loc[:, 'mix'] = self.obs.observations.mix_background.copy()
            except AttributeError :
                logger.warning("Missing background concentrations. Assuming mix_background=0")
                self.obs.observations.loc[:, 'mix_background'] = 0.
                self.obs.observations.loc[:, 'mix'] = 0.
            for cat in self.emis.categories :
                self.obs.observations.mix += self.obs.observations.loc[:, f'mix_{cat.name}'].values
        print('completed footprints.runForward() self.obs.observations.mix=', flush=True)
        print(self.obs.observations.mix, flush=True)

        # self.obs.save_tar(self.obsfile) # ==> moved to __main__

    def runAdjoint(self):
        # 1) Create an empty adjoint structure
        #region = Region(self.rcf)
        #start = datetime(*self.rcf.rcfGet('run.time.start'))
        #end = datetime(*self.rcf.rcfGet('run.time.end'))
        # essentially this meant in older versions of the yml config file:  start=(datetime(2012, 5, 1))) with start being a datetime object
        # now with run.time.start being a Timestamp string this needs to be changed to:
        timestamp_string = self.rcf.rcfGet('run.time.start') #"2018-01-01 00:00:00"
        format_string = "%Y-%m-%d %H:%M:%S"
        start = datetime.strptime(timestamp_string, format_string)
        timestamp_string = self.rcf.rcfGet('run.time.end') #"2018-12-31 23:59:59"
        end = datetime.strptime(timestamp_string, format_string)
        logger.debug(f'start={start},  end={end} from run.time.end={timestamp_string}')

        adj = Data()
        for tracer in self.tracers :
            region = grid_from_rc(self.rcf, name=tracer)
            cats=self.rcf.rcfGet(f'emissions.{tracer}.categories')
            logger.debug(f'rcf.rcfGet(emissions.{tracer}.categories)={cats}')
            categories = [c for c in self.rcf.rcfGet(f'emissions.{tracer}.categories') if self.rcf.rcfGet(f'emissions.{tracer}.{c}.optimize', default=False)]
            dt = time_interval(self.rcf.rcfGet(f'emissions.{tracer}.interval'))
            adj.add_tracer(CreateStruct_adj(tracer, categories, region, start, end, dt))
        adj = Flux(adj)

        for tracer in adj.tracers :

            # 2) Loop over the footprint files
            filenames = self.obs.observations.loc[self.obs.observations.tracer == tracer]
            filenames = filenames.footprint.dropna().drop_duplicates()
    #        filenames = self.obs.observations.footprint.dropna().drop_duplicates()

            nobs = array([self.obs.observations.loc[self.obs.observations.footprint == f].shape[0] for f in filenames])
            filenames = [filenames.values[i] for i in argsort(nobs)[::-1]]

            # I use the common because these are not iterable, but it would probably be cleaner to use apply_asinc and pass these as arguments to loop_adjoint 
            common['categories'] = adj[tracer].categories
            common['region'] = adj[tracer].grid
            common['start'] = adj[tracer].start
            common['end'] = adj[tracer].end
            common['tres'] = adj[tracer].period
            # common['tmpdir'] = self.rcf.rcfGet('run.paths.temp')
            common['tmpdir'] = self.rcf['run']['paths']['temp']
            common['obslist'] = self.obs.observations
            common['fpclass'] = self.FootprintFileClass

            res = self._adjoint_loop(filenames)

            for w in res:
                with open(w, 'rb') as fid :
                    compact = pickle.load(fid)
                    for cat in compact[tracer]:
                        adjfield = compact[tracer][cat]
                        adj[tracer][cat].data[adjfield.indices] += adjfield.values
            
            #adj.add_tracer(adj[tracer].data)
            # 3) Write the updated adjoint field
            # WriteStruct(adj.data, self.emfile)  # ==> moved to __main__
        return adj

    def adjoint_test(self):
        # 1) Get the list of categories to be optimized:
        self.obs.observations.loc[:, 'mix_background'] = 0.
        #self.obs.observations = self.obs.observations.loc[:0]
        tracers = self.rcf.rcfGet('run.tracers',  default=['CO2'])
        categories = [c for c in self.rcf.rcfGet('emissions.categories') if self.rcf.rcfGet(f'emissions.{tracers[0]}.{c}.optimize', totype=bool, default=False) is True]
        self.runForward(categories=categories)
        x1 = self.emis.asvec()
        self.obs.observations.dropna(subset=['mix'], inplace=True)
        y1 = self.obs.observations.loc[:, 'mix'].values
        y2 = y1 # random.randn(y1.shape[0])
        self.obs.observations.loc[:, 'dy'] = y2
        adj = self.runAdjoint()
        x2 = adj.asvec()
        adjtest = 1 - dot(x1, x2)/dot(y1, y2)
        logger.info(f"Adjoint test: {dot(x1, x2)-dot(y1, y2) = }")
        logger.info(f"Adjoint test: {1 - dot(x1, x2)/dot(y1, y2) = }")
        if abs(adjtest) < finfo('float32').eps :
            logger.info("Success")
        else :
            logger.warning("Adjoint test failed")
        logger.info(f"Assumed machine precision of: {finfo('float32').eps = }")

    def _adjoint_loop_serial(self, filenames):
        res = []
        for filename in tqdm(filenames):
            res.append(loop_adjoint(filename, silent=False))
        return res

    def _adjoint_loop_mp(self, filenames):
        with Pool(processes=self.ncpus) as pool :
            res = list(tqdm(pool.imap(loop_adjoint, filenames, chunksize=1), total=len(filenames)))
        return res 

    def _forward_loop_serial(self, filenames):
        res = []
        for filename in tqdm(filenames):
            res.append(loop_forward(filename, silent=False))
        return res

    def _forward_loop_mp(self, filenames):
        with Pool(processes=self.ncpus) as pool :
            res = list(tqdm(pool.imap(loop_forward, filenames, chunksize=1), total=len(filenames)))
        return res

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
