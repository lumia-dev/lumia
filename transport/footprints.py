#!/usr/bin/env python

import os
from numpy import array_equal, nan
import ray
from datetime import datetime, timedelta
from h5py import File
from tqdm import tqdm
from lumia.Tools import rctools, Categories
from lumia import obsdb
from lumia.formatters.lagrange import ReadStruct, WriteStruct, Struct
from lumia.Tools import regions
import logging

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


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
        self.end = tend[0]
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
    def __init__(self, filename, silent=False):
        self.filename = filename
        self.silent = silent
        self._initialized = False
        self.Footprint = LumiaFootprint

    def run(self, obslist, emis, step):
        self.open()  # Read basic information from the file itself
        self.setup(emis.coordinates, emis.origin, emis.tres)
        self.set_origin(emis.origin)

        # 1st, check which obsids are present in the file:
        footprint_valid = [o in self.footprints for o in obslist.obsid]
        obslist = obslist.loc[footprint_valid].copy()
        if step == 'forward':
            return self._runForward(obslist, emis)

    def _runForward(self, obslist, emis):
        """
        Compute the (foreground) concentration for a specific obs (given by its obsid) and a surface flux, provided as argument
        """
        for iobs, obs in tqdm(obslist.iterrows(), desc=self.filename, total=obslist.shape[0], disable=self.silent):
            fpt = self.getFootprint(obs.obsid, origin=self.origin)
            for cat in emis.categories :
                obslist.loc[iobs, cat] = fpt.to_conc(emis.data[cat]['emis'])
        self.close()
        return obslist.loc[:, emis.categories]


class SpatialCoordinates:
    def __init__(self, **kwargs):
        attrs = ['lons', 'lon0', 'dlon', 'nlon', 'lats', 'lat0', 'dlat', 'nlat']
        for attr in attrs :
            setattr(self, attr, kwargs.get(attr, None))

        # The following enables constructing a set of spatial coordinates from
        # an object that would already have some of the required attributes
        if 'obj' in kwargs :
            for attr in attrs :
                if hasattr(attr, 'obj'):
                    setattr(self, attr, getattr(attr, 'obj'))

        self._autocomplete()

    def itime_to_times(self, it):
        return self.origin+it*self.dt

    def _autocomplete(self):
        """
        Tries to calculate missing values based on the ones that are present
        For now, dlon and dlat are deduced from lons and lats
        """
        if self.lons is None and None not in [self.lon0, self.dlon, self.nlon]:
            self.lons = [self.lon0 + i*self.dlon for i in range(self.nlon)]
        elif self.dlon is None and self.lons is not None :
            self.dlon = self.lons[1]-self.lons[0]
        if self.lats is None and None not in [self.lat0, self.dlat, self.nlon]:
            self.lats = [self.lat0 + i*self.dlat for i in range(self.nlat)]
        elif self.dlat is None and self.lats is not None :
            self.dlat = self.lats[1]-self.lats[0]

    def __le__(self, other):
        res = all([l in other.lons for l in self.lons])
        res *= all([l in other.lats for l in self.lats])
        res *= (other.dlon == self.dlon) * (other.dlat == self.dlat)
        return res

    def __eq__(self, other):
        return array_equal(self.lats, other.lats) * array_equal(self.lons, other.lons)


class LumiaFootprint:
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
        self.itims += shift_t
        self.origin = origin

    def to_conc(self, flux):
        # Here we assume that flux has the exact same shape and coordinates as the footprints
        # TODO: make this a bit more explicit ... 
        s = (self.itims >= 0) * (self.itims <= flux.shape[0])
        return (flux[self.itims[s], self.ilats[s], self.ilons[s]]*self.sensi[s]).sum()*0.0002897


class LumiaFootprintFile(FootprintFile):
    def open(self, mode='r'):
        if not self._initialized and mode == 'r' :
            self.ds = File(self.filename, 'r')
            self.close = self.ds.close
            self.footprints = [x for x in self.ds.keys()]

            coordinates = SpatialCoordinates(
                lats=self.ds['latitudes'][:],
                lons=self.ds['longitudes'][:]
            )

            self.origin = datetime.strptime(self.ds.attrs['start'], '%Y-%m-%d %H:%M:%S')

            # Initialize the Footprint class:
            self.Footprint.lats = coordinates.lats
            self.Footprint.lons = coordinates.lons
            self.Footprint.dlat = coordinates.dlat
            self.Footprint.dlon = coordinates.dlon
            self.Footprint.dt = timedelta(seconds=self.ds.attrs['tres'])
            #self.Footprint.origin = datetime.strptime(self.ds.attrs['start'], '%Y-%m-%d %H:%M:%S')
            self._initialized = True

        else :
            with File(self.filename, 'r') as ds :
                if 'tres' in ds.attrs :
                    self.origin = datetime.strptime(ds.attrs['start'], '%Y-%m-%d %H:%M:%S')
                    self.tres = timedelta(seconds=ds.attrs['tres'])
                    self._initialized = True

    def setup(self, coords, origin, dt):
        assert SpatialCoordinates(obj=self.Footprint) == coords
        assert self.Footprint.dt == dt, print(self.Footprint.dt, dt)

    def set_origin(self, origin):
        # Calculate the number of time steps between the Footprint class (i.e 
        # the data in the file) and the requested new origin
        shift_t = (self.origin-origin)/self.Footprint.dt
        assert shift_t-int(shift_t) == 0

        # Store the number of time steps and set the new origin of the Footprint class
        self.shift_t = int(shift_t)
        self.origin = origin

    def getFootprint(self, obsid, origin=None):
        fp = LumiaFootprint()
        fp.itims = self.ds[obsid]['itims'][:] + self.shift_t
        fp.ilats = self.ds[obsid]['ilats'][:]
        fp.ilons = self.ds[obsid]['ilons'][:]
        fp.sensi = self.ds[obsid]['sensi'][:]
        fp.origin = self.origin
        valid = sum(fp.sensi) > 0
        if not valid :
            logger.info(f"No usable data found in footprint {obsid}")
            logger.info(f"footprint covers the period {fp.itime_to_times(fp.itims.min())} to {fp.itime_to_times(fp.itims.max())}")
        return fp
    
    def init_write(self, ds, footprint, origin=None):
        origin = footprint.origin if origin is None else origin
        ds.attrs['tres'] = footprint.ds.total_seconds()
        ds.attrs['start'] = footprint.origin.strftime('%Y-%m-%d %H:%M:%S')
        ds['latitudes'] = footprint.lats
        ds['longitudes'] = footprint.lons

    def addFootprint(self, obsid, footprint):
        with File(self.filename, 'a') as ds :
            if not self._initialized :
                self.init_write(ds, footprint)
            ds[f"{obsid}/ilons"] = footprint.ilons
            ds[f"{obsid}/ilats"] = footprint.ilats
            ds[f"{obsid}/itims"] = footprint.itims
            ds[f"{obsid}/sensi"] = footprint.sensi


@ray.remote
def ray_worker(filename, obslist, emis, FootprintFileClass):
    fpf = FootprintFileClass(filename, silent=True)
    return fpf.run(obslist, emis, 'forward')


class FootprintTransport:
    def __init__(self, rcf, obs, emfile, FootprintFileClass, mp=False, checkfile=None):
        self.rcf = rctools.rc(rcf)
        self.obs = obsdb(obs)
        self.obs.checkIndex(reindex=True)
        self.emfile = emfile

        # Just in case ...
        self.obsfile = obs
        self.rcfile = rcf

        # Internals
        self.executable = __file__
        self.ncpus = self.rcf.get('model.transport.split', default=os.cpu_count())
        if mp :
            self._set_parallelization('ray')
        self.FootprintFileClass = FootprintFileClass
        #self.batch = os.environ['INTERACTIVE'] == 'F'

        self.categories = Categories(self.rcf)
        self.checkfile=checkfile
        logger.debug(checkfile)

    def _set_parallelization(self, mode=False):
        if not mode :
            self._forward_loop = self._forward_loop_serial
        elif mode == 'ray' :
            self._forward_loop = self._forward_loop_ray

    def checkFootprints(self, path):
        """
        (Try to) guess the path to the files containing the footprints, and the path to the footprints in the files.
        This adds (or edits) three columns in the observation dataframe:
        - footprint : path to the footprint files
        - footprint_file_valid : whether the footprint file exists or not
        - obsid : path to the observation within the file
        """
        # Add the footprint files
        fnames = [f'{o.code}.{o.height:.0f}m.{o.time.year}-{o.time.month:02.0f}.hdf' for o in self.obs.observations.itertuples()]
        fnames = [os.path.join(path, f) for f in fnames]
        fnames = [f if os.path.exists(f) else nan for f in fnames]
        self.obs.observations.loc[:, 'footprint'] = fnames 

        # Construct the obs ids:
        obsids = [f'{o.code}.{o.height:.0f}m.{o.time.to_pydatetime().strftime("%Y%m%d-%H%M%S")}' for o in self.obs.observations.itertuples()]
        self.obs.observations.loc[:, 'obsid'] = obsids

    def runForward(self):
        # Read the emissions:
        self.emis = Flux(self.emfile)

        # Add the flux columns to the observations dataframe:
        for cat in self.emis.categories:
            self.obs.observations.loc[:, cat] = nan

        filenames = self.obs.observations.footprint.dropna().drop_duplicates()
        self._forward_loop(filenames)

    def _forward_loop_ray(self, filenames):
        ray.init()
        emis_id = ray.put(self.emis)
        workers = []
        for filename in filenames:
            obslist = self.obs.observations.loc[self.obs.observations.footprint == filename, ['obsid']].copy()
            workers.append(ray_worker.remote(filename, obslist, emis_id, FootprintFileClass=self.FootprintFileClass))
        
        for w in tqdm(workers) :
            obs = ray.get(w)
            for field in obs.columns :
                self.obs.observations.loc[obs.index, field] = obs.loc[:, field]
        ray.shutdown()
        return

    def _forward_loop_serial(self, filenames):
        for filename in tqdm(filenames):
            obslist = self.obs.observations.loc[self.obs.observations.footprint == filename, ['obsid']].copy()
            obslist = self.FootprintFileClass(filename).run(obslist, self.emis, 'forward')
            for field in obslist.columns :
                self.obs.observations.loc[obslist.index, field] = obslist.loc[:, field]