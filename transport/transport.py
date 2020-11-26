#!/usr/bin/env python

import os
import sys
import logging
from datetime import datetime, timedelta
from numpy import array_equal, nan
from h5py import File
from lumia import tqdm
from lumia import obsdb
from lumia.Tools import rctools, Categories
from lumia.formatters.lagrange import ReadStruct, WriteStruct
import ray

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


class Flux:
    """
    This class slighly adapts the "Struct" class from the lumia.formatters.lagrange module: it ensures that
    all the fields are on the exact same grid (including temporally), and stores some variables related to that
    grid, to make the conformity check with the footprints easier and allow adjusting the time indices, which
    don't have the same origin.
    TODO: this should be directly part of the "Struct" class itself
    """
    def __init__(self, fluxfile):
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
        self.start = tstart[0]
        self.tres = tstart[1]-tstart[0]
        self.nt, self.nlat, self.nlon = self.data[cat0]['emis'].shape

    def write(self, outfile):
        WriteStruct(self.data, outfile)


class Footprint:
    shift_t = 0
    shift_lat = 0
    shift_lon = 0
    max_t = nan      # Enforce an invalid value to make sure it crashes
    max_ilon = nan   # if they are not explicitly defined
    max_ilat = nan

    # These two are only for debug
    origin = None
    dt = 0

    def __init__(self, h5group):
        ilats = h5group['ilats'][:] + self.shift_lat
        ilons = h5group['ilons'][:] + self.shift_lon
        itims = h5group['itims'][:] + self.shift_t
        sensi = h5group['sensi'][:]

        select = (itims >= 0) * (itims < self.max_t) 
        select *= (ilons >= 0) * (ilons < self.max_ilon) 
        select *= (ilats >= 0) * (ilats < self.max_ilat)

        self.ilats = ilats[select]
        self.ilons = ilons[select]
        self.itims = itims[select]
        self.sensi = sensi[select]
        self.valid = sum(self.sensi) > 0
        if not self.valid :
            logger.warn(f"No usable data found in footprint {h5group}")
            logger.debug(f"footprint covers the period {self.itime_to_times(itims.min())} to {self.itime_to_times(itims.max())}")

    def itime_to_times(self, it):
        return self.origin+it*self.dt

    def to_conc(self, flux):
        return (flux[self.itims, self.ilats, self.ilons]*self.sensi).sum()*0.0002897


class FootprintFile:
    """
    This class is used to apply forward or adjoint transport to the footprints contained in a single
    footprint file (which can itself contain multiple footprints).
    The format of the footprints is a HDF5 file, with one group for each footprint (the group name identifies
    the observation). Within each group, there are four variables:
        - sensi: the non-zero elements of the footprint
        - ilats, ilons, itims: the lat/lon/time grid indices of the values in sensi
    The grid is dedined spatially by the "latitudes", "longitudes" top-level variables, and temporally by the
    "tres" and "start" dataset attributes.

    The main entry point is the "run" method, which takes three arguments:
        - an instance of the "Observations" class (obs or departures list)
        - an instance of the "Flux" class (fluxes or adjoint field)
        - an action ("forward" or "adjoint")
    """
    def __init__(self, filename, silent=False):
        self.filename = filename

        # Default values :
        self.shift_t = 0
        self.shift_lon = 0
        self.shift_lat = 0

        self._initialized = False

        self.silent=silent

    def init(self):
        if not self._initialized:
            self.ds = File(self.filename, 'r')
            self.tres = timedelta(seconds=self.ds.attrs['tres'])
            self.origin = datetime.strptime(self.ds.attrs['start'], '%Y-%m-%d %H:%M:%S')
            self.close = self.ds.close
            self.footprints = [x for x in self.ds.keys()]
            self._initialized = True

    def run(self, obslist, flux, step):
        """
        Main entry point of the FootprintFile class.
        * The "obs" argument is an instance of the "Observations" class, and contains as "data" attribute a pandas
          DataFrame with at least the following columns:
           - obsid: the unique id of each obs (which identifies the footprint it corresponds to in the file)
           - dy (in adjoint runs only): a list of model-data mismatches
        * The "emis" argument is an instance of the "Flux" class, and contains either the fluxes to be applied
          in the forward run, or the adjoint structure to where the results of the adjoint are stored.
        * The "step" argument needs to be either "forward" or "adjoint"
        """

        self.init()

        # 1st, check which obsids are present in the file:
        footprint_valid = [o in self.footprints for o in obslist.obsid]
        obslist = obslist.loc[footprint_valid].copy()
        if step == 'forward':
            return self._runForward(obslist, flux)

    def calc_time_shift(self, new_origin):
        shift_t = (self.origin-new_origin)/self.tres
        assert shift_t-int(shift_t) == 0
        self.shift_t = int(shift_t)

    def set_origin(self, emis):
        """
        The ilats/ilons/itims indices may need to be adjusted if the footprints and fluxes are not on the same grid.
        Typically this is needed for the footprints, as their temporal grid is specific to the footprint file.
        Spatial grid shifts may be added here in the future ...
        """
        # Check that the flux and footprints have the same time resolution:
        assert self.tres == emis.tres, f"The time resolution of the footprint file ({self.tres}) and of the fluxes ({emis.tres}) do not match"

        # Calculate the temporal shift
        self.calc_time_shift(emis.start)

        # Edit the "Footprint" class so that it uses these shifted values
        Footprint.shift_t = self.shift_t
        Footprint.shift_lat = 0
        Footprint.shift_lon = 0
        Footprint.max_t = emis.nt
        Footprint.max_ilat = emis.nlat
        Footprint.max_ilon = emis.nlon
        Footprint.origin = self.origin
        Footprint.dt = self.tres

    def _runForward(self, obslist, emis):
        """
        Compute the (foreground) concentration for a specific obs (given by its obsid) and a surface flux, provided as argument
        """
        self.set_origin(emis)
        for iobs, obs in tqdm(obslist.iterrows(), desc=self.filename, total=obslist.shape[0], disable=self.silent):
            fpt = Footprint(self.ds[obs.obsid])
        #    logger.debug('tata')
            for cat in emis.categories :
                obslist.loc[iobs, cat] = fpt.to_conc(emis.data[cat]['emis'])
        return obslist.loc[:, emis.categories]


@ray.remote
def ray_worker(filename, obslist, emis):
    fpf = FootprintFile(filename, silent=True)
    return fpf.run(obslist, emis, 'forward')


class Lagrange:
    def __init__(self, rcf, obs, emfile, mp=False, checkfile=None):
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
        self._set_parallelization('ray')
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
            workers.append(ray_worker.remote(filename, obslist, emis_id))
        
        for w in tqdm(workers) :
            obs = ray.get(w)
            for field in obs.columns :
                self.obs.observations.loc[obs.index, field] = obs.loc[:, field]
        ray.shutdown()
        return

    def _forward_loop_serial(self, filenames):
        for filename in tqdm(filenames):
            obslist = self.obs.observations.loc[self.obs.observations.footprint == filename, ['obsid']].copy()
            obslist = FootprintFile(filename).run(obslist, self.emis, 'forward')
            for field in obslist.columns :
                self.obs.observations.loc[obslist.index, field] = obslist.loc[:, field]


if __name__ == '__main__':
    from argparse import ArgumentParser, REMAINDER

    logger = logging.getLogger(os.path.basename(__file__))

    # Read arguments:
    p = ArgumentParser()
    p.add_argument('--forward', '-f', action='store_true', default=False, help="Do a forward run")
    p.add_argument('--adjoint', '-a', action='store_true', default=False, help="Do an adjoint run")
    p.add_argument('--serial', '-s', action='store_true', default=False, help="Run on a single CPU")
    p.add_argument('--check-footprints', action='store_true', default=True, help="Locate the footprint files and check them", dest='checkFootprints')
    p.add_argument('--checkfile', '-c')
    p.add_argument('--rc')
    p.add_argument('--db', required=True)
    p.add_argument('--emis', required=True)
    p.add_argument('--verbosity', '-v', default='INFO')
    p.add_argument('args', nargs=REMAINDER)
    args = p.parse_args(sys.argv[1:])

    logger.setLevel(args.verbosity)

    # Create the transport model
    model = Lagrange(args.rc, args.db, args.emis, mp=not args.serial, checkfile=args.checkfile)

    # Check footprints
    # This is the default behaviour but can be turned of (for instance during the inversion steps)
    # then, the "footprint" and "footprint_valid" columns must be provided
    if args.checkFootprints:
        model.checkFootprints(model.rcf.get('path.footprints'))

    if args.forward :
        model.runForward()
        model.write(args.obs)

#    if args.adjoint :
#        model.runAdjoint()
    if args.checkfile is not None :
        open(args.checkfile, 'w').close()