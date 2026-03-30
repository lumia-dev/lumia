# from pandas import Timedelta, Timestamp, DataFrame, TimedeltaIndex, concat
# import h5py
# import sys
# import os
# import logging
# from lumia.Tools.gridtools import Grid
# from numpy import inf
# from loguru import logger
# from typing import List
# from types import SimpleNamespace
# from dataclasses import asdict

# from footprints import FootprintTransport, FootprintFile, SpatialCoordinates
# from archive import Archive
# from numpy import array, nan, meshgrid, nonzero
# from tqdm import tqdm

# logger = logging.getLogger(os.path.basename(__file__))

from calendar import c
import sys
import os
import logging
import xarray as xr
import h5py
from h5py import File
from datetime import datetime, timedelta
from footprints_ffCO2 import FootprintTransport, FootprintFile, SpatialCoordinates
# from concentrations import interp_file, read_conc_file
from archive import Archive
from numpy import array, nan, meshgrid, nonzero
from netCDF4 import Dataset, chartostring
from tqdm import tqdm

logger = logging.getLogger(os.path.basename(__file__))


class Interval:
    def __init__(self, key):
        self.key = key
        t1, t2 = key.split('_')
        t1 = datetime.strptime(t1, '%Y%m%d%H%M%S')
        t2 = datetime.strptime(t2, '%Y%m%d%H%M%S')
        self.start = min(t1, t2)
        self.end = max(t1, t2)
        self.dt = self.end-self.start
        self.valid = t1 > t2

    def calc_index(self, origin):
        itim = (self.start-origin)/self.dt
        assert itim%1 == 0
        return int(itim)

    def __lt__(self, other):
        assert isinstance(other, self.__class__)
        assert self.dt == other.dt
        return self.start < other.start


class LumiaFootprintFile(FootprintFile):

    def read(self):

        if not os.path.exists(self.filename):
            return False
        self.ds = File(self.filename, 'r')
        self.close = self.ds.close
        self.footprints = [x for x in self.ds.keys() if isinstance(self.ds[x], h5py.Group)]

        # Store time and space coordinates
        try :
            self.coordinates = SpatialCoordinates(
                lats=self.ds['latitudes'][:],
                lons=self.ds['longitudes'][:]
            )
        except :
            print(self.filename)
            raise RuntimeError

        self.dt = timedelta(seconds=int(abs(self.ds.attrs['run_loutstep'])))
        self.origin = datetime.strptime(self.ds.attrs['origin'], '%Y-%m-%d %H:%M:%S')

        # Copy them to the Footprint class 
        self.Footprint.lats = self.coordinates.lats
        self.Footprint.lons = self.coordinates.lons
        self.Footprint.dlat = self.coordinates.dlat
        self.Footprint.dlon = self.coordinates.dlon
        self.Footprint.dt = self.dt

        self._initialized = True

    def close(self):
        pass

    def setup(self, coords, origin, dt):
        assert self.Footprint.dt == dt, print(self.Footprint.dt, dt)
        # Calculate the number of time steps between the Footprint class (i.e 
        # the data in the file) and the requested new origin
        shift_t = (self.origin-origin)/self.dt
        assert shift_t-int(shift_t) == 0

        # Store the number of time steps and set the new origin of the Footprint class
        self.shift_t = int(shift_t)
        self.origin = origin

    def getFootprint(self, obsid, origin=None):

        fp = self.Footprint()
        fp.itims = self.ds[obsid]['itims'][:] 
        fp.ilats = self.ds[obsid]['ilats'][:]
        fp.ilons = self.ds[obsid]['ilons'][:]

        if self.ds[obsid]['sensi'].attrs.get('units') == 's m3 kg-1':
            fp.sensi = self.ds[obsid]['sensi'][:] * 0.0002897
        
        fp.origin = self.origin

        fp.itims += self.shift_t

        valid = sum(fp.sensi) > 0
        if not valid :
            msg = f"No usable data found in footprint {obsid}"
            if len(fp.itims) == 0 :
                logger.info(msg+" (the footprint is empty)")
            else :
                logger.info(msg+ f": the footprint covers the period {fp.itime_to_times(fp.itims.min())} to {fp.itime_to_times(fp.itims.max())}")
        return fp

    def writeFootprints(self, obs, footprint):
        raise NotImplementedError
    
class LumiaFootprintTransport(FootprintTransport):
    def __init__(self, rcf, obs, emfile=None, atmdel=None, mp=False, checkfile=None, ncpus=None):
        super().__init__(rcf, obs, emfile, atmdel, LumiaFootprintFile, mp, checkfile, ncpus)

    def genFileNames(self, tr, t):
        return [f'{o.site}.{o.height:.0f}m.{o.time.strftime("%Y-%m")}.hdf' for o in self.obs.observations.loc[(self.obs.observations.tracer == tr) & (self.obs.observations.type == t)].itertuples()]

    def checkFootprints(self, path, archive=None):

        for tr, tp in path.items():
            for t, p in tp.items():
                cache = Archive(p, parent=Archive(archive))
                fnames = array(self.genFileNames(tr, t))
                exists = array([cache.get(f, dest=p, fail=False) for f in tqdm(self.genFileNames(tr, t), desc=f"Checking footprints for {tr} {t}")])
                fnames = array([os.path.join(p, fname) for fname in fnames])
                # if fnames is empty, the following line will raise a warning
                if len(fnames) == 0 :
                    logger.warning(f"No observations found for {tr} {t}")
                else:
                    self.obs.observations.loc[(self.obs.observations.tracer == tr) & (self.obs.observations.type == t), 'footprint'] = fnames
                    self.obs.observations.loc[(self.obs.observations.tracer == tr) & (self.obs.observations.type == t)].loc[~exists, 'footprint'] = nan

        
        # Drop the rows with nan footprints
        self.obs.observations.dropna(subset=['footprint'], inplace=True)

    def genObsIDs(self):
        
        exists = array([os.path.exists(fname) for fname in self.obs.observations.footprint])
        self.obs.observations.loc[~exists, 'footprint'] = nan

        # Construct the obs ids:
        obsids = [f'{o.site}.{o.height:.0f}m.{o.time.to_pydatetime().strftime("%Y%m%d-%H%M%S")}' for o in self.obs.observations.itertuples()]
        self.obs.observations.loc[:, 'obsid'] = obsids


if __name__ == '__main__':
    import sys
    from argparse import ArgumentParser, REMAINDER

    logger = logging.getLogger(os.path.basename(__file__))

    p = ArgumentParser()
    p.add_argument('--forward', '-f', action='store_true', default=False, help="Do a forward run")
    p.add_argument('--adjoint', '-a', action='store_true', default=False, help="Do an adjoint run")
    p.add_argument('--adjtest', '-t', action='store_true', default=False, help="Perform an adjoint test")
    p.add_argument('--serial', '-s', action='store_true', default=False, help="Run on a single CPU")
    p.add_argument('--ncpus', '-n', default=32)
    p.add_argument('--verbosity', '-v', default='INFO')
    # p.add_argument('--background', '-b', type=str, nargs='*', default=None, help="Path or glob pattern pointing to concentrations files to use as background (files should be in the CAMS format). If a 'mix_background' field is present in the observations, the backgrounds won't be re-interpolated")
    p.add_argument('--rc')
    p.add_argument('--db', required=True)
    p.add_argument('--emis', required=True) 
    p.add_argument('--atmdel')
    p.add_argument('--no-check-footprints', action='store_false', default=True, help="Locate the footprint files and check them. Should be set to False if a `footprints` column is already present in the observation file", dest='checkFootprints')
    p.add_argument('args', nargs=REMAINDER)
    args = p.parse_args(sys.argv[1:])

    logger.setLevel(args.verbosity)
    logger.info('test logger')
    logger.debug('test logger')
    logger.warning('test logger')

    # Create the transport model
    model = LumiaFootprintTransport(args.rc, args.db, args.emis, args.atmdel, mp = not args.serial, ncpus=args.ncpus) 

    if args.checkFootprints: 
        ftp_path = {}
        trlist = model.rcf.get('obs.tracers') if isinstance(model.rcf.get('obs.tracers'), list) else [model.rcf.get('obs.tracers')]
        for tr in trlist:
            ftp_path[tr] = {}
            tplist = model.rcf.get(f'obs.type.{tr}') if isinstance(model.rcf.get(f'obs.type.{tr}'), list) else [model.rcf.get(f'obs.type.{tr}')]
            for tp in tplist:
                ftp_path[tr][tp] = model.rcf.get(f'path.{tr}.{tp}.footprints')
        model.checkFootprints(ftp_path)
    model.genObsIDs()

    if args.forward :
        model.runForward()

    elif args.adjoint :
        model.runAdjoint()

    elif args.adjtest :
        model.adjoint_test()
