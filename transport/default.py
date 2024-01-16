#!/usr/bin/env python
import os
from h5py import File
from numpy import nan, array, int32, float32
from datetime import datetime, timedelta
from transport.footprints import FootprintTransport, FootprintFile, SpatialCoordinates
from archive import Archive
from tqdm import tqdm
from loguru import logger
import logging


class LumiaFootprintFile(FootprintFile):
    def read(self):
        if not os.path.exists(self.filename):
            return False
        self.ds = File(self.filename, 'r')
        self.close = self.ds.close
        self.footprints = [x for x in self.ds.keys()]

        # Store time and space coordinates
        try :
            self.coordinates = SpatialCoordinates(
                lats=self.ds['latitudes'][:],
                lons=self.ds['longitudes'][:]
            )
        except Exception :
            print(self.filename)
            raise RuntimeError
        self.origin = datetime.strptime(self.ds.attrs['start'], '%Y-%m-%d %H:%M:%S')
        self.dt = timedelta(seconds=self.ds.attrs['tres'])

        # Copy them to the Footprint class 
        self.Footprint.lats = self.coordinates.lats
        self.Footprint.lons = self.coordinates.lons
        self.Footprint.dlat = self.coordinates.dlat
        self.Footprint.dlon = self.coordinates.dlon
        self.Footprint.dt = self.dt

        self._initialized = True
        return True

    def setup(self, coords, origin, dt):
        #assert self.coordinates == coords, pdb.set_trace()
        assert self.Footprint.dt == dt, print(self.Footprint.dt, dt)
        #    logger.warning("Skipping assertion error for testing ... fixme urgently!!!")
        # Calculate the number of time steps between the Footprint class (i.e 
        # the data in the file) and the requested new origin
        shift_t = (self.origin-origin)/self.dt
        assert shift_t-int(shift_t) == 0

        # Store the number of time steps and set the new origin of the Footprint class
        self.shift_t = int(shift_t)
        self.origin = origin

    def getFootprint(self, obsid, origin=None):
        fp = self.Footprint()
        fp.itims = self.ds[obsid]['itims'][:] + self.shift_t
        fp.ilats = self.ds[obsid]['ilats'][:]
        fp.ilons = self.ds[obsid]['ilons'][:]
        fp.sensi = self.ds[obsid]['sensi'][:] * 0.0002897
        fp.origin = self.origin
        valid = sum(fp.sensi) > 0
        if not valid :
            msg = f"No usable data found in footprint {obsid}"
            if len(fp.itims) == 0 :
                logger.info(msg+" (the footprint is empty)")
            else :
                logger.info(msg+ f": the footprint covers the period {fp.itime_to_times(fp.itims.min())} to {fp.itime_to_times(fp.itims.max())}")
        return fp
    
    def writeFootprint(self, obs, footprint):
        obsid = f'{obs.code}.{obs.height:.0f}m.{obs.time.to_pydatetime().strftime("%Y%m%d-%H%M%S")}'
        with File(self.filename, 'a') as ds :
            if obsid in ds :
                logger.warning(f"Footprint {obsid} already in {self.filename}. Passing ...")
            else :
                if "tres" in ds.attrs :
                    self.origin = datetime.strptime(ds.attrs['start'], '%Y-%m-%d %H:%M:%S')
                else :
                    ds.attrs['tres'] = footprint.dt.total_seconds()
                    ds.attrs['start'] = footprint.origin.strftime('%Y-%m-%d %H:%M:%S')
                    ds['latitudes'] = footprint.lats
                    ds['longitudes'] = footprint.lons
                if not hasattr(self, 'origin'):
                    self.origin = datetime(footprint.origin.year, footprint.origin.month, 1)
                footprint.shift_origin(self.origin)
                ds[f"{obsid}/ilons"] = footprint.ilons.astype(int32)
                ds[f"{obsid}/ilats"] = footprint.ilats.astype(int32)
                ds[f"{obsid}/itims"] = footprint.itims.astype(int32)
                ds[f"{obsid}/sensi"] = footprint.sensi.astype(float32)


class LumiaFootprintTransport(FootprintTransport):
    def __init__(self, rcf, obs, emfile=None, mp=False, ncpus=None):
        super().__init__(rcf, obs, emfile, LumiaFootprintFile, mp, ncpus=ncpus)

    def genFileNames(self):
        return [f'{o.code.lower()}.{o.height:.0f}m.{o.time.year}-{o.time.month:02.0f}.hdf' for o in self.obs.observations.itertuples()]

    def checkFootprints(self, path, archive=None):
        """
        (Try to) guess the path to the files containing the footprints, and the path to the footprints in the files.
        This adds (or edits) three columns in the observation dataframe:
        - footprint : path to the footprint files
        - footprint_file_valid : whether the footprint file exists or not
        - obsid : path to the observation within the file
        """
        cache = Archive(path, parent=Archive(archive))

        # Add the footprint files
        fnames = array(self.genFileNames())
        exists = array([cache.get(f, dest=path, fail=False) for f in tqdm(self.genFileNames(), desc="Check footprints")])
        fnames = array([os.path.join(path, fname) for fname in fnames])
        self.obs.observations.loc[:, 'footprint'] = fnames
        self.obs.observations.loc[~exists, 'footprint'] = nan

        # Construct the obs ids:
        obsids = [f'{o.code.lower()}.{o.height:.0f}m.{o.time.to_pydatetime().strftime("%Y%m%d-%H%M%S")}' for o in self.obs.observations.loc[exists].itertuples()]
        self.obs.observations.loc[exists, 'obsid'] = obsids


if __name__ == '__main__':
    import sys
    from argparse import ArgumentParser, REMAINDER

    logging.captureWarnings(True)
    p = ArgumentParser()
    p.add_argument('--forward', '-f', action='store_true', default=False, help="Do a forward run")
    p.add_argument('--adjoint', '-a', action='store_true', default=False, help="Do an adjoint run")
    p.add_argument('--adjtest', '-t', action='store_true', default=False, help="Perform and adjoint test")
    p.add_argument('--serial', '-s', action='store_true', default=False, help="Run on a single CPU")
    p.add_argument('--ncpus', '-n', default=None)
    p.add_argument('--verbosity', '-v', default='INFO')
    p.add_argument('--rc')
    p.add_argument('--db', required=True)
    p.add_argument('--emis', required=True)
    p.add_argument('--check-footprints', action='store_true', default=True, help="Locate the footprint files and check them. Should be set to False if a `footprints` column is already present in the observation file", dest='checkFootprints')
    p.add_argument('args', nargs=REMAINDER)
    args = p.parse_args(sys.argv[1:])
    # Create the transport model
    model = LumiaFootprintTransport(args.rc, args.db, args.emis, mp=not args.serial, ncpus=args.ncpus)

    if args.checkFootprints:
        model.checkFootprints(model.rcf.rcfGet('path.footprints'))

    if args.forward :
        model.runForward()
        model.obs.save_tar(model.obsfile)

    elif args.adjoint :
        adj = model.runAdjoint()
        logger.debug(f'lumia.transport.default.write(model.emfile={model.emfile})')
        adj.write(model.emfile)

    elif args.adjtest :
        model.adjoint_test()
