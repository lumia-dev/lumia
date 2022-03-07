#!/usr/bin/env python
from netCDF4 import Dataset
from datetime import datetime
from numpy import zeros, array, nan, unique
from transport.footprints import FootprintFile, SpatialCoordinates, FootprintTransport
from archive import Archive
from tqdm import tqdm
from lumia.timers import Timer
from multiprocessing import Pool

# Main ==> move?
import os

from datetime import timedelta
from loguru import logger

class StiltFootprintFile(FootprintFile):
    def read(self):
        if not os.path.exists(self.filename) :
            return False
        self.ds = Dataset(self.filename, 'r')
        self.close = self.ds.close

        # List of footprints/times availables:
        sitecode = getattr(self.ds, 'Footprints_receptor').split(';')[0].split(':')[1].strip().lower()
        height = int(getattr(self.ds, 'sampling_height').split()[0])
        times = [x.split('_')[1] for x in self.ds.variables.keys() if x.endswith('val')]
        times = [datetime.strptime(x, '%Y%m%d%H') for x in times]

        # store the times for which valid footprints exist (if there is any "nan", mask.prod() will be True so the footprint won't be used)
        # read only the indptr value for checking the nans as it is the smallest of the four variables
        self.footprints = {f'{sitecode}.{t.strftime("%Y%m%d-%H%M%S")}' : t for t in times if not self.ds[t.strftime('ftp_%Y%m%d%H_indptr')][:].mask.prod()}

        # Store time and space coordinates
        self.coordinates = SpatialCoordinates(
            lon0 = self.ds.lon_ll+self.ds.lon_res/2.,
            dlon = self.ds.lon_res,
            nlon = int(self.ds.numpix_x),
            lat0 = self.ds.lat_ll+self.ds.lat_res/2.,
            dlat = self.ds.lat_res,
            nlat = int(self.ds.numpix_y),
        )
        self.dt = timedelta(hours=1)
        
        # Initial setup of the Footprint class
        self.Footprint.lats = self.coordinates.lats
        self.Footprint.lons = self.coordinates.lons
        self.Footprint.dlat = self.coordinates.dlat
        self.Footprint.dlon = self.coordinates.dlon
        self.Footprint.dt = self.dt
        return True

    def setup(self, coords, origin, dt):
        # Make sure that the requested domain is within that of the footprints
        assert coords <= self.coordinates 

        # Re-setup the "Footprint" object:
        self.Footprint.lats = coords.lats.tolist()
        self.Footprint.lons = coords.lons.tolist()
        self.Footprint.dlat = coords.dlat
        self.Footprint.dlon = coords.dlon
        self.Footprint.lon0 = self.Footprint.lons[0]-self.Footprint.dlon/2.
        self.Footprint.lat0 = self.Footprint.lats[0]-self.Footprint.dlat/2.

        self.origin = origin

    def getFootprint(self, obsid, origin=None):
        time = self.footprints[obsid]

        # Read raw data
        # The lat/lon coordinates in the file indicate the ll corner, but we want the center. So add half steps
        lons = self.ds[time.strftime('ftp_%Y%m%d%H_lon')][:] + self.Footprint.dlon/2.
        lats = self.ds[time.strftime('ftp_%Y%m%d%H_lat')][:] + self.Footprint.dlat/2.
        npt = self.ds[time.strftime('ftp_%Y%m%d%H_indptr')][:]
        sensi = self.ds[time.strftime('ftp_%Y%m%d%H_val')][:]

        # Select :
        select = (lats >= self.Footprint.lats[0]) * (lats <= self.Footprint.lats[-1]) 
        select *= (lons >= self.Footprint.lons[0]) * (lons <= self.Footprint.lons[-1])

        # Reconstruct ilats/ilons :
        ilats = (lats[select]-self.Footprint.lat0)/self.Footprint.dlat
        ilons = (lons[select]-self.Footprint.lon0)/self.Footprint.dlon
        ilats = ilats.astype(int)
        ilons = ilons.astype(int)

        # Reconstruct itims
        itims = zeros(len(lons))
        prev = 0
        for it, n in enumerate(npt[1:]):
            itims[prev:prev+n] = -it
            prev = n
        itims = itims.astype(int)[select]

        # Create the footprint
        fp = self.Footprint(origin=time)
        fp.sensi = sensi[select] 
        fp.ilats = ilats
        fp.ilons = ilons
        fp.itims = itims

        if origin is not None :
            fp.shift_origin(origin)
        
        return fp 


def loadFp(fname):
    fp = StiltFootprintFile(fname)
    if fp.read():
        return {'filename':fp.filename, 'footprints':fp.footprints}
    else :
        return {'filename':fname}


class StiltFootprintTransport(FootprintTransport):
    def __init__(self, rcf, obs, emfile, mp, checkfile):
        super().__init__(rcf, obs, emfile, StiltFootprintFile, mp, checkfile)

    def genFileNames(self):
        return [f'footprint_{o.sitecode_CSR}_{o.time.year}{o.time.month:02.0f}.nc' for o in self.obs.observations.itertuples()]

    def checkFootprints(self, path, archive=None, force=False):
        # skip the whole checkFootprint thingie if the columns are already in the database (to save time)
        if 'footprint' in self.obs.observations.columns and 'obsid' in self.obs.observations.columns and not force:
            return

        cache = Archive(path, parent=Archive(archive))

        fnames = array(self.genFileNames())
        exists = array([cache.get(f, dest=path, fail=False) for f in tqdm(self.genFileNames(), desc="Check footprints")])
        fnames = array([os.path.join(path, fname) for fname in fnames])
        self.obs.observations.loc[:, 'footprint'] = fnames
        self.obs.observations.loc[~exists, 'footprint'] = nan

        # Construct the obs ids:
        obsids = [f'{o.sitecode_CSR.lower()}.{o.time.to_pydatetime().strftime("%Y%m%d-%H%M%S")}' for o in self.obs.observations.loc[exists].itertuples()]
        self.obs.observations.loc[exists, 'obsid'] = obsids

        # Check if the footprints actually exist in the files:
        with Pool() as pp :
            fpts = tqdm(list(pp.imap(loadFp, unique(fnames), chunksize=1)), total=len(unique(fnames)))
        
        for fp in fpts :
            if 'footprints' in fp :
                filename = fp['filename']
                oids = self.obs.observations.loc[self.obs.observations.footprint == filename, 'obsid'].values
                invalid = array([o not in fp['footprints'] for o in oids])
                ind = self.obs.observations.loc[(self.obs.observations.footprint == filename)].loc[invalid].index
                self.obs.observations.loc[ind, 'footprint'] = nan


if __name__ == '__main__':
    import sys
    from argparse import ArgumentParser, REMAINDER

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

    # Create the transport model
    model = StiltFootprintTransport(args.rc, args.db, args.emis, mp=not args.serial, checkfile=args.checkfile)

    # Check footprints
    # This is the default behaviour but can be turned of (for instance during the inversion steps)
    # then, the "footprint" and "footprint_valid" columns must be provided
    if args.checkFootprints:
        model.checkFootprints(model.rcf.get('path.footprints'))

    if args.forward :
        model.runForward()
        #model.write(args.obs)

    if args.adjoint :
        model.runAdjoint()

    if args.checkfile is not None :
        open(args.checkfile, 'w').close()