#!/usr/bin/env python
from netCDF4 import Dataset
from datetime import datetime
from numpy import zeros, array
from footprints import FootprintFile, SpatialCoordinates, FootprintTransport

# Main ==> move?
import os

from datetime import timedelta
import logging

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


class StiltFootprintFile(FootprintFile):
    def open(self):
        if not self._initialized :
            self.ds = Dataset(self.filename, 'r')
            self.close = self.ds.close

            # List of footprints/times availables:
            sitecode = getattr(self.ds, 'Footprints_receptor').split(';')[0].split(':')[1].strip().lower()
            height = int(getattr(self.ds, 'sampling_height').split()[0])
            times = [x.split('_')[1] for x in self.ds.variables.keys() if x.endswith('val')]
            times = [datetime.strptime(x, '%Y%m%d%H') for x in times]
            self.footprints = {f'{sitecode}.{height:.0f}m.{t.strftime("%Y%m%d-%H%M%S")}' : t for t in times}

            coordinates = SpatialCoordinates(
                lon0 = self.ds.lon_ll+self.ds.lon_res/2.,
                dlon = self.ds.lon_res,
                nlon = self.ds.numpix_x,
                lat0 = self.ds.lat_ll+self.ds.lat_res/2.,
                dlat = self.ds.lat_res,
                nlat = self.ds.numpix_y,
            )

            # Initial setup of the Footprint class
            self.Footprint.lats = coordinates.lats
            self.Footprint.lons = coordinates.lons
            self.Footprint.dlat = coordinates.dlat
            self.Footprint.dlon = coordinates.dlon
            self.Footprint.dt = timedelta(hours=1) 

            self._initialized = True

    def setup(self, coords, origin, dt):
        # Make sure that the requested domain is within that of the footprints
        assert coords <= SpatialCoordinates(obj=self.Footprint) 

        # Re-setup the "Footprint" object:
        self.Footprint.lats = coords.lats
        self.Footprint.lons = coords.lons
        self.Footprint.dlat = coords.dlat
        self.Footprint.dlon = coords.dlon

    def set_origin(self, origin):
        self.origin = origin

    def getFootprint(self, obsid, origin=None):
        time = self.footprints[obsid]

        # Read raw data
        lons = self.ds[time.strftime('ftp_%Y%m%d%H_lon')][:]
        lats = self.ds[time.strftime('ftp_%Y%m%d%H_lat')][:]
        npt = self.ds[time.strftime('ftp_%Y%m%d%H_indptr')][:]
        sensi = self.ds[time.strftime('ftp_%Y%m%d%H_sensi')][:]

        # Select :
        select = array([l in self.Footprint.lats for l in lats])
        select *= array([l in self.Footprint.lons for l in lons])

        # Reconstruct ilats/ilons :
        ilats = [self.Footprint.lats.index(l) for l in lats[select]]
        ilons = [self.Footprint.lons.index(l) for l in lons[select]]

        # Reconstruct itims
        itims = zeros(len(lons))
        prev = 0
        for it, n in enumerate(npt[select]):
            itims[prev:prev+n] = -it

        # Create the footprint
        fp = self.Footprint(origin=time)
        fp.sensi = sensi[select] 
        fp.ilats = ilats
        fp.ilons = ilons
        fp.itims = itims

        if origin is not None :
            fp.shift_origin(origin)
        
        return fp 


if __name__ == '__main__':
    import sys
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
    model = FootprintTransport(args.rc, args.db, args.emis, StiltFootprintFile, mp=not args.serial, checkfile=args.checkfile)

    # Check footprints
    # This is the default behaviour but can be turned of (for instance during the inversion steps)
    # then, the "footprint" and "footprint_valid" columns must be provided
    if args.checkFootprints:
        model.checkFootprints(model.rcf.get('path.footprints'))

    if args.forward :
        model.runForward()
        #model.write(args.obs)

#    if args.adjoint :
#        model.runAdjoint()
    if args.checkfile is not None :
        open(args.checkfile, 'w').close()