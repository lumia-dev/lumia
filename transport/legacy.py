#!/usr/bin/env python

import os
import logging
from h5py import File
from datetime import datetime
from footprints import FootprintTransport, FootprintFile, SpatialCoordinates

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

    def calc_index(self, origin):
        itim = (self.start-origin)/self.dt
        assert itim%1 == 0
        return int(itim)

    def __lt__(self, other):
        assert isinstance(other, self.__class__)
        assert self.dt == other.dt
        return self.start < other.start


class LegacyFootprintFile(FootprintFile):
    def read(self):
        self.footprints = {}
        with File(self.filename, 'r') as fid :
            for k, v in fid.items():
                if k not in ['latitudes', 'longitudes']:
                    obstime = datetime.strptime(k, '%Y%m%d%H%M%S')
                    obsid = f'{self.sitecode}.{self.height}m.{obstime.strftime("%Y%m%d-%H%M")}'
                    self.footprints[obsid] = k
            
            # Store time and space coordinates :
            sitecode, height, month = self.filename.split('.')[:3]

            self.coordinates = SpatialCoordinates(
                lats = fid['latitudes'][:],
                lons = fid['longitudes'][:]
            )
            self.origin = datetime.strptime(month, '%Y-%m')
            self.intervals = []
            for k, v in h5group.items():
                self.intervals.append(Interval(k))
                self.dt = self.intervals[0].dt

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
        assert self.coordinates == coords
        assert self.Footprint.dt == dt, print(self.Footprint.dt, dt)

    def getFootprint(self, obsid, origin=None):
        if origin is None :
            origin = self.origin

        fp = self.Footprint()
        with File(self.filename, 'r') as fid :
            h5group = fid[self.footprints[obsid]]
            for interv in sorted(self.intervals)[::-1]:
                itim = interv.calc_index(origin)
                resp = h5group[interv.key]['resp'][:]
                fp.ilats.extend(h5group[interv.key]['ilats'][:].astype(int16))
                fp.ilons.extend(h5group[interv.key]['ilons'][:].astype(int16))
                fp.itims.extend(repeat(itim, resp.shape[0]).astype(int16))
                fp.sensi.extend(resp)
                fp.origin = origin
                valid = sum(fp.sensi) > 0
                if not valid :
                    logger.info(f"No usable data found in footprint {obsid}")
                    logger.info(f"footprint covers the period {fp.itime_to_times(fp.itims.min())} to {fp.itime_to_times(fp.itims.max())}")
                return fp

    def writeFootprints(self, obs, footprint):
        raise NotImplementedError


class LegacyFootprintTransport(FootprintTransport):
    def __init__(self, rcf, obs, emfile=None, mp=False, checkfile=None):
        super().__init__(rcf, obs, emfile, LegacyFootprintFile, mp, checkfile)

    def getFileNames(self):
        return [f'{o.site.lower()}.{o.height:.0f}m.{o.time.strftime('%Y-%m')}.hdf' for o in self.obs.observations.itertuples()]

    def checkFootprints(self, path, archive=None):
        cache = Archive(path, parent=Archive(archive))
        fnames = array(self.genFileNames())
        exists = array([cache.get(f, fail=False) for f in tqdm(self.genFileNames(), desc="Check footprints")])
        fnames[~exists] = nan
        self.obs.observations.loc[:, 'footprint'] = fnames

        # Construct the obs ids:
        obsids = [f'{o.site.lower()}.{o.height:.0f}m.{o.time.to_pydatetime().strftime("%Y%m%d-%H%M%S")}' for o in self.obs.observations.itertuples()]
        self.obs.observations.loc[:, 'obsid'] = obsids


if __name__ == '__main__':
    import sys
    from argparse import ArgumentParser, REMAINDER

    logger = logging.getLogger(os.path.basename(__file__))

    p = ArgumentParser()
    p.add_argument('--forward', '-f', action='store_true', default=False, help="Do a forward run")
    p.add_argument('--adjoint', '-a', action='store_true', default=False, help="Do an adjoint run")
    p.add_argument('--serial', '-s', action='store_true', default=False, help="Run on a single CPU")
    p.add_argument('--verbosity', '-v', default='INFO')
    p.add_argument('--rc')
    p.add_argument('--db', required=True)
    p.add_argument('--emis', required=True)
    p.add_argument('--checkfile', '-c')
    p.add_argument('--check-footprints', action='store_true', default=True, help="Locate the footprint files and check them. Should be set to False if a `footprints` column is already present in the observation file", dest='checkFootprints')
    p.add_argument('args', nargs=REMAINDER)
    args = p.parse_args(sys.argv[1:])

    logger.setLevel(args.verbosity)
    logger.info('test logger')
    logger.debug('test logger')
    logger.warning('test logger')

    # Create the transport model
    model = LegacyFootprintTransport(args.rc, args.db, args.emis, mp=not args.serial, checkfile=args.checkfile)

    if args.checkFootprints:
        model.checkFootprints(model.rcf.get('path.footprints'))

    if args.forward :
        model.runForward()

    if args.adjoint :
        model.runAdjoint()