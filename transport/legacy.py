#!/usr/bin/env python

from calendar import c
import sys
import os
import logging
from h5py import File
from datetime import datetime, timedelta
from footprints import FootprintTransport, FootprintFile, SpatialCoordinates
from archive import Archive
from numpy import int16, repeat, array, nan
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


class LegacyFootprintFile(FootprintFile):
    def read(self):
        # self.footprints = {}
        # self.sitecode, self.height, month = os.path.basename(self.filename).split('.')[:3]
        # logger.debug(f"opening file {self.filename}, with sitecode {self.sitecode}, height {self.height} and month {month}")
        # intervals = []
        # self.data = {}
        # with File(self.filename, 'r') as fid :
        #     for k, v in fid.items():
        #         if k not in ['latitudes', 'longitudes']:
        #             import pdb; pdb.set_trace()
        #             obstime = datetime.strptime(k, '%Y%m%d%H%M%S')
        #             obsid = f'{self.sitecode}.{self.height}.{obstime.strftime("%Y%m%d-%H%M%S")}'
        #             self.footprints[obsid] = k
        #             self.data[obsid] = {}

        #             for o in fid[k].keys():
        #                 intv = Interval(o)
        #                 if intv.valid :
        #                     intervals.append(intv)
        #                     coords_with_s = 'ilats' in fid[k][o]
        #             #        self.data[obsid][o] = {
        #             #            'ilats': fid[k][o]['ilats'][:].astype(int16),
        #             #            'ilons': fid[k][o]['ilons'][:].astype(int16),
        #             #            'resp': fid[k][o]['resp'][:]
        #             #        }

        #     # Store time and space coordinates :
        #     self.coordinates = SpatialCoordinates(
        #         lats = fid['latitudes'][:],
        #         lons = fid['longitudes'][:]
        #     )

        #     self.origin = datetime.strptime(month, '%Y-%m')
        #     if len(intervals) == 0 :
        #         logger.error(f"Footprint file {self.filename} is empty. Delete it and run again")
        #         sys.exit()
        #     self.dt = intervals[0].dt

        #     # Copy them to the Footprint class 
        #     self.Footprint.lats = self.coordinates.lats
        #     self.Footprint.lons = self.coordinates.lons
        #     self.Footprint.dlat = self.coordinates.dlat
        #     self.Footprint.dlon = self.coordinates.dlon
        #     self.Footprint.dt = self.dt
        #     self.Footprint.origin = self.origin

        #     # read if the coordinate fields are ilat/ilon or ilats/ilons:
        #     self.ilat_field = 'ilats' if coords_with_s else 'ilat'
        #     self.ilon_field = 'ilons' if coords_with_s else 'ilon'

        #     self._initialized = True
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
        except :
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

    def close(self):
        pass

    def setup(self, coords, origin, dt):
        # import pdb; pdb.set_trace()
        # assert self.coordinates == coords
        # assert self.Footprint.dt == dt, print(self.Footprint.dt, dt)
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
        # fp = self.Footprint()
        # with File(self.filename, 'r') as fid :
        #     h5group = fid[self.footprints[obsid]]
        # #h5group = self.data[obsid]

        #     intervals = []
        #     for k, v in h5group.items():
        #         interv = Interval(k)
        #         if interv.valid :
        #             intervals.append(interv)

        #     for interv in sorted(intervals)[::-1]:
        #         itim = interv.calc_index(self.origin)
        #         resp = h5group[interv.key]['resp'][:]
        #         fp.ilats.extend(h5group[interv.key]['ilat'][:].astype(int16))
        #         fp.ilons.extend(h5group[interv.key]['ilon'][:].astype(int16))
        #         fp.itims.extend(repeat(itim, resp.shape[0]).astype(int16))
        #         fp.sensi.extend(resp)

        # fp.ilats = array(fp.ilats)
        # fp.ilons = array(fp.ilons)
        # fp.itims = array(fp.itims)
        # fp.sensi = array(fp.sensi)
        # fp.intervals = intervals

        # if origin is not None :
        #     fp.shift_origin(origin) 

        # valid = sum(fp.sensi) > 0
        # if not valid :
        #     logger.info(f"No usable data found in footprint {obsid}")
        #     #logger.info(f"footprint covers the period {fp.itime_to_times(fp.itims.min())} to {fp.itime_to_times(fp.itims.max())}")
        # return fp
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

    def writeFootprints(self, obs, footprint):
        raise NotImplementedError


class LegacyFootprintTransport(FootprintTransport):
    def __init__(self, rcf, obs, emfile=None, atmdel=None, mp=False, checkfile=None, ncpus=None):
        super().__init__(rcf, obs, emfile, atmdel, LegacyFootprintFile, mp, checkfile, ncpus)

    def genFileNames(self, tr):
        return [f'{o.site}.{o.height:.0f}m.{o.time.strftime("%Y-%m")}.hdf' for o in self.obs.observations.loc[self.obs.observations.tracer==tr].itertuples()]

    def checkFootprints(self, path, archive=None):

        for tr, p in path.items():
            cache = Archive(p, parent=Archive(archive))

            fnames = array(self.genFileNames(tr))
            exists = array([cache.get(f, dest=p, fail=False) for f in tqdm(self.genFileNames(tr), desc=f"Checking footprints for {tr}")])
            fnames = array([os.path.join(p, fname) for fname in fnames])
            self.obs.observations.loc[self.obs.observations.tracer == tr, 'footprint'] = fnames
            self.obs.observations.loc[self.obs.observations.tracer==tr].loc[~exists, 'footprint'] = nan

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
    p.add_argument('--rc')
    p.add_argument('--db', required=True)
    p.add_argument('--emis', required=True) # TODO:
    p.add_argument('--atmdel')#, required=True) # TODO:
    # p.add_argument('--ffdel', required=True) # TODO:
    p.add_argument('--no-check-footprints', action='store_false', default=True, help="Locate the footprint files and check them. Should be set to False if a `footprints` column is already present in the observation file", dest='checkFootprints')
    p.add_argument('args', nargs=REMAINDER)
    args = p.parse_args(sys.argv[1:])

    logger.setLevel(args.verbosity)
    logger.info('test logger')
    logger.debug('test logger')
    logger.warning('test logger')

    # Create the transport model
    model = LegacyFootprintTransport(args.rc, args.db, args.emis, args.atmdel, mp= not args.serial, ncpus=args.ncpus) #mp=not args.serial

    if args.checkFootprints: 
        ftp_path = {}
        trlist = model.rcf.get('obs.tracers') if isinstance(model.rcf.get('obs.tracers'), list) else [model.rcf.get('obs.tracers')]
        for tr in trlist:
            ftp_path[tr] = model.rcf.get(f'path.{tr}.footprints')
        model.checkFootprints(ftp_path)
    model.genObsIDs()

    if args.forward :
        model.runForward()

    elif args.adjoint :
        model.runAdjoint()

    elif args.adjtest :
        model.adjoint_test()