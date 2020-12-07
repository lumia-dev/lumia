#!/usr/bin/env python

import os
from glob import glob
import logging
from datetime import datetime, timedelta
from numpy import array, nan, meshgrid, nonzero
from netCDF4 import Dataset, chartostring
from transport.footprints import FootprintFile, SpatialCoordinates, FootprintTransport

logger = logging.getLogger(os.path.basename(__file__))


class FlexpartFootprintFile(FootprintFile):
    def __init__(self, filename, silent=False, cache=False):
        super().__init__(filename, silent, cache)
        self.open()
        self.shift_t = 0

    def open(self):
        if not self._initialized :
            with Dataset(self.filename, 'r') as ds :
                # List of footprints/times available:
                self.footprints = [x.strip() for x in chartostring(ds['RELCOM'][:])]

                # Store time and space coordinates :
                self.coordinates = SpatialCoordinates(
                    lats = ds['latitude'][:],
                    lons = ds['longitude'][:]
                )
                self.dt = timedelta(seconds=0.+abs(ds.loutaver))
                self.origin = datetime.strptime(ds.iedate+ds.ietime, '%Y%m%d%H%M%S')-self.dt#/2

                # Copy them to the Footprint class 
                self.Footprint.lats = self.coordinates.lats
                self.Footprint.lons = self.coordinates.lons
                self.Footprint.dlat = self.coordinates.dlat
                self.Footprint.dlon = self.coordinates.dlon
                self.Footprint.dt = self.dt

                # Construct a grid of indices:
                nt, nlat, nlon = ds.dimensions['time'].size, self.coordinates.nlat, self.coordinates.nlon
                self.grids = [x.reshape(-1) for x in meshgrid(range(nt), range(nlat), range(nlon), copy=False)]

                self._initialized = True

    def setup(self, coords, origin, dt):
        assert self.coordinates == coords
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

        with Dataset(self.filename, 'r') as ds :
            sensi = ds['spec001_mr'][0, self.footprints.index(obsid), :, 0, :, :].reshape(-1)
            select = nonzero(sensi)
            fp.itims = -self.grids[0][select] + self.shift_t
            fp.ilats = self.grids[1][select]
            fp.ilons = self.grids[2][select]
            fp.sensi = sensi[select]
            fp.origin = self.origin
            valid = sum(fp.sensi) > 0
            if not valid :
                logger.info(f"No usable data found in footprint {obsid}")
                try :
                    logger.info(f"footprint covers the period {fp.itime_to_times(fp.itims.min())} to {fp.itime_to_times(fp.itims.max())}")
                except ValueError :
                    logger.info(f"footprint is empty!")
                    pass
            return fp
            
    def close(self):
        pass


class FlexpartFootprintTransport(FootprintTransport):
    def __init__(self, rcf, obs, emis=None, mp=None, checkfile=None):
        super().__init__(rcf, obs, emis, FlexpartFootprintFile, mp, checkfile)
        self.footprint_files = {} 

    def checkFiles(self, path):
        """
        With the native FLEXPART format, there is no way to know what footprint is in what files,
        without actually opening the files. So here we open them:
        """
        folders = [os.path.dirname(x) for x in glob(os.path.join(path, '*', 'flexpart.ok'))]
        gridfiles = [glob(os.path.join(f, 'grid_time_??????????????.nc')) for f in folders]
        flist = []
        for igf, gf in enumerate(gridfiles) :
            if len(gf) == 1 :
                flist.append(gf[0])
            else :
                # Take the gridfile that is both more recent than the header and older than the okfile
                ages = array(flist.sort(key=os.path.getctime))
                agemax = os.path.getctime(os.path.join(folders[igf], 'flexpart.ok'))
                agemin = os.path.getctime(os.path.join(folders[igf], 'header'))
                gf = array(gf)[(ages > agemin)*(ages < agemax)]
                assert len(gf) == 0
                flist.append(gf)

        self.footprint_ids = {}
        for fname in flist :
            fpf = FlexpartFootprintFile(fname)
            self.footprint_files[fname] = fpf
            self.footprint_ids = {**self.footprint_ids, **dict.fromkeys(fpf.footprints, fpf.filename)}

    def genFileNames(self):
        """
        This method is only really needed for writing files in the format implemented by this 
        class (i.e. FLEXPART grid_time format). Currently writing files in FLEXPART format is just
        not implemented.
        """
        raise NotImplementedError

    def checkFootprints(self, path, archive=None):
        obsids = [f'{o.code}.{o.height:.0f}m.{o.time.strftime("%Y%m%d%H%M%S")}' for o in self.obs.observations.itertuples()]
        self.obs.observations.loc[:, 'obsid'] = obsids
        self.checkFiles(path)
        self.obs.observations.loc[:, 'footprint'] = [self.footprint_ids.get(oid, nan) for oid in self.obs.observations.obsid.values]

    def get(self, filename):
        return self.footprint_files[filename]