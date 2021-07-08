#!/usr/bin/env python

from .regions import region
from numpy import expand_dims, array, squeeze, repeat
import logging

logger = logging.getLogger(__name__)


class Region(region):
    def __init__(self, rcf):
        rname = rcf.get('region')
        region.__init__(
            self,
            name=rname,
            lat0=rcf.get('region.lat0'), lat1=rcf.get('region.lat1'), dlat=rcf.get('region.dlat'),
            lon0=rcf.get('region.lon0'), lon1=rcf.get('region.lon1'), dlon=rcf.get('region.dlon')
        )

class GriddedData:
    def __init__(self, data, reg):
        self.data = data
        self.region = reg

    def regrid(self, newreg, weigh_by_area=False):
        assert newreg <= self.region

        if len(self.data.shape) == 2 :
            # Add a dummy vertical dimension if we deal with surface data
            data = expand_dims(self.data, 0)

        if self.region.dlon == newreg.dlon and self.region.dlat == newreg.dlat :
            data_out = self.crop(newreg)
        elif self.region.dlon > newreg.dlon and self.region.dlat > newreg.dlat :
            data_out = self.refine(newreg, weigh_by_area=weigh_by_area in [True, 'refine'])
        elif self.region.dlon < newreg.dlon and self.region.dlat < newreg.dlat :
            data_out = self.coarsen(newreg, weigh_by_area=weigh_by_area in [True, 'coarsen'])
        else :
            # In theory, there are other cases, but since they are unlikely, I didn't implement them
            logger.error("Requested regridding is not implemented (yet?)")
            raise NotImplementedError

        return data_out

    def refine(self, newreg, weigh_by_area):
        logger.debug("Refine grid, %s weigh by area" % (["don't", "do"][weigh_by_area]))

        # Create an intermediate grid, with the same resolution as the new grid and the same boundaries as the original one:
        regX = region(lat0=self.region.latmin, lat1=self.region.latmax, dlat=newreg.dlat,
                      lon0=self.region.lonmin, lon1=self.region.lonmax, dlon=newreg.dlon
        )

        # Make sure that the new data can be generated just by dividing the original data
        assert self.region.dlat % newreg.dlat == 0
        assert self.region.dlon % newreg.dlon == 0
        assert newreg.latmin in regX.lat0
        assert newreg.lonmin in regX.lon0

        ratio_lats = self.region.dlat / newreg.dlat
        ratio_lons = self.region.dlon / newreg.dlon

        # Work on a copy of the data
        data = self.data.copy()

        # convert to unit/m2 for the conversion
        if weigh_by_area:
            data /= self.region.area[None, :, :]

        # refine:
        data = repeat(data, int(ratio_lats), axis=1)
        data = repeat(data, int(ratio_lons), axis=2)

        # Concert back to the original unit
        if weigh_by_area:
            data *= regX.area[None, :, :]

        # "data" is on a different grid than "self.data", so we need to instantiate a new "GriddedData" object
        return GriddedData(data, regX).crop(newreg)

    def coarsen(self, newreg, weigh_by_area):
        """
        Coarsen a 3D array by aggregating pixels along the lat and lon axis.
        """
        logger.debug("Coarsen grid, %s weigh by area" % (["do", "don't"][weigh_by_area]))

        # Create an intermediate grid, with the same resolution as the new grid and the same boundaries as the original one:
        regX = region(lat0=self.region.latmin, lat1=self.region.latmax, dlat=newreg.dlat,
                      lon0=self.region.lonmin, lon1=self.region.lonmax, dlon=newreg.dlon
        )

        # Make sure that the new data can be generated just by aggregating the original data
        assert newreg.dlat % self.region.dlat == 0
        assert newreg.dlon % self.region.dlon == 0
        assert newreg.latmin in regX.lat0
        assert newreg.lonmin in regX.lon0

        ratio_lats = int(newreg.dlat / self.region.dlat)
        ratio_lons = int(newreg.dlon / self.region.dlon)

        # work on a copy of the data
        data = self.data.copy()

        # convert to unit/m2 for the conversion
        if weigh_by_area:
            data *= self.region.area[None, :, :]
        nlev = data.shape[0]

        # Coarsen:
        data = data.reshape(nlev, self.region.nlat, regX.nlon, ratio_lons).sum(3)
        data = data.reshape(nlev, regX.nlat, ratio_lats, regX.nlon).sum(2)

        # Convert back to the original unit:
        if weigh_by_area:
            data /= regX.area[None, :, :]

        # "data" is on a different grid than "self.data", so we need to instantiate a new "GriddedData" object
        return GriddedData(data, regX).crop(newreg)

    def crop(self, newreg):
        logger.debug("crop grid")

        # ensure that the new grid is a subset of the old one
        assert all([l in self.region.lats for l in newreg.lats])
        assert all([l in self.region.lons for l in newreg.lons])

        # crop 
        slat = array([l in newreg.lats for l in self.region.lats])
        slon = array([l in newreg.lons for l in self.region.lons])

        # return. Remove the dummy vertical dimension if possible
        return squeeze(self.data[:, slat, :][:, :, slon])