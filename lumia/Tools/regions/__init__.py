#!/usr/bin/env python

from numpy import *

class region:
    def __init__(self, name=None, longitudes=None, latitudes=None, lon0=None, lon1=None, lat0=None, lat1=None, dlon=None, dlat=None, nlon=None, nlat=None):
        if name in ['eur1x1', 'eur100x100']:
            self.lonmin = -25.
            self.lonmax = 45.
            self.latmin = 23.
            self.latmax = 83.
            self.dlon = 1.
            self.dlat = 1.
        if name in ['glb6x4','glb600x400'] :
           self.lonmin = -180
           self.lonmax = 180
           self.latmin = -90
           self.latmax = 90
           self.dlon = 6.
           self.dlat = 4.
        if name in ['glb1x1','glb100x100'] :
            self.lonmin = -180
            self.lonmax = 180
            self.latmin = -90
            self.latmax = 90
            self.dlon = 1.
            self.dlat = 1.
        self.name = name
        if iterable(longitudes) : self.lons = longitudes
        if iterable(latitudes) : self.lats = latitudes
        if lon0 is not None : self.lonmin = lon0
        if lat0 is not None : self.latmin = lat0
        if lon1 is not None : self.lonmax = lon1
        if lat1 is not None : self.latmax = lat1
        if dlon is not None : self.dlon = dlon
        if dlat is not None : self.dlat = dlat
        if not None in [nlon, lon0, lon1]:
            self.longitudes = linspace(lon0, lon1, nlon+1)
            self.longitudes = (self.longitudes[1:]+self.longitudes[:-1])/2.
            if dlon is None : self.dlon = (lon1-lon0)/nlon
        if not None in [nlat, lat0, lat1]:
            self.latitudes = linspace(lat0, lat1, nlat+1)
            self.latitudes = (self.latitudes[1:]+self.latitudes[:-1])/2.
            if dlat is None : self.dlat = (lat1-lat0)/nlat
        self.setup()

    def zeros(self):
        return zeros((self.nlat, self.nlon))

    def setup(self):
        if hasattr(self, 'lonmin') and hasattr(self, 'lonmax') and hasattr(self, 'latmin') and hasattr(self, 'latmax') and hasattr(self, 'dlon') and hasattr(self, 'dlat'):
            self.nlon = int(floor((self.lonmax-self.lonmin)/self.dlon))
            self.nlat = int(floor((self.latmax-self.latmin)/self.dlat))
            self.lon0 = linspace(self.lonmin, self.lonmax-self.dlon, self.nlon)
            self.lon1 = self.lon0+self.dlon
            self.lons = self.lon0+self.dlon/2.
            self.lat0 = linspace(self.latmin, self.latmax-self.dlat, self.nlat)
            self.lat1 = self.lat0+self.dlat
            self.lats = self.lat0+self.dlat/2.
        elif hasattr(self, 'lons') and hasattr(self, 'lats'):
            dlon = diff(self.lons)
            dlat = diff(self.lats)
            if len(unique(dlon)) == 1 and len(unique(dlat)) == 1:
                self.dlon = unique(dlon)
                self.dlat = unique(dlat)
            else :
                if amax(dlon-around(dlon, decimals=2)) < 1.e-5 : self.dlon=around(dlon, decimals=2)[0]
                if amax(dlat-around(dlat, decimals=2)) < 1.e-5 : self.dlat=around(dlat, decimals=2)[0]
            self.lon0 = self.lons-self.dlon/2.
            self.lon1 = self.lons+self.dlon/2.
            self.lat1 = self.lats+self.dlat/2.
            self.lat0 = self.lats-self.dlat/2.
            self.latmin, self.latmax = self.lat0.min(), self.lat1.max()
            self.lonmin, self.lonmax = self.lon0.min(), self.lon1.max()
            self.nlon = size(self.lons)
            self.nlat = size(self.lats)
        try :
            self.calc_area()
        except MemoryError :
            print "Too much pixels, area array won't be created (MemoryError)"
        if self.lonmax-self.lonmin == 360 and self.latmax-self.latmin == 180 :
            self.isglobal = True
        else :
            self.isglobal = False

    def GetIndicesFromLons(self, lons):
        try:
            ndat = len(lons)
        except TypeError:
            lons = [lons]
        if type(lons) in [list, tuple]: lons = array(lons)
        ilons = array([int((x - self.lonmin) / self.dlon) for x in lons])
        ilons[ilons < 0] = -1
        ilons[ilons >= self.nlon] = -1
        return ilons

    def GetIndicesFromLats(self, lats):
        try:
            ndat = len(lats)
        except TypeError:
            lats = [lats]
        if type(lats) in [list, tuple]: lats = array(lats)
        ilats = array([int((x - self.latmin) / self.dlat) for x in lats])
        ilats[ilats < 0] = -1
        ilats[ilats >= self.nlat] = -1
        return ilats

    def GetIndicesFromLonsLats(self, lons, lats):
        lons = self.GetIndicesFromLons(lons)
        lats = self.GetIndicesFromLats(lats)
        return [(x, y) for (x, y) in zip(lons, lats)]

    def basemap(self,**kwargs):
        from mpl_toolkits.basemap import Basemap
        m1 = Basemap(llcrnrlat=self.latmin, llcrnrlon=self.lonmin, urcrnrlat=self.latmax, urcrnrlon=self.lonmax, **kwargs)
        return m1

    def get_land_mask(self, refine_factor=1):
        """ Returns the proportion (from 0 to 1) of land in each pixel
        By default, if the type (land or ocean) of the center of the pixel determines the land/ocean type of the whole pixel.
        If the optional argument "refine_factor" is > 1, the land/ocean mask is first computed on the refined grid, and then averaged on the region grid (accounting for grid box area differences)"""
        assert isinstance(refine_factor, int), "refine_factor must be an integer"
        r2 = region(lon0=self.lonmin, lon1=self.lonmax, lat0=self.latmin, lat1=self.latmax, dlon=self.dlon/refine_factor, dlat=self.dlat/refine_factor)
        r2.calc_area()
        m1 = r2.basemap()
        lsm = zeros((r2.nlat, r2.nlon))
        for ilat, lat in enumerate(r2.lats):
            for ilon, lon in enumerate(r2.lons):
                lsm[ilat, ilon] = 1. if m1.is_land(lon, lat) else 0.
        lsm_coarse = zeros((self.nlat, self.nlon))
        for ilat in xrange(self.nlat):
            for ilon in xrange(self.nlon):
                lsm_coarse[ilat, ilon] = average(lsm[ilat*refine_factor:(ilat+1)*refine_factor, ilon*refine_factor:(ilon+1)*refine_factor], weights=r2.area[ilat*refine_factor:(ilat+1)*refine_factor, ilon*refine_factor:(ilon+1)*refine_factor])
        return lsm_coarse

    def plotGrid(self, color='cyan'):
        m1 = self.basemap()
        m1.drawcoastlines()
        for lon in self.lon0 : axvline(lon, c=color)
        for lat in self.lat0 : axhline(lat, c=color)

    def calc_area(self):
        #self.area_m2 = calc_area_m2(self.lons0, self.lons1, self.lats0, self.lats1)
        R_e = 6378100.0 # Radius of Earth in meters
        dlon_rad = self.dlon*pi/180.
        area = zeros((self.nlat+1, self.nlon), float64)
        lats = (pi/180.)*linspace(self.latmin, self.latmax, self.nlat+1)
        for ilat, lat in enumerate(lats):
            area[ilat,:] = R_e**2*dlon_rad*sin(lat)
        self.area = diff(area, axis=0)

    def is_contained_by(self, region):
        return True if self.lonmin >= region.lonmin and self.latmin >= region.latmin and self.lonmax <= region.lonmax and self.latmax <= region.latmax else False

    def contains(self, region):
        return region.is_contained_by(self)

    def is_the_same_as(self, region):
        return self.lonmin == region.lonmin and self.lonmax == region.lonmax and self.latmin == region.latmin and self.latmax == region.latmax and self.dlat == region.dlat and self.dlon == region.dlon

    def containsPoint(self, lon, lat):
        if lon >= self.lonmin and lon <= self.lonmax and lat >= self.latmin and lat <= self.latmax : return True
        return False

    def __eq__(self, other):
        if type(other) == type(self) :
            return self.is_the_same_as(other)
        else :
            return False

    def __ne__(self, other):
        return True-self.__eq__(other)

    def __lt__(self, other):
        return self.is_contained_by(other)

    def __le__(self, other):
        return self.is_contained_by(other)

    def __gt__(self, other):
        return self.contains(other)

    def __ge__(self, other):
        return self.contains(other)

    def __repr__(self):
        return self.name

    def describe(self):
        """
        returns a string describing the main characteristics of the region
        """
        line = ''
        if self.name is not None : line = '          <m>name: <w><i>%s\n'%self.name
        latmin = '%.2fN'%self.latmin if self.latmin >= 0 else '%.2fS'%abs(self.latmin)
        latmax = '%.2fN'%self.latmax if self.latmax >= 0 else '%.2fS'%abs(self.latmax)
        lonmin = '%.2fE'%self.lonmin if self.lonmin >= 0 else '%.2fW'%abs(self.lonmin)
        lonmax = '%.2fE'%self.lonmax if self.lonmax >= 0 else '%.2fW'%abs(self.lonmax)
        line += '    <m>boundaries:<w> <i>%s -- %s; %s -- %s\n'%(latmin, latmax, lonmin, lonmax)
        line += '  <m>lat interval: <w><i>%.1f\n'%self.dlat
        line += '  <m>lon interval: <w><i>%.1f'%self.dlon
        return line

def surfaceAreaGrid(lat_specs, lon_specs):
    lat_beg, lat_end, lat_div = lat_specs
    lon_beg, lon_end, lon_div = lon_specs
    R_e = 6378100.0 # Radius of Earth in meters
    dLon = (pi/180.) * (lon_end-lon_beg)/lon_div
    # Construct empty array of the right size that will later hold the grid areas
    # We'll construct an array of R_e^2 * dLon * sin(lat) for each latitude, then take the difference between
    # successive rows later. Hence now the array will have one extra row.
    dS = zeros((lat_div+1, lon_div), float64)
    Lats = (pi/180.) * linspace(lat_beg, lat_end, lat_div+1)
    # If lat_specs == (-30,60,45), then Lats will be (pi/180) * (-30., -28., -26., ... 58., 60.)
    for i, lat in enumerate(Lats):
        dS[i] = R_e * R_e * dLon * sin(lat)
    dS = diff(dS, axis=0)
    # dS is now (lat_div x lon_div)
    return dS

