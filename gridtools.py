#!/usr/bin/env python

from dataclasses import dataclass, field
from numpy import meshgrid, ndarray, linspace, pi, zeros, float64, sin, diff, searchsorted, array, pad, moveaxis, arange, typing, put_along_axis, ceil, append
from types import SimpleNamespace
from loguru import logger
from tqdm.autonotebook import tqdm
from h5py import File
from cartopy.io import shapereader
from shapely.geometry import Point
from shapely.ops import unary_union
from typing import List, Union, Tuple
from shapely.prepared import prep
import xarray as xr
from numpy.typing import NDArray


class LandMask:
    def __init__(self) -> None:
        self.initialized = False

    def init(self):
        land_shp_fname = shapereader.natural_earth(resolution='50m', category='physical', name='land')
        land_geom = unary_union(list(shapereader.Reader(land_shp_fname).geometries()))
        self.land = prep(land_geom)
        self.initialized = True
    
    def is_land(self, lat: float, lon: float) -> bool :
        if not self.initialized :
            self.init()
        return self.land.contains(Point(lat, lon))

    def get_mask(self, grid):
        lsm = zeros((grid.nlat, grid.nlon))
        for ilat, lat in enumerate(grid.latc):
            for ilon, lon in enumerate(grid.lonc):
                lsm[ilat, ilon] = self.is_land(lon, lat)
        return GriddedData(lsm.astype(float), grid, density=True)


land_mask = LandMask()


@dataclass
class RectiLinearGrid:
    lon_corners  : typing.NDArray
    lat_corners  : typing.NDArray
    dlon         : float = None
    radius_earth : float = field(default=6_378_100.0, repr=False)
    cyclic       : bool = None
    _global      : bool = False
    
    def __post_init__(self):
        if self.east - self.west == 360 and self.cyclic is None:
            self.cyclic = True
        elif self.cyclic is None :
            self.cyclic = False
        if self.cyclic and self.south == -90 and self.north == 90:
            self._global = True

    @property
    def south(self) -> float:
        return self.lat_corners.min()
    
    @property
    def north(self) -> float:
        return self.lat_corners.max()

    @property
    def west(self) -> float:
        return self.lon_corners.min()

    @property
    def east(self) -> float:
        return self.lon_corners.max()

    @property
    def nlat(self) -> int:
        return len(self.lat_corners) - 1
    
    @property
    def nlon(self) -> int:
        return len(self.lon_corners) - 1

    @property
    def lonc(self) -> typing.NDArray:
        return (self.lon_corners[1:] + self.lon_corners[:-1]) / 2.
    
    @property
    def latc(self) -> typing.NDArray:
        return (self.lat_corners[1:] + self.lat_corners[:-1]) / 2.

    @property
    def latb(self) -> List[slice]:
        return [slice(self.lat_corners[_], self.lat_corners[_ + 1]) for _ in range(self.nlat)]
    
    @property
    def lat_b(self) -> List[slice]:
        return [_.start for _ in self.latb] + [self.latb[-1].stop]
    
    @property
    def lon_b(self) -> List[slice]:
        return [_.start for _ in self.lonb] + [self.lonb[-1].stop]

    @property
    def lonb(self) -> List[slice]:
        return [slice(self.lon_corners[_], self.lon_corners[_ + 1]) for _ in range(self.nlon)]
    
    @property
    def xr_dataset(self) -> xr.Dataset:
        return xr.Dataset(
            dict(
                lon = (['lon'], self.lonc, {'units' : 'degrees_north'}),
                lat = (['lat'], self.latc, {'units' : 'degrees_east'}),
                lat_b = (['lat_b'], self.lat_b, {'units' : 'degrees_north'}),
                lon_b = (['lon_b'], self.lon_b, {'units' : 'degrees_east'}),
            )
        )
    
    @property
    def area(self):
        dlon_rad = self.dlon * pi / 180.
        area = zeros((self.nlat+1, self.nlon), float64)
        lats = ( pi / 180. ) * self.lat_corners
        for ilat, lat in enumerate(lats):
            area[ilat, :] = self.radius_earth**2 * dlon_rad * sin(lat)
        return diff(area, axis=0)

    def expand_longitudes(self, west: float, east: float) -> Tuple["RectiLinearGrid", int, int]:
        """
        Create a new grid, spanning (if necessary) a wider range of longitudes. 
        """
        nsteps_east = 0
        nsteps_west = 0
        if west < self.west:
            nsteps_west = ceil((self.west - west) / self.dlon)
            west = self.west - self.dlon * nsteps_west
            
        if east > self.east:
            nsteps_east = ceil((east - self.east) / self.dlon)
            east = self.east + self.dlon * nsteps_east
            
        lon_corners = linspace(west, east, self.nlon + nsteps_east + nsteps_east + 1)
        return RectiLinearGrid(lon_corners=lon_corners, lat_corners=self.lat_corners, radius_earth=self.radius_earth, cyclic=False)

    def calc_overlap_matrices(self, other : "RectiLinearGrid") -> SimpleNamespace:
        """
        Calculate the overlaping between two regular grids.
        The function returns an namespace with a "lat" and "lon" attribute (arrays):
        - lat[i1, j1] is the fraction of a grid cell of lat index "i1", in the original grid, that is contained by a grid cell of lat index "j1" in the new grid (assuming they have the same longitude boundaries)
        - lon[i2, j2] is the fraction of a longitude interval "i2" (in the original grid) that is contained by a longitude interval "j2" in the new grid (assuming they have the same latitude boundaries).
        The product lat[i1, j1] * lon[i2, j2] gives the fraction of the grid cell (i1, j1) in the original grid that is contained in the grid cell (i2, j2) in the new grid.
        """

        # If both grids are cyclic, create a temporary expanded grid that fully contains the original grid
        # and wrap it around itself in a second time:
        overlaps_lat = self.calc_overlaps_lat(other)
        
        if self.cyclic and other.cyclic:
            # Create the temporary grid and calculate the overlaps for it
            tmpgrid = other.expand_longitudes(self.west, self.east)
            overlaps_lon = self.calc_overlaps_lon(tmpgrid)
            
            # Isolate the edge bands (whatever is outside the limits of the original grid)
            west_band = overlaps_lon[:, tmpgrid.lonc < self.west]
            east_band = overlaps_lon[:, tmpgrid.lonc > self.east]
            
            # Isolate the center band (what is within the limits of the original grid
            overlaps_lon = overlaps_lon[:, (tmpgrid.lonc >= self.west) * (tmpgrid.lonc <= self.east)]
            
            # Add the edge bands to the center one
            if west_band.shape[1] > 0 :
                overlaps_lon[:, :west_band.shape[1]] += west_band
            if east_band.shape[1] > 0 :
                overlaps_lon[:, -east_band.shape[1]:] += east_band
        else :
            overlaps_lon = self.calc_overlaps_lon(other)
        
        return SimpleNamespace(lat=overlaps_lat, lon=overlaps_lon)
        
    def calc_overlaps_lat(self, other: "RectiLinearGrid") -> typing.NDArray:
        overlaps = zeros((self.nlat, other.nlat))

        for ilat1, latb1 in enumerate(self.latb):
            for ilat2, latb2 in enumerate(other.latb):
                # Calculate what fraction of a grid cell ilat1 would end up in a grid cell ilat2
                minlat = max(latb1.start, latb2.start) * pi /180
                maxlat = min(latb1.stop, latb2.stop) * pi /180
                
                if minlat < maxlat:
                    area1 = sin(maxlat - minlat)
                    area2 = sin(latb1.stop * pi / 180 - latb1.start * pi / 180)
                    overlaps[ilat1, ilat2] = area1 / area2
        return overlaps

    def calc_overlaps_lon(self, other: "RectiLinearGrid") -> typing.NDArray:
        overlaps = zeros((self.nlon, other.nlon))
        for ilon1, lonb1 in enumerate(self.lonb):
            for ilon2, lonb2 in enumerate(other.lonb):
                # Calculate what fraction of a grid cell ilon1 would end up in a grid cell ilon2
                minlon = max(lonb1.start, lonb2.start)
                maxlon = min(lonb1.stop, lonb2.stop)
                if minlon < maxlon :
                    overlaps[ilon1, ilon2] = (maxlon - minlon) / (lonb1.stop - lonb1.start)
        return overlaps
        

class LMDZGrid(RectiLinearGrid):
    
    @classmethod
    def global_from_npoints(cls, nlon: int, nlat: int) -> "LMDZGrid":
        """
        Global LMDZ grid:
        - doesn't start at -180
        - has half-grids at the poles
        """
        dlon = 360. / nlon
        west = -180 - dlon / 2.
        east = 180 - dlon / 2.
        lon_corners = linspace(west, east, nlon + 1)
        
        dlat = 180. / (nlat - 1)
        south = -90 + dlat / 2.
        north = 90 - dlat / 2.
        lat_corners = append(append(-90, linspace(south, north, nlat - 1)), 90)
        return cls(lon_corners=lon_corners, lat_corners=lat_corners, dlon=dlon)
    

@dataclass
class TM5Grid(RectiLinearGrid):
    dlat : float = 1.
    
    @classmethod
    def global_from_steps(cls, dlon: float, dlat: float) -> "TM5Grid":
        nlon = 360 / dlon
        nlat = 180 / dlat
        assert nlat == int(nlat)
        assert nlon == int(nlon)
        return cls(lon_corners=linspace(-180, 180, int(nlon)+1), lat_corners=linspace(-90, 90, int(nlat)+1), dlon=dlon, dlat=dlat)

    @classmethod
    def global1x1(cls) -> "TM5Grid":
        return cls.global_from_steps(1, 1)


@dataclass
class LUMIAGrid(RectiLinearGrid):
    dlat : float = 0.5
    
    @classmethod
    def EUROCOM(cls, dlon: float=.5, dlat: Union[None, float]=None) -> "LUMIAGrid":
        if dlat is None :
            dlat = dlon
        nlon = 50 / dlon
        nlat = 40 / dlat
        assert nlon == int(nlon)
        assert nlat == int(nlat)
        return cls(lon_corners=linspace(-15, 35, int(nlon) + 1), lat_corners = linspace(33, 73, int(nlat) + 1), dlon=dlon)
        


@dataclass
class SpatialData:
    data        : typing.NDArray
    grid        : RectiLinearGrid
    lon_axis    : int
    lat_axis    : int
    density     : bool = False

    def to_quantity(self, inplace: bool = False) -> "SpatialData":
        """ 
        Converts amounts (e.g. kg, umol) to (spatial) fluxes (e.g. kg/m2, umol/m2, etc.).
        The temporal dimension is unchanged (e.g. kg/s will be converted to kg/s/m**2)

        Args:
            inplace (bool, optional): whether to modify the object in memory or to return a new one. 
        """
        # 1) Move the lat and lon to last positions:
        data = self.data.swapaxes(self.lon_axis, -1).swapaxes(self.lat_axis, -2)
        data = self._reorder_axes(data=self.data, position='end')
        data = data * self.grid.area
        data = self._reset_axes(data=data, position='end')
        
        # Return
        if inplace :
            self.data = data
            self.density = False
            return self
        else :
            return SpatialData(data=data, grid=self.grid, lon_axis=self.lon_axis, lat_axis=self.lat_axis)

    def _reorder_axes(self, data : typing.NDArray = None, position : str = 'end') -> typing.NDArray:
        """
        Move the lat and lon axis at the start or end (default) of the axis list
        Arguments :
            position : should be "start" (move lat axis to axis 0 and lon axis to axis 1) or "end" (move lat axis to axis -2 and lon axis to axis -1)
            data: array to reorder (optional, default: self.data)
        
        Return:
            reordered (view of the) axis 
        """
        if data is None :
            data = self.data

        if {'start':True, 'end':False}[position]:         # this will raise and error if position is not "start" or "end"
            if self.lat_axis > self.lon_axis :
                # lat after lon in original array ==> need to move lon first
                data = moveaxis(data, self.lon_axis, 0)   # Move lon axis to first position (no change on lon position)
                data = moveaxis(data, self.lat_axis, 0)   # Move lat axis on first position (changes lon position to axis 1)
            else :
                # lon after lat in original array ==> no change to their relative order
                data = moveaxis(data, self.lat_axis, 0)   # Move lat axis to first position (no change on lon position)
                data = moveaxis(data, self.lon_axis, 1)   # Move lon axis to second position (no change on lat position)
        else :
            if self.lat_axis > self.lon_axis:
                # lat after lon in original array ==> move lat to last, then lon to last
                data = moveaxis(data, self.lat_axis, -1)
                data = moveaxis(data, self.lon_axis, -1)
            else :
                # lon after lat in original array ==> move lon to last, then lat to before last 
                data = moveaxis(data, self.lon_axis, -1)
                data = moveaxis(data, self.lat_axis, -2)
        return data

    def _reset_axes(self, data : typing.NDArray = None, position: str = 'end') -> typing.NDArray:
        """
        Undo the effect of "reorder_axes". The exact same arguments should be used.
        """
        if data is None :
            data = self.data
            
        if {'start':True, 'end':False}[position]:         # this will raise and error if position is not "start" or "end"
            if self.lat_axis > self.lon_axis:
                # lat after lon in original array, but lat first in "data"
                data = moveaxis(data, 0, self.lat_axis)     # move lat from position 0 to its final position. lon is now in position 0
                data = moveaxis(data, 0, self.lon_axis)     # move lon from position 0 to its final position (no effect on lat)
            else :
                # lon after lat in original array and in "data" ==> just move lon, then lat
                data = moveaxis(data, 1, self.lon_axis)     # move lon to its final position
                data = moveaxis(data, 0, self.lat_axis)     # move lat to its final position
        else :
            if self.lat_axis > self.lon_axis:
                # lat after lon in original array, but lat first in "data"
                data = moveaxis(data, -1, self.lon_axis)    # move lon to its final position (now before lat). lat is now last axis
                data = moveaxis(data, -1, self.lat_axis)    # move lat to its final position (no effect on lon)
            else :
                # lon after lat in original array and in "data"
                data = moveaxis(data, -2, self.lat_axis)    # move lat to its final position (no effect on lon)
                data = moveaxis(data, -1, self.lon_axis)    # move lon to its final position (no effect on lat)
        return data
    
    def to_density(self, inplace: bool=True) -> "SpatialData":
        """
        Converts spatial fluxes (e.g. kg/m**2, umol/m**2, s/m**2) to amounts (kg, umol, s, etc.)
        The non-spatial dimensions (if any) are unchanged (e.g. kg/s/m**2 will be converted to kg/s)

        Args:
            inplace (bool, optional): whether to modify the object in memory or to return a new one. 
        """
        # 1) Move the lat and lon to last positions:
        data = self._reorder_axes(data=self.data, position='end')
        data = data / self.grid.area
        data = self._reset_axes(data=data, position='end')
        
        # Return
        if inplace :
            self.data = data
            self.density = False
            return self
        else :
            return SpatialData(data=data, grid=self.grid, lon_axis=self.lon_axis, lat_axis=self.lat_axis)
            
    def regrid(self, destgrid: RectiLinearGrid) -> "SpatialData":
        
        # Transition matrices:
        trans = self.grid.calc_overlap_matrices(destgrid)

        # Create destination array:
        shp = list(self.data.shape)
        shp[self.lon_axis] = destgrid.nlon
        shp[self.lat_axis] = destgrid.nlat
        if len(shp) == 2 :
            shp.append(1)
        data_out = zeros(shp)
        
        # Move the lat in first position and the lon in second:
        data_out = self._reorder_axes(data=data_out, position='start')
        
        # Convert to units / grid-cell, if needed:
        if self.density :
            data = self.to_quantity(inplace=False).data
        else :
            data = self.data
            
        # Apply the regridding:
        # For every gridcell of the output grid, the following does:
        #   - multiply the original array by the latitude overlap matrix
        #   - multiply the original array by the longitude overlap matrix
        #   - sum up the result and store it in the (ilat, ilon) of the destination array
        
        for ilat in tqdm(arange(destgrid.nlat)):
            # Ensure that the lat axis is in last position:
            fraction_lat = data.swapaxes(self.lat_axis, -1).copy()
            fraction_lat *= trans.lat[:, ilat]
            fraction_lat = fraction_lat.swapaxes(-1, self.lat_axis)

            for ilon in arange(destgrid.nlon):
                fraction_lat_lon = fraction_lat.swapaxes(self.lon_axis, -1).copy()
                fraction_lat_lon *= trans.lon[:, ilon]
                fraction_lat_lon = fraction_lat_lon.swapaxes(-1, self.lon_axis)
                data_out[ilat, ilon, :] = fraction_lat_lon.sum((self.lat_axis, self.lon_axis))

        # Move the axes in their original positions:
        data_out = self._reset_axes(data=data_out, position='start')

        # collapse the extra dimension that may have been created:
        if len(self.data.shape) < len(data_out.shape):
            data_out = data_out.squeeze(axis=-1)
        
        # Create the output structure:
        data_out = SpatialData(data=data_out, grid=destgrid, lon_axis=self.lon_axis, lat_axis=self.lat_axis) 


        # Re-convert to density, if needed:
        if self.density :
            data_out.to_density()

        return data_out
    

@dataclass
class Grid:
    lon0 : float = None
    lon1 : float = None
    lat0 : float = None
    lat1 : float = None
    dlon : float = None
    dlat : float = None
    nlon : int = None
    nlat : int = None
    latb : ndarray = field(default=None, repr=False, compare=False)
    latc : ndarray = field(default=None, repr=False, compare=False)
    lonb : ndarray = field(default=None, repr=False, compare=False)
    lonc : ndarray = field(default=None, repr=False, compare=False)
    radius_earth : float = field(default=6_378_100.0, repr=False)

    def __post_init__(self):
        """
        Ensure that all variables are set. The region can be initialized:
        - by specifying directly lat/lon coordinates (set type to b if these are boundaries)
        - by specifying lonmin, lonmax and dlon (and same in latitude)
        - by specifying lonmin, dlon and nlon (and sams in latitude)
        """

        # Set the longitudes first
        if self.dlon is None :
            if self.lonc is not None :
                self.dlon = self.lonc[1] - self.lonc[0]
            elif self.lonb is not None :
                self.dlon = self.lonb[1] - self.lonb[0]
            elif self.lon0 is not None and self.lon1 is not None and self.nlon is not None :
                self.dlon = (self.lon1 - self.lon0)/self.nlon
            # logger.debug(f"Set {self.dlon = }")

        if self.lon0 is None:
            if self.lonb is not None :
                self.lon0 = self.lonb.min()
            elif self.lonc is not None :
                self.lon0 = self.lonc.min() - self.dlon / 2
            # logger.debug(f"Set {self.lon0 = }")

        if self.nlon is None :
            if self.lonc is not None :
                self.nlon = len(self.lonc)
            elif self.lonb is not None :
                self.nlon = len(self.lonb) - 1
            elif self.lon0 is not None and self.lon1 is not None and self.dlon is not None :
                nlon = (self.lon1 - self.lon0) / self.dlon
                assert abs(nlon - round(nlon)) < 1.e-7, f'{nlon}, {self.lon1=}, {self.lon0=}, {self.dlon=}'
                self.nlon = round(nlon)

        # At this stage, we are sure to have at least dlon, lonmin and nlon, so use them only:`
        if self.lon1 is None :
            self.lon1 = self.lon0 + self.nlon * self.dlon

        if self.lonb is None :
            self.lonb = linspace(self.lon0, self.lon1, self.nlon + 1)

        if self.lonc is None :
            self.lonc = linspace(self.lon0 + self.dlon/2., self.lon1 - self.dlon/2., self.nlon)

        # Repeat the same thing for the latitudes:
        if self.dlat is None :
            if self.latc is not None :
                self.dlat = self.latc[1] - self.latc[0]
            elif self.lonb is not None :
                self.dlat = self.latb[1] - self.latb[0]
            elif self.lat0 is not None and self.lat1 is not None and self.nlat is not None :
                self.dlat = (self.lat1 - self.lat0)/self.nlat

        if self.lat0 is None:
            if self.latb is not None :
                self.lat0 = self.latb.min()
            elif self.latc is not None :
                self.lat0 = self.latc.min() - self.dlat / 2

        if self.nlat is None :
            if self.latc is not None :
                self.nlat = len(self.latc)
            elif self.latb is not None :
                self.nlat = len(self.latb) - 1
            elif self.lat0 is not None and self.lat1 is not None and self.dlat is not None :
                nlat = (self.lat1 - self.lat0) / self.dlat
                assert abs(nlat - round(nlat)) < 1.e-7
                self.nlat = round(nlat)

        # At this stage, we are sure to have at least dlon, lonmin and nlon, so use them only:
        if self.lat1 is None :
            self.lat1 = self.lat0 + self.nlat * self.dlat

        if self.latb is None :
            self.latb = linspace(self.lat0, self.lat1, self.nlat + 1)

        if self.latc is None :
            self.latc = linspace(self.lat0 + self.dlat/2., self.lat1 - self.dlat/2., self.nlat)

#        self.area = self.calc_area()

        self.round()

    def round(self, decimals=5):
        """
        Round the coordinates
        """
        self.latc = self.latc.round(decimals)
        self.latb = self.latb.round(decimals)
        self.lonc = self.lonc.round(decimals)
        self.lonb = self.lonb.round(decimals)
        self.lon0 = round(self.lon0, decimals)
        self.lat0 = round(self.lat0, decimals)
        self.lon1 = round(self.lon1, decimals)
        self.lat1 = round(self.lat1, decimals)

    @property
    def area(self) -> ndarray :
        return self.calc_area()

    @property
    def extent(self) -> List[float]:
        return [self.lon0, self.lon1, self.lat0, self.lat1]

    def calc_area(self):
        dlon_rad = self.dlon * pi / 180.
        area = zeros((self.nlat+1, self.nlon), float64)
        lats = ( pi / 180. ) * self.latb
        for ilat, lat in enumerate(lats):
            area[ilat, :] = self.radius_earth**2 * dlon_rad * sin(lat)
        return diff(area, axis=0)

    def get_land_mask(self, refine_factor=1, from_file=False) -> NDArray:
        """ Returns the proportion (from 0 to 1) of land in each pixel
        By default, if the type (land or ocean) of the center of the pixel determines the land/ocean type of the whole pixel.
        If the optional argument "refine_factor" is > 1, the land/ocean mask is first computed on the refined grid, and then averaged on the region grid (accounting for grid box area differences)"""
        if from_file :
            with File(from_file, 'r') as df :
                return df['lsm'][:]
        assert isinstance(refine_factor, int), f"refine factor must be an integer ({refine_factor=})"

        # 1. Create a finer resolution grid
        r2 = Grid(lon0=self.lon0, lat0=self.lat0, lon1=self.lon1, lat1=self.lat1, dlon=self.dlon/refine_factor, dlat=self.dlat/refine_factor)
        return land_mask.get_mask(r2).transform(self).data

    def mesh(self, reshape=None):
        lons, lats = meshgrid(self.lonc, self.latc)
        lons = lons.reshape(reshape)
        lats = lats.reshape(reshape)
        return lons, lats

    @property
    def indices(self):
        return arange(self.area.size)

    @property
    def shape(self):
        return (self.nlat, self.nlon)

    def __getitem__(self, item):
        """
        Enables reading the attributes as dictionary items.
        This enables constructing methods that can take indifferently a Grid or dict object.
        """
        return getattr(self, item)

    def __le__(self, other):
        return (self.lon0 >= other.lon0) & (self.lon1 <= other.lon1) & (self.lat0 >= other.lat0) & (self.lat1 <= other.lat1)

    def __lt__(self, other):
        return (self.lon0 > other.lon0) & (self.lon1 < other.lon1) & (self.lat0 > other.lat0) & (self.lat1 < other.lat1)


def calc_overlap_lons(sgrid, dgrid):
    """
    :param sgrid: grid specification for the source region. Can also be provided as a dictionary
    :param dgrid: grid specification for the dest region
    :return: O(s, d) matrix, where O[s, d] is the fraction (from 0 to 1) of the lon interval s in the source grid that is within the interval d in the destination grid
    """
    assert dgrid <= sgrid

    overlap = zeros((sgrid.nlon, dgrid.nlon))

    for ilon_s in range(sgrid.nlon):
        lon0, lon1 = sgrid.lonb[ilon_s], sgrid.lonb[ilon_s+1]
        ilon_d_min = max(0, searchsorted(dgrid.lonb, lon0) - 1)
        ilon_d_max = min(dgrid.nlon, searchsorted(dgrid.lonb, lon1))
        for ilon_d in range(ilon_d_min, ilon_d_max) :
            # get the partial lat-lon interval of the source grid that is in the current destination interval:
            lon_max = min(lon1, dgrid.lonb[ilon_d + 1])
            lon_min = max(lon0, dgrid.lonb[ilon_d])
            overlap[ilon_s, ilon_d] = (lon_max - lon_min) / (lon1 - lon0)
    return overlap


def calc_overlap_lats(sgrid, dgrid):
    """
    :param sgrid: grid specification for the source region. Can also be provided as a dictionary
    :param dgrid: grid specification for the dest region
    :return: O(s, d) matrix, where O[s, d] is the fraction (from 0 to 1) of the lat interval s in the source grid that is within the interval d in the destination grid
    """
    assert dgrid <= sgrid

    overlap = zeros((sgrid.nlat, dgrid.nlat))

    slatb = sgrid.latb * pi/360
    dlatb = dgrid.latb * pi/360

    for ilat_s in range(sgrid.nlat):
        lat0, lat1 = slatb[ilat_s], slatb[ilat_s+1]
        ilat_d_min = max(0, searchsorted(dlatb, lat0) - 1)
        ilat_d_max = min(dgrid.nlat, searchsorted(dlatb, lat1))
        for ilat_d in range(ilat_d_min, ilat_d_max) :
            # get the partial lat-lon interval of the source grid that is in the current destination interval:
            lat_max = min(lat1, dlatb[ilat_d + 1])
            lat_min = max(lat0, dlatb[ilat_d])
            overlap[ilat_s, ilat_d] = (lat_max - lat_min) / (lat1 - lat0)
            if overlap[ilat_s, ilat_d] < 0 : 
                print(lat_min, lat_max, lat0, lat1, ilat_s, ilat_d)
    return overlap


def calc_overlap_matrices(reg1, reg2):
    overlap_lat = calc_overlap_lats(reg1, reg2)
    overlap_lon = calc_overlap_lons(reg1, reg2)
    return SimpleNamespace(lat=overlap_lat, lon=overlap_lon)


@dataclass
class GriddedData:
    """
    The GriddedData class can be used to regrid data spatially.
    Arguments:
        :param data: array to regrid
        :param grid: spatial grid of the data
        :param axis: indices of the dimensions corresponding to the latitude and longitude. For instance, if data is a 3D array, with dimensions (time, lat, lon), then axis should be (1, 2). In practice, use the "dims" parameter instead.
        :param density: whether the data are density (i.e. units/m2) or quantity (i.e. unit/gridbox).
        :param dims: list of dimension names. Should contain "lat" and "lon".
    """
    data    : typing.NDArray
    grid    : Grid
    axis    : list = None
    density : bool = False
    dims    : list = None

    def __post_init__(self):
        if self.dims is not None :
            self.axis = [self.dims.index('lat'), self.dims.index('lon')]
        if self.axis is None :
            self.axis = (0, 1)

    def to_quantity(self, inplace=False):

        logger.info(f"{inplace = }")

        # 1) Move the lat and lon to last positions:

        if self.axis[1] > self.axis[0] :
            # If lat is before lon:
            data = moveaxis(self.data, self.axis[1], -1)  # Move lon to last position
            data = moveaxis(data, self.axis[0], -2)       # Move lat to before last position
        else:
            # if lon is before lat
            data = moveaxis(self.data, self.axis[0], -1)  # Move lat to last position
            data = moveaxis(data, self.axis[1], -1)       # Move lon to last position

        # Multiply by the grid cell area
        data = data * self.grid.area

        if self.axis[1] > self.axis[0]:
            # If lat is before lon:
            data = moveaxis(data, -2, self.axis[0])  # Move lat from before last position to original location
            data = moveaxis(data, -1, self.axis[1])  # Move lon from last position to original position
        else :
            # If lon is before lat:
            data = moveaxis(data, -1, self.axis[1])  # Move lon from last position to original position
            data = moveaxis(data, -1, self.axis[0])  # Move lat from last position to original position

        # Return
        if inplace :
            self.data = data
            self.density = False
            return self
        else :
            return GriddedData(data, self.grid, self.axis, density=False, dims=self.dims)

    def to_density(self, inplace=False):

        logger.info(f"{inplace = }")

        # 1) Move the lat and lon to last positions:

        if self.axis[1] > self.axis[0] :
            # If lat is before lon:
            data = moveaxis(self.data, self.axis[1], -1)  # Move lon to last position
            data = moveaxis(data, self.axis[0], -2)       # Move lat to before last position
        else:
            # if lon is before lat
            data = moveaxis(self.data, self.axis[0], -1)  # Move lat to last position
            data = moveaxis(data, self.axis[1], -1)       # Move lon to last position

        # Multiply by the grid cell area
        data = data / self.grid.area

        if self.axis[1] > self.axis[0]:
            # If lat is before lon:
            data = moveaxis(data, -2, self.axis[0])  # Move lat from before last position to original location
            data = moveaxis(data, -1, self.axis[1])  # Move lon from last position to original position
        else :
            # If lon is before lat:
            data = moveaxis(data, -1, self.axis[1])  # Move lon from last position to original position
            data = moveaxis(data, -1, self.axis[0])  # Move lat from last position to original position

        # Return
        if inplace :
            self.data = data
            self.density = True
            return self
        else :
            return GriddedData(data, self.grid, self.axis, density=True, dims=self.dims)

    def transform(self, destgrid: Grid, padding: Union[float, int, bool]=None, inplace: bool=False) -> "GriddedData":
        """
        Regrid (crop, refine, coarsen, pad) data on a different grid.
        Arguments:
            :param destgrid: grid in which the output data should be.
            :param padding: If a value is provided, the regridded data will be padded with this value where the boundaries of the destination grid exceed those from the source grid.
            :param inplace: determine whether the regridding operation should return a new object or

        """
        data = self

        density = False
        if self.density :
            data = data.to_quantity(inplace=inplace)
            density = True

        if padding is not None :
            data = data.pad(destgrid, padding, inplace=inplace)

        if destgrid < self.grid :
            data = data.crop(destgrid, inplace=inplace)

        if data.grid.dlon < destgrid.dlon and data.grid.dlat < destgrid.dlat :
            data = data.coarsen(destgrid, inplace=inplace)

        if data.grid.dlon > destgrid.dlon and data.grid.dlat > destgrid.dlat :
            data = data.refine(destgrid, inplace=inplace)

        if density :
            data = data.to_density(inplace=inplace)

        return data

    def rebin(self, destgrid: Grid, inplace: bool=False):
        """
        Regrid the data by simple rebinning. 

        Args:
            destgrid (Grid): Requested grid 
            inplace (bool, optional): whether to return a new GriddedData instance, or to modify the data in the current one 
        """
        
        overlaps = calc_overlap_matrices(self.grid, destgrid)

    def coarsen(self, destgrid : Grid, inplace=False):
        logger.info(destgrid)

        # ensure that the new grid is within the old one
        assert self.grid >= destgrid

        # Compute overlap ratios between the two grids
        overlaps = calc_overlap_matrices(self.grid, destgrid)

        # Create a temporary data container for regridding along longitude:
        shp = list(self.data.shape)
        shp[self.axis[1]] = destgrid.nlon
        temp = zeros(shp)

        # Move the longitude to 1st position in temp array, and to last position in self.data:
        temp = moveaxis(temp, self.axis[1], 0)
        self.data = moveaxis(self.data, self.axis[1], -1)

        # Do the longitude coarsening:
        for ilon in tqdm(range(destgrid.nlon)):
            temp[ilon, :] = (self.data * overlaps.lon[:, ilon]).sum(-1)

        # Put the dimensions back in the right order:
        self.data = moveaxis(self.data, -1, self.axis[1])
        temp = moveaxis(temp, 0, self.axis[1])

        # Create final coarsened array:
        shp[self.axis[0]] = destgrid.nlat
        coarsened = zeros(shp)

        # Swap the dimensions again:
        coarsened = moveaxis(coarsened, self.axis[0], 0)
        temp = moveaxis(temp, self.axis[0], -1)

        # Do the latitude coarsening:
        for ilat in tqdm(range(destgrid.nlat)):
            coarsened[ilat, :] = (temp * overlaps.lat[:, ilat]).sum(-1)

        # Put the dimensions back in place:
        coarsened = moveaxis(coarsened, 0, self.axis[0])
        del temp

        # Return:
        if inplace :
            self.data = coarsened
            self.grid = destgrid
            return self
        else :
            return GriddedData(coarsened, destgrid, axis=self.axis, density=self.density, dims=self.dims)

    def refine(self, destgrid : Grid, inplace=False) :
        # Create an intermediate grid, with the same resolution as the new grid and the same boundaries as the original one:
        tgrid = Grid(lat0=self.grid.lat0, lat1=self.grid.lat1, dlat=destgrid.dlat,
                     lon1=self.grid.lon1, lon0=self.grid.lon0, dlon=destgrid.dlon)
        
        # Calculate transision matrices:
        trans = calc_overlap_matrices(self.grid, tgrid)
        
        # Do the regridding:
        shp = self.data.shape
        shp[self.axis[0]] = tgrid.nlat
        shp[self.axis[1]] = tgrid.nlon
        out = zeros(shp)
        
        for ilat in range(tgrid.nlat):
            outlat = out.take(0, axis=self.axis[0]) * 0.
            # This is the equivalent of outlat = out[:, 0, :] * 0., if self.axis[0] is 1
            
            # Move lat axis in last position to perform the multiplication, then move it back to original position
            data = self.data.swapaxes(self.axis[0], -1).copy()
            data *= trans.lat
            data = data.swapaxes(-1, self.axis[0])
            data = data.sum(axis=self.axis[0])
       
            for ilon in range(tgrid.nlon):
                # We have lost one axis, account for it if needed:
                ax1 = self.axis[1]
                if self.axis[1] > self.axis[0] :
                    ax1 = self.axis[1] - 1
       
                # Repeat the axis swap trick:
                data = data.swapaxes(ax1, -1)
                data *= trans.lon
                data = data.swapaxes(-1, ax1)
       
                data = data.sum(axis=ax1) # This has all dims of self.data except lat and lon
       
                # Store this in output array:
                put_along_axis(outlat, ilon, data, ax1) # outlat has all dims of self.data except lat
                # this is the equivalent to :
                # outlat[ilon, :, :] = data, if "ax1" is 0,
                # outlat[:, ilon, :] = data, if "ax1" is 1, etc.
                
            put_along_axis(out, ilat, outlat, self.axis[0])
            # This is the equivalent of :
            # out[ilat, :, :] = outlat, if self.axis[0] is 0, etc.
        
        out = GriddedData(out, tgrid, axis=self.axis).crop(destgrid)
        if inplace :
            self.data = out.data
            self.grid = destgrid
            return self
        else :
            return out

    def crop(self, destgrid, inplace=False):
        logger.info(destgrid)

        # ensure that the new grid is a subset of the old one
        assert destgrid.lat0 in self.grid.latb
        assert destgrid.lat1 in self.grid.latb
        assert destgrid.lon0 in self.grid.lonb
        assert destgrid.lon1 in self.grid.lonb
#        assert all([l in self.grid.latc for l in destgrid.latc])
#        assert all([l in self.grid.lonc for l in destgrid.lonc])

        # crop:
        #slat = array([l in destgrid.latc for l in self.grid.latc])
        #slon = array([l in destgrid.lonc for l in self.grid.lonc])
        slat = array([destgrid.lat0 < l < destgrid.lat1 for l in self.grid.latc])
        slon = array([destgrid.lon0 < l < destgrid.lon1 for l in self.grid.lonc])

        data = self.data.swapaxes(self.axis[0], 0)
        data = data[slat, :, :]
        data = data.swapaxes(0, self.axis[0])
        data = data.swapaxes(self.axis[1], 0)
        data = data[slon, :, :]
        data = data.swapaxes(0, self.axis[1])

        newgrid = Grid(latc=self.grid.latc[slat], lonc=self.grid.lonc[slon], dlat=self.grid.dlat, dlon=self.grid.dlon)

        if inplace :
            self.data = data
            self.grid = newgrid
            return self
        else :
            return GriddedData(data, newgrid, self.axis, density=self.density, dims=self.dims)

    def pad(self, boundaries : Grid, padding, inplace=False):
        """
        Expand the existing arrays with "padding", up to the boundaries of the "destgrid".
        the "destgrid" argument is ideally an instance of the "Grid" class, but can also just be a dictionary.
        """
        logger.info(f"{boundaries = }; {padding = }")

        # 1) Create a new grid:
        pad_before_lon, lon0 = 0, self.grid.lon0
        while lon0 > boundaries['lon0'] :
            lon0 -= self.grid.dlon
            pad_before_lon += 1
        pad_before_lat, lat0 = 0, self.grid.lat0
        while lat0 > boundaries['lat0'] :
            lat0 -= self.grid.dlat
            pad_before_lat += 1
        pad_after_lon, lon1 = 0, self.grid.lon1
        while lon1 < boundaries['lon1'] :
            lon1 += self.grid.dlon
            pad_after_lon += 1
        pad_after_lat, lat1 = 0, self.grid.lat1
        while lat1 < boundaries['lat1'] :
            lat1 += self.grid.dlat
            pad_after_lat += 1
        newgrid = Grid(dlon=self.grid.dlon, dlat=self.grid.dlat, lon0=lon0, lon1=lon1, lat0=lat0, lat1=lat1)

        # 2) Create an array based on that grid, fill with "padding"
        # numpy "pad" method requires the number of elements to be added before and after, for each dim:
        padding_locations = [(0,0) for _ in range(len(self.data.shape))]
        padding_locations[self.axis[0]] = (pad_before_lat, pad_after_lat)
        padding_locations[self.axis[1]] = (pad_before_lon, pad_after_lon)

        data = pad(self.data, padding_locations, constant_values=padding)

        # 4) Return or modify self, based in the requested "inplace"
        if inplace :
            return GriddedData(data, newgrid, self.axis, density=self.density, dims=self.dims)
        else :
            self.data = data
            self.grid = newgrid
            return self

    def as_dataArray(self, coords: dict=None, dims: list=None, attrs: dict=None, **kwargs):
        if dims is None and self.dims is None :
            logger.error("Dimensions must be provided, either through the dims keyword or when instantiating the class")
        if coords is None :
            kwargs['lat'] = kwargs.get('lat', self.grid.latc)
            kwargs['lon'] = kwargs.get('lon', self.grid.lonc)
            coords = {}
            for dim in self.dims :
                coords[dim] = kwargs.get(dim)
        return xr.DataArray(
            data = self.data,
            dims = self.dims,
            coords = coords,
            attrs = attrs
        )


def grid_from_rc(rcf, name=None):
    pfx1 = 'grid.' 
    if name is not None :
        pfx0 = f'grid.{name}.'
    else :
        pfx0 = pfx1
    lon0 = rcf.get(f'{pfx0}lon0', default=rcf.get(f'{pfx1}lon0', default=None))
    lon1 = rcf.get(f'{pfx0}lon1', default=rcf.get(f'{pfx1}lon1', default=None))
    dlon = rcf.get(f'{pfx0}dlon', default=rcf.get(f'{pfx1}dlon', default=None))
    nlon = rcf.get(f'{pfx0}nlon', default=rcf.get(f'{pfx1}nlon', default=None))
    lat0 = rcf.get(f'{pfx0}lat0', default=rcf.get(f'{pfx1}lat0', default=None))
    lat1 = rcf.get(f'{pfx0}lat1', default=rcf.get(f'{pfx1}lat1', default=None))
    dlat = rcf.get(f'{pfx0}dlat', default=rcf.get(f'{pfx1}dlat', default=None))
    nlat = rcf.get(f'{pfx0}nlat', default=rcf.get(f'{pfx1}nlat', default=None))
    return Grid(lon0=lon0, lat0=lat0, lon1=lon1, lat1=lat1, dlon=dlon, dlat=dlat, nlon=nlon, nlat=nlat)

