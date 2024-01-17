#!/usr/bin/env python

from lumia.formatters.lagrange import Struct, ReadStruct
from lumia.obsdb import obsdb
import xarray as xr
import os
from loguru import logger
from numpy import array_equal, linspace, float64, zeros, diff, pi, sin, array, issubdtype, datetime64, int64
from pandas import Timestamp, ExcelWriter
from lumia.obsdb import obsdb


class RegularHgrid:
    def __init__(self, lats, lons):
        self.lats = lats
        self.lons = lons
        self.dlon = diff(self.lons)[0]
        self.dlat = diff(self.lats)[0]
        self.nlat = len(self.lats)
        self.nlon = len(self.lons)
        self.latmin = self.lats.min()-self.dlat/2
        self.latmax = self.lats.max()+self.dlat/2

    def calc_area(self):
        R_e = 6378100.0 # Radius of Earth in meters
        dlon_rad = self.dlon*pi/180.
        area = zeros((self.nlat+1, self.nlon), float64)
        lats = (pi/180.)*linspace(self.latmin, self.latmax, self.nlat+1)
        for ilat, lat in enumerate(lats):
            area[ilat,:] = R_e**2*dlon_rad*sin(lat)
        return diff(area, axis=0)


class RegularTgrid:
    def __init__(self, times):
        # Make sure that times is an array of datetimes or Timestamp:
        if isinstance(times, xr.DataArray):
            times = times.data
        if issubdtype(times.dtype, datetime64):
            times = array([Timestamp(t) for t in times.astype(int64)])
        self.times = times
        self.dt = self.times[1]-self.times[0]

    def shift(self, shift):
        self.times += shift

    def calc_len_sec(self):
        times0 = self.times-self.dt/2
        times1 = self.times+self.dt/2
        return array([t.total_seconds() for t in times1-times0])


class RegularGrid(RegularHgrid, RegularTgrid):
    def __init__(self, lats=None, lons=None, times=None):
        if lats is not None and lons is not None :
            RegularHgrid.__init__(self, lats, lons)
        if times is not None :
            RegularTgrid.__init__(self, times)


class Flux(xr.Dataset):
    def __init__(self, unit='umol/m2/s', *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_mass(self, unit='PgC', scale='Pg', molar_mass=12.011):

        # Main scaling factor
        scale_in = {'umol':1.e-6}[self.unit.split('/')[0]]
        scale_out = {'Pg':1.e-15, 'Tg':1.e-9}[scale]
        scalefac = molar_mass*scale_out*scale_in

        # Area correction
        grid = RegularGrid(self.lat, self.lon, self.time)
        scalefac *= grid.calc_area()

        # Temporal aggregation
        scalefac *= grid.dt.total_seconds()

        # Apply:
        for cat in self.categories :
            self[cat].data = self[cat].data*scalefac[None, :, :]

        self.attrs['unit'] = unit

    def add_step(self, other, label_self='apri', label_other='apos'):
        cat_diff = []
        for cat in self.categories :
            if not array_equal(self[cat], other[cat]):
                self[f'{cat}.{label_other}'] = (('time', 'lat', 'lon'), other[cat].data)
                cat_diff.append(f'{cat}.{label_other}')
        self.attrs[f'categories.{label_other}'] = cat_diff
        self.attrs['categories'] = self.attrs['categories']+cat_diff


class Struct(Struct):
    def to_xarray(self):
        ds = Flux() 
        for cat in self.keys():
            ds[cat] = (('time', 'lat', 'lon'), self[cat]['emis'])
            cat0 = cat
        # Make sure that all data are on the same coordinates
        assert all([array_equal(self[cat0]['lats'], self[cat]['lats']) for cat in self.keys()])
        assert all([array_equal(self[cat0]['lons'], self[cat]['lons']) for cat in self.keys()])
        assert all([array_equal(self[cat0]['time_interval']['time_start'], self[cat]['time_interval']['time_start']) for cat in self.keys()])
        assert all([array_equal(self[cat0]['time_interval']['time_end'], self[cat]['time_interval']['time_end']) for cat in self.keys()])
        
        # Create a full set of coordinates
        grid = RegularGrid(self[cat]['lats'], self[cat]['lons'], self[cat]['time_interval']['time_start'])
        grid.shift(grid.dt/2)

        ds.coords['lat'] = grid.lats
        ds.coords['lon'] = grid.lons
        ds.coords['time'] = grid.times
        
        ds.attrs['unit'] = 'umol/m2/2'
        ds.attrs['dt'] = grid.dt.total_seconds()
        ds.attrs['dlat'] = grid.dlat
        ds.attrs['dlon'] = grid.dlon
        ds['area'] = (('lat', 'lon'), grid.calc_area())
        ds['area'].attrs['unit'] = 'm2'
        ds.attrs['categories'] = list(self.keys())
        return ds
    
    def to_netCDF(self, ncfile):
        logger.debug(f'lumia_tools.formatters.to_netcdf() L124: writing data to ncfile={ncfile}')
        self.to_xarray().to_netcdf(ncfile)


class Observations(obsdb):
    """
    This class is used to combine the prior and posterior obsdbs, and save the relevant information from
    them in a shareable file format (xlsx)
    """
    def to_excel(self, filename, only_columns=None, drop_columns=None):
        if drop_columns is not None :
            obs = self.observations.drop(columns=drop_columns, inplace=True)
        elif only_columns is not None :
            obs = self.observations.loc[:, only_columns]
        else :
            obs = self.observations
        with ExcelWriter(filename) as xls :
            obs.to_excel(xls, sheet_name='observations')
            self.sites.to_excel(xls, sheet_name='sites')


class Postprocessor:
    def __init__(self, folder, flux=True, obs=False):
        self.path = folder
        self.flux_apri = os.path.join(self.path, 'modelData.apri.nc')
        self.flux_apos = os.path.join(self.path, 'modelData.apos.nc')
        self.obs_apri = os.path.join(self.path, 'observations.apri.tar.gz')
        self.obs_apos = os.path.join(self.path, 'observations.apos.tar.gz')

        if os.path.exists(self.flux_apos) and os.path.exists(self.flux_apri):
            # Combine the fluxes
            self.flux = self.combineFluxes()

            # Combine the observations
#            os.remove(self.obs_apri)

            # Remove one of the comm_files
#            os.remove(os.path.join(self.path, 'comm_file.apos.nc4'))
    
    def combineFluxes(self):
        apri = ReadStruct(self.flux_apri, structClass=Struct).to_xarray()
        apos = ReadStruct(self.flux_apos, structClass=Struct).to_xarray()
        apri.add_step(apos)
        return apri

    def write_flux(self, fname='optim_fluxes.nc', remove_originals=False):
        self.flux.to_netcdf(os.path.join(self.path, fname))
        if remove_originals:
            os.remove(self.flux_apri)
            os.remove(self.flux_apos)

    def write_obs(self, fname='observations.xlsx', only_columns=None, drop_columns=None):
        obs = Observations(self.obs_apos)
        obs.to_excel(os.path.join(self.path, fname), only_columns, drop_columns)
