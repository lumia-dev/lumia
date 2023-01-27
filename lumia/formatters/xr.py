import os
import sys
from typing import Union, List, Tuple
from pathlib import Path
import subprocess
from pint import Quantity
import xarray as xr
from dataclasses import dataclass, field, asdict
from numpy import ndarray, unique, array, zeros, nan
from gridtools import Grid
from rctools import RcFile
from datetime import datetime
from pandas import PeriodIndex, Timestamp, DatetimeIndex
from loguru import logger
from lumia.units import units_registry as ureg
from gridtools import grid_from_rc
from pandas import date_range
from pandas.tseries.frequencies import DateOffset, to_offset
from lumia.tracers import species, Unit
from lumia.Tools.time_tools import periods_to_intervals
from netCDF4 import Dataset
# from lumia.icosPortalAccess import  readLv3NcFileFromCarbonPortal   # as fromICP
import icosPortalAccess.readLv3NcFileFromCarbonPortal as fromICP
import numbers
from archive import Rclone
from typing import Iterator


@dataclass
class Constructor:
    _value : Union[str, dict] = None

    def __post_init__(self):
        if isinstance(self._value, Constructor):
            self._value = self._value.dict

    @property
    def dict(self) -> dict:
        if isinstance(self._value, dict):
            return self._value
        cats = [c.split('*') for c in self._value.replace(' ', '').replace('-', '+-1*').split('+')]
        return {v[-1] : array(v[:-1], dtype=float).prod() for v in cats}

    @property
    def str(self) -> str:
        if isinstance(self._value, str):
            return self._value
        return '+'.join([f'{v}*{k}' for (k, v) in self._value.items()])

    @property
    def items(self):
        return self.dict.items

    @property
    def keys(self):
        return self.dict.keys


@dataclass
class Category:
    name      : str
    tracer    : str
    optimized : bool = False
    optimization_interval : DateOffset = None
    apply_lsm : bool = True
    is_ocean  : bool = False
    n_optim_points : int = None
    horizontal_correlation : str = None
    temporal_correlation   : str = None
    total_uncertainty : float = nan
    unit_emis : Quantity = None
    unit_mix : Quantity = None
    unit_budget : Quantity = None
    unit_optim  : Quantity = None
    meta : bool = False
    constructor : Constructor = None
    transported : bool = True

    def as_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, name, kwargs):
        return cls(name, **{k: v for k, v in kwargs.items() if k in cls.__dataclass_fields__})


def offset_to_pint(offset: DateOffset):
    try :
        return (offset.nanos * 1.e-9) * ureg.s
    except ValueError :
        if offset.freqstr in ['M', 'MS'] :
            return offset.n * ureg.month
        elif offset.freqstr in ['A', 'AS', 'Y', 'YS']:
            return offset.n * ureg.year
        elif offset.freqstr == 'W':
            return offset.n * ureg.week


class TracerEmis(xr.Dataset):
    __slots__ = 'grid', '_mapping'

    def __init__(self, *args, tracer_name : str = None, grid : Grid = None, time: DatetimeIndex = None, units: Quantity = None, timestep: str = None, attrs=None, categories: dict = None):

        self._mapping = {'time': None, 'space': None}  # TODO: replace by a dedicated class?

        # If we are initializing from an existing Dataset
        if args :
            super().__init__(*args, attrs=attrs)
            self.grid = Grid(latc=self.lat.values, lonc=self.lon.values)

        else :
            # Ensure we have the correct data types:
            time = DatetimeIndex(time)
            timestep = to_offset(timestep).freqstr

            super().__init__(
                coords=dict(time=time, lat=grid.latc, lon=grid.lonc),
                attrs=attrs
            )

            assert tracer_name is not None
            assert grid is not None
            assert time is not None

            self.attrs['tracer'] = tracer_name
            self.attrs['categories'] = []
            self.attrs['timestep'] = timestep
            self.attrs['units'] = units
            self.grid = grid

            self['area'] = xr.DataArray(data=grid.area, dims=['lat', 'lon'], attrs={'units': ureg('m**2').units})
            self['timestep_length'] = xr.DataArray((time + to_offset(timestep) - time).total_seconds().values, dims=['time', ], attrs={'units': ureg.s})

            # If any field has been passed to the constructor, add it here:
            if categories is not None :
                for cat, value in categories.items() :
                    if isinstance(value, dict) :
                        self.add_cat(cat, value['data'], value.get('attrs', None))
                    else :
                        self.add_cat(cat, value)


    def __getitem__(self, key) -> xr.DataArray:
        var = super().__getitem__(key)
        if var.attrs.get('meta', False):
            arr = xr.DataArray(coords=self.coords, dims=['time', 'lat', 'lon'], data=zeros(self.shape), attrs=var.attrs)
            for cat, coeff in Constructor(var.constructor).items():
                arr.data[:] += coeff * self[cat].data
            return arr
        else :
            return var

    # Category iterators:
    def iter_cats(self) -> Iterator[Category]:
        for cat in self.attrs['categories']:
            yield Category.from_dict(cat, {**self.variables[cat].attrs, **self.attrs, **species[self.tracer].__dict__})

    @property
    def shape(self) -> Tuple[int, int, int]:
        return self.dims['time'], self.dims['lat'], self.dims['lon']

    @property
    def optimized_categories(self) -> List[Category]:
        return [c for c in self.iter_cats() if c.optimized]

    @property
    def transported_categories(self) -> List[Category]:
        return [c for c in self.iter_cats() if c.transported]

    @property
    def base_categories(self) -> List[Category]:
        return [c for c in self.iter_cats() if not c.meta]

    @property
    def meta_categories(self) -> List[Category]:
        return [c for c in self.iter_cats() if c.meta]

    @property
    def period_index(self) -> PeriodIndex:
        """
        Provides a pandas "PeriodIndex" view of the time coordinate
        """
        return self.time.to_index().to_period(self.attrs['timestep'])

    # Time accessors
    @property
    def intervals(self):
        return periods_to_intervals(self.period_index)

    @property
    def timestep(self):
        return offset_to_pint(to_offset(self.attrs['timestep']))

    @property
    def period(self):
        return to_offset(self.attrs['timestep'])

    @property
    def timestamp(self):
        return array([Timestamp(t) for t in self.time.data])

    @property
    def start(self):
        return Timestamp(self.time.min().values)

    @property
    def end(self):
        return Timestamp(self.time.max().values) + self.period

    # Spatial and temporal mapping
    @property
    def temporal_mapping(self):
        return self._mapping['time']

    @temporal_mapping.setter
    def temporal_mapping(self, value):
        self._mapping['time'] = value

    @property
    def spatial_mapping(self):
        return self._mapping['space']

    @spatial_mapping.setter
    def spatial_mapping(self, value):
        self._mapping['space'] = value

    # Regular methods
    def add_cat(self, name: str, value: ndarray, attrs: dict = None):
        if isinstance(value, numbers.Number):
            value = zeros(self.shape) + value
        assert isinstance(value, ndarray), logger.error(f"The value provided is not a numpy array ({type(value) = }")
        # print(value.shape,  flush=True)
        # print(self.shape,  flush=True)
        assert value.shape == self.shape, logger.error(f"Shape mismatch between the value provided ({value.shape}) and the rest of the dataset ({self.shape})")
        if attrs is None:
            attrs = {'tracer': self.tracer}
        self[name] = xr.DataArray(value, dims=['time', 'lat', 'lon'], attrs=attrs)
        self.attrs['categories'].append(name)

    def add_metacat(self, name: str, constructor: Union[dict, str], attrs: dict = None):
        """
        A meta-category is a category that is constructed based on a linear combination of several other categories. Besides this, it is treated as any other category by the inversion.
        Internally, it is just an empty variable, with the "meta" attribute set to True, and a "constructor" attribute, plus the standard attributes of other categories.
        Arguments:
            name: name of the meta-category
            constructor: linear combination of categories that constitute the metacat
            attrs: optional dictionary containing the (netcdf) attributes of the meta-category

        Example:

            # Load basic categories
            em = Data(...)
            em.add_cat('global_wetlands', ...)
            em.add_cat('tropical_wetlands', ...)
            em.add_cat('fossil', ...)
            em.add_cat('fires', ...)
            em.add_cat('waste', ...)


            # Create a "anthrop" category, containing 40% the fires plus the waste and fossil emissions:
            em.add_metacat('anthrop', '0.4*fires + waste + fossil')

            # Create a "nat_fires" category containing the remaining 60% of "fires":
            em.add_metacat('nat_fires', '0.6*fires')

            # Create a "wetlands" by subtracting "tropical_wetlands" from "global_wetlands"
            em.add_metacat('wetlands', 'global_wetlands - tropical_wetlands')

            In an inversion, one would typically then set the "wetlands", "fossil", "fires" and "waste" categories to non-transported/non-optimized. 
        """
        self[name] = xr.DataArray(None)
        self.attrs['categories'].append(name)
        attrs = dict() if attrs is None else attrs 
        attrs['meta'] = True
        attrs['constructor'] = Constructor(constructor)
        self.variables[name].attrs.update(attrs)

        # All categories aggregated in the meta-category are not further transported, unless they have
        # previously been explicitly tagged as transported
        for cat in attrs['constructor'].keys():
            self.variables[cat].attrs['transported'] = max(False, self.variables[cat].attrs.get('transported', False)) 

    def print_summary(self, units=None):
        if units is None :
            units = species[self.tracer].unit_budget
        original_unit = self.units
        self.convert(units)
        
        for cat in self.categories :
            monthly_emis = self[cat].resample(time='MS', closed='left').sum(['lat', 'lon', 'time'])
            logger.info("===============================")
            logger.info(f"{cat}:")
            for year in unique(monthly_emis.time.dt.year):
                logger.info(f'{year}:')
                monthly_emis_year = monthly_emis.sel(time=slice(Timestamp(year, 1, 1), Timestamp(year, 12, 31)))  # where(monthly_emis.time.dt.year == year)
                for em in monthly_emis_year :
                    logger.info(f'  {em.time.dt.strftime("%B").data}: {em.data:7.2f} {units}')
                logger.info("    --------------------------")
                logger.info(f"   Total : {monthly_emis_year.sum().data:7.2f} {units}")
        self.convert(original_unit)

    def to_extensive(self):
        if self.units.dimensionality.get('[time]') + self.units.dimensionality.get('[length]') == 0 :
            return
        new_unit = self.units * ureg('s') * ureg('m**2')
        self.convert(str(new_unit.u))
        assert self.units.dimensionality.get('[time]') == 0, self.units
        assert self.units.dimensionality.get('[length]') == 0, self.units

    def to_intensive(self):
        if self.units.dimensionality.get('[time]') == -1 and self.units.dimensionality.get('[length]') == -2 :
            return
        new_unit = self.units / ureg.s / ureg.m**2
        self.convert(str(new_unit))
        assert self.units.dimensionality.get('[time]') == -1, self.units
        assert self.units.dimensionality.get('[length]') == -2, self.units

    def to_intensive_adj(self):
        new_unit = self.units / ureg.s / ureg.m**2
        self.convert(str(new_unit.u))

    def convert(self, destunit: Union[str, Unit, Quantity]):
        dest = destunit
        coeff = 1.
        if isinstance(destunit, str):
            dest = ureg(destunit).units
        elif isinstance(destunit, Quantity):
            dest = destunit.units
            coeff = destunit.magnitude

        for cat in self.base_categories :
            # Check if we need to multiply or divide by time and area:

            power_t = (dest / self.units).dimensionality.get('[time]')
            power_s = (dest / self.units).dimensionality.get('[length]')

            catunits = self.units

            # from units/m2 to units/gricell
            if power_s == 2 :
                self[cat.name].data *= self.area.data
                catunits *= self.area.units
            # from units/gridcell to units/m2
            elif power_s == -2 :
                self[cat.name].data /= self.area.data
                catunits /= self.area.units
            elif power_s != 0 :
                raise RuntimeError(f"Unexpected units conversion request: {self[cat.name].data.unit} to {dest} ({power_s = })")

            # From units/s to units/tstep
            if power_t == 1 :
                self[cat.name].data = (self[cat.name].data.swapaxes(0, -1) * self.timestep_length.data).swapaxes(0, -1)
                catunits *= self.timestep_length.units
            # From units/tstep to units/s
            elif power_t == -1 :
                self[cat.name].data = (self[cat.name].data.swapaxes(0, -1) / self.timestep_length.data).swapaxes(0, -1)
                catunits /= self.timestep_length.units
            elif power_t != 0 :
                raise RuntimeError(f"Unexpected units conversion request: {self[cat.name].data.units} to {dest} ({power_t =})")

            # Finally, convert:
            self[cat.name].data = (self[cat.name].data * catunits).to(dest).magnitude * coeff
            
        self.attrs['units'] = dest

    def to_netcdf(self, filename, group=None, only_transported=False, **kwargs):

        # Replace the standard xarray.Dataset.to_netcdf method, which is too limitative
        with Dataset(filename, 'w') as nc:
            if group is not None :
                nc = nc.createGroup(group)

            # Dimensions and coordinates
            for dim in self.dims:
                nc.createDimension(dim, len(self[dim]))

            # Coordinates
            for var in self.coords:
                vartype = self[var].dtype
                if vartype == 'datetime64[ns]':
                    data = (self[var].data - self[var].data[0]) / 1.e9
                    nc.createVariable(var, 'int64', self[var].dims)
                    nc[var].units = f'seconds since {self[var][0].dt.strftime("%Y-%m-%d").data}'
                    nc[var].calendar = 'proleptic_gregorian'
                    nc[var][:] = data
                else :
                    nc.createVariable(var, self[var].dtype, self[var].dims)
                    nc[var][:] = self[var].data

            varlist = ['area', 'timestep_length']
            if only_transported :
                varlist.extend([c.name for c in self.transported_categories])
            else :
                varlist.extend([c for c in self.categories])

            # data variables
            for var in varlist :
                nc.createVariable(var, self[var].dtype, self[var].dims)
                nc[var][:] = self[var].data

                # Copy var attributes:
                for k, v in attrs_to_nc(self[var].attrs).items():
                    setattr(nc[var], k, v)

            # global attributes
            for k, v in attrs_to_nc(self.attrs).items():
                setattr(nc, k, v)

                if only_transported :
                    nc.categories = [c.name for c in self.transported_categories]

        if self.temporal_mapping:
            self.temporal_mapping.to_netcdf(filename, group=f'{group}/temporal_mapping', mode='a')
            self.spatial_mapping.to_netcdf(filename, group=f'{group}/spatial_mapping', mode='a')

    def resolve_metacats(self) -> None:
        """
        This will uncouple the value of the meta-categories from the value of their "parent" categories (so that they can now be updated independently).
        The "meta" flags are renamed in "_meta" (so the meta-categories are treated as a normal ones by __getitem__, but can be made into metacats easily again), and the dummy data is replaced by the actual values that the metacats represent.
        """
        for cat in self.iter_cats():
            if cat.meta :
                # Retrieve the value of the metacat, change the attributes of the returned data
                value = self[cat.name]
                del value.attrs['meta']
                value.attrs['_meta'] = True

                # Delete also the meta attribute from the original variable, so that it can be edited
                del self.variables[cat.name].attrs['meta']

                # Copy the resolved value to the variable
                self[cat.name] = value

    def dimensionality(self, dim: str) -> int:
        """
        Return the dimensionality of the data, in either time or space.
        Argument:
            dim : one of "time" or "length"
        """
        return self.units.dimensionality.get(f'[{dim}]')


def attrs_to_nc(attrs: dict) -> dict:
    """
    Convert items of a dictionary that cannot be written as netCDF attributes to a netCDF-compliant format.
    """
    # Make sure we work on a copy of the dictionary
    attrs = {k: v for (k, v) in attrs.items()}

    # Store the name of the variables that have been converted
    to_bool = []
    to_units = []

    # Do the actual conversion
    for k, v in attrs.items():
        if isinstance(v, bool):
            attrs[k] = int(v)
            to_bool.append(k)
        if isinstance(v, Unit):
            attrs[k] = str(v)
            to_units.append(k)
        if isinstance(v, Constructor):
            attrs[k] = v.str

    # add attributes listing the variable conversions (for converting back)
    if to_bool :
        attrs['_bool'] = to_bool
    if to_units :
        attrs['_units'] = to_units
    return attrs


def nc_to_attrs(attrs: dict) -> dict:
    for attr in attrs.get('_bool', []):
        attrs[attr] = bool(attrs[attr])
    for attr in attrs.get('_units', []):
        attrs[attr] = ureg(attrs[attr]).units
    if '_bool' in attrs:
        del attrs['_bool']
    if '_units' in attrs:
        del attrs['_units']
    if 'constructor' in attrs :
        attrs['constructor'] = Constructor(attrs['constructor'])
    return attrs

    
@dataclass
class Data:
    _tracers : dict = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self._tracers, TracerEmis):
            self._tracers = {self._tracers.name: self._tracers}
        for tr in self._tracers :
            setattr(self, tr, self._tracers[tr])

    def add_tracer(self, tracer: TracerEmis):
        self._tracers[tracer.tracer] = tracer
        setattr(self, tracer.tracer, self._tracers[tracer.tracer])

    def print_summary(self):
        for tracer in self._tracers :
            self[tracer].print_summary()

    def __getitem__(self, item):
        return self._tracers[item]

    def __setitem__(self, key, value):
        if isinstance(value, TracerEmis):
            self._tracers[key] = value
        else :
            raise TypeError(f"can only set an instance of {TracerEmis} as class item")

    def to_extensive(self):
        """
        Convert the data to extensive units (e.g. umol, PgC)
        """
        for tr in self._tracers :
            self[tr].to_extensive()

    def to_intensive(self):
        """
        Convert the data to intensive units (e.g. umol/m2/s, PgC/m2/s)
        """
        for tr in self._tracers :
            self[tr].to_intensive()

    def to_intensive_adj(self):
        """
        Adjoint of to_intensive (e.g. convert data from umol/m2/s to umol/m4/s2)
        """
        for tr in self._tracers :
            self[tr].to_intensive_adj()

    def convert(self, units: Union[str, dict]) -> None:
        """
        convert all tracers to units specified by the "units" argument.
        Alternatively, "units" can be provided as a string, then all tracers will be converted to that unit.
        """
        if isinstance(units, str):
            units = {tr: units for tr in self.tracers}
        for tr in self.tracers :
            self[tr].convert(units[tr])

    def resample(self, time=None, lat=None, lon=None, grid=None, inplace=False) -> "Data":
        new = self if inplace else Data()
        for tracer in self.tracers :
            if time:
                # Resample the emissions for that tracer
                resampled_data = self[tracer][self[tracer].categories].resample(time=time)
                if self[tracer].dimensionality('time') == 0:
                    resampled_data = resampled_data.sum()
                elif self[tracer].dimensionality('time') == -1:
                    resampled_data = resampled_data.mean()

                # Create new tracer for storing this:
                tr = TracerEmis(
                    tracer_name = tracer,
                    grid = self[tracer].grid,
                    time = resampled_data.time,
                    units = self[tracer].units,
                    timestep = to_offset(time).freqstr,
                )
                for cat in self[tracer].base_categories:
                    tr.add_cat(cat.name, resampled_data[cat.name].values, attrs=self[tracer][cat.name].attrs)

                for cat in self[tracer].meta_categories:
                    tr.add_metacat(cat.name, cat.constructor, self[tracer].variables[cat.name].attrs)

                new.add_tracer(tr)

            elif lat or lon or grid:
                raise NotImplementedError
        return new

    def to_netcdf(self, filename, zlib=True, complevel=1, **kwargs):
        if not zlib :
            complevel = 0.
        encoding = dict(zlib=zlib, complevel=complevel)
        for tracer in self._tracers :
            self[tracer].to_netcdf(filename, group=tracer, encoding={var: encoding for var in self[tracer].data_vars}, engine='h5netcdf', **kwargs)

    @property
    def tracers(self):
        return list(self._tracers.keys())

    @property
    def units(self):
        return {tr : str(self[tr].units) for tr in self.tracers}
    
    @property
    def optimized_categories(self) -> List[Category] :
        """ Returns an iterable with each existing combination of tracer and optimized categories.
        This just avoids the nested loops "for tracer in self.tracers: for cat in self.tracers[tracer].optimized_categories ..."
        """
        cats = []
        for tracer in self.tracers :
            for cat in self[tracer].optimized_categories :
                cats.append(cat)
        return cats

    @property
    def transported_categories(self) -> List[Category] :
        """
        Return the list of transported emission categories (i.e. typically the meta-categories + the categories not part of any meta-category).
        """
        cats = []
        for tracer in self.tracers :
            for cat in self[tracer].transported_categories:
                cats.append(cat)
        return cats

    @property
    def categories(self) -> List[Category] :
        """ Returns an iterable with each existing combination of tracer and categories.
        This just avoids the nested loops "for tracer in self.tracers: for cat in self.tracers[tracer].categories ..."
        """
        cats = []
        for tracer in self.tracers :
            for cat in self[tracer].iter_cats() :
                cats.append(cat)
        return cats

    def copy(self, copy_emis : bool = True, copy_attrs : bool = True) -> "Data":
        """
        This returns a copy of the object, possibly without all the attributes
        The distinction between class and metaclass is respected.
        Arguments:
            copy_emis (optional, default True): copy the emissions from the source category to the new one
            copy_attrs (optional, default True): copy the attributes as well
        """
        new = Data()
        for tr in self._tracers.values():
            new.add_tracer(TracerEmis(
                tracer_name=tr.tracer,
                grid=tr.grid,
                time=tr.timestamp,
                units=tr.units,
                timestep=tr.period))
        
        if copy_emis :
            for cat in self.categories :
                attrs = self[cat.tracer][cat.name].attrs if copy_attrs else None
                if cat.meta :
                    new[cat.tracer].add_metacat(cat.name, self[cat.tracer][cat.name].constructor.dict, attrs=attrs)
                else :
                    new[cat.tracer].add_cat(cat.name, self[cat.tracer][cat.name].data.copy(), attrs=attrs)
        
        return new

    def empty_like(self, fillvalue = 0., copy_attrs: bool = True) -> "Data":
        """
        Returns a copy of the current Data structure, but with all data set to zero (or to the value provided by the optional "fillvalue" argument.
        """
        new = self.copy(copy_attrs = copy_attrs)
        new.set_zero()
        return new

    def set_zero(self, fillvalue = 0) -> None:
        for cat in self.categories:
            self[cat.tracer][cat.name].data[:] = fillvalue

    def resolve_metacats(self) -> None:
        for tr in self.tracers:
            self[tr].resolve_metacats()

    @classmethod
    def from_file(cls, filename : Union[str, Path], units: Union[str, dict, Unit, Quantity] = None) -> "Data":
        """
        Create a new "Data" object based on a netCDF file (such as previously written by Data.to_netcdf).
        Arguments:
            filename: path to the netCDF file
            units (ptional): convert the data in specific units. units can either be a string or a dictionary, in which case, each dictionary element gives the unit requested for each tracer. If no units is provided, use what's in the file.

        Usage:
            from xr import Data
            emis = Data.from_file(filename, units='PgC')
            emis = Data.from_file(filename, units={'co2':'PgC', 'ch4','TgCH4'})
        """

        em = cls()

        with Dataset(filename, 'r') as fid :
            for tracer in fid.groups:
                with xr.open_dataset(filename, group=tracer) as ds :
                    grid = Grid(latc=ds.lat.values, lonc=ds.lon.values)
                    em.add_tracer(TracerEmis(
                        tracer_name=tracer,
                        grid=grid,
                        time=ds.time,
                        units=ureg(ds.units),
                        timestep=ds.timestep))
                    if isinstance(ds.categories, str):
                        ds.attrs['categories'] = [ds.categories]
                    for cat in ds.categories :
                        if ds[cat].attrs.get('meta', False) :
                            em[tracer].add_metacat(cat, ds[cat].constructor, attrs=ds[cat].attrs)
                        else :
                            em[tracer].add_cat(cat, ds[cat].data, attrs=ds[cat].attrs)

                # Convert (if needed!):
                if units is not None:
                    if isinstance(units, (str, Unit, Quantity)):
                        em[tracer].convert(units)
                    elif isinstance(units, dict):
                        em[tracer].convert(units[tracer])
                    else :
                        logger.critical(f'Unrecognized type ({type(units)}) for argument "units" ')
                        raise NotImplementedError

                # Check if mapping datasets are also there:
                if 'temporal_mapping' in fid[tracer].groups:
                    em[tracer]._mapping = {
                        'time': xr.open_dataset(filename, group=f'{tracer}/temporal_mapping'),
                        'space': xr.open_dataset(filename, group=f'{tracer}/spatial_mapping')
                    }

        return em

    @classmethod
    def from_rc(cls, rcf: RcFile, start: Union[datetime, Timestamp, str], end: Union[datetime, str, Timestamp]) -> "Data":
        """
        Create a Data structure from a rc-file, with the following keys defined:
        - tracers
        - emissions.{tracer}.region (for each tracer defined by "tracers")
        - emissions.{tracer}.categories
        - emissions.{tracer}.interval
        - emissions.{tracer}.{cat}.origin
        - emissions.{tracer}.prefix

        Additionally, start and time arguments must be provided
        """
        em = cls()
        # TODO: we need to loop through the tracers, not hard-wire co2
        for tr in list(rcf.get('run.tracers')):

            # Create spatial grid - provided by minLat, maxLat, dLat, minLong, maxLong, dLong (e.g. Europe, quarter degree)
            #grid = grid_from_rc(rcf, name=rcf.get(f'emissions.{tr}.region'))
            grid = rcf.get(f'emissions.{tr}.region')

            # Create temporal grid:
            freq = rcf.get(f'emissions.{tr}.interval')  # get the time resolution requested in the rc file, key emissions.co2.interval, e.g. 1h
            time = date_range(start, end, freq=freq, inclusive='left') # the time interval requested in the rc file

            # Get tracer characteristics
            unit_emis = species[tr].unit_emis  # what units are the emissions data in? e.g. 'micromole / meter ** 2 / second'

            # Add new tracer to the emission object
            em.add_tracer(TracerEmis(tracer_name=tr,
                                     grid=grid,
                                     time=time,
                                     units=unit_emis,
                                     timestep=freq))  # .seconds * ur('s')))

            freq_src = rcf.get(f'emissions.{tr}.resample', default=False)
            if not freq_src:
                # If emissions.{tr}.resample is False/not defined, get the emissions at their native resolution.
                # otherwise, resample them from whatever the 'emissions.{tr}.resample' key points to
                freq_src = freq

            # Import emissions for each category of that tracer
            for cat in rcf.get(f'emissions.{tr}.categories'):
                origin = rcf.get(f'emissions.{tr}.categories.{cat}.origin', fallback=f'emissions.{tr}.categories.{cat}')
                logger.debug("tr.path= "+rcf.get(f'emissions.{tr}.path'))
                #logger.debug("tr.region= "+rcf.get(f'emissions.{tr}.region'))   # !! one cannot simply assign a Grid to a string
                regionGrid=rcf.get(f'emissions.{tr}.region')
                # print(regionGrid,  flush=True)
                sRegion="lon0=%.3f, lon1=%.3f, lat0=%.3f, lat1=%.3f, dlon=%.3f, dlat=%.3f, nlon=%d, nlat=%d"%(regionGrid.lon0, regionGrid.lon1,  regionGrid.lat0,  regionGrid.lat1,  regionGrid.dlon,  regionGrid.dlat,  regionGrid.nlon,  regionGrid.nlat)
                logger.debug("tr.region= "+ sRegion)
                logger.debug("freq_src= "+freq_src)
                logger.debug("tr.prefix "+rcf.get(f'emissions.{tr}.prefix'))
                logger.debug("origin="+origin)
                # emis = load_preprocessed(prefix, start, end, freq=freq, archive=rcf.get(f'emissions.{tr}.path'),  grid=grid)
                myPath2FluxData1=rcf.get(f'emissions.{tr}.path')
                myPath2FluxData3=rcf.get(f'emissions.{tr}.interval')
                myPath2FluxData2=''
                try:
                    myPath2FluxData2=rcf.get(f'emissions.{tr}.regionName')
                except:
                    print('Warning: No key emissions:TRACER:regionName found in user defined resource file (used in pathnames). I shall try to guess it...',  flush=True)
                    mygrid=rcf.get(f'emissions.{tr}.region')
                    if((250==int(mygrid.dlat*1000)) and (250==int(mygrid.dlon*1000)) and (abs((0.5*(mygrid.lat0+mygrid.lat1))-53)<mygrid.dlat)and (abs((0.5*(mygrid.lon0+mygrid.lon1))-10)<mygrid.dlon)):
                        myPath2FluxData2='eurocom025x025' # It is highly likely that the region is centered in Europe and has a lat/lon grid of a quarter degree
                    else:
                        print('Abort in lumia/formatter/xr.py: My guess of eurocom025x025 was not a very good guess. Please provide a emissions:TRACER:regionName key in your yml configuration file and try again.', flush=True)
                        sys.exit(1)
                if(myPath2FluxData1[-1]!=os.path.sep):
                    myPath2FluxData1=myPath2FluxData1+os.path.sep
                myPath2FluxData=myPath2FluxData1+myPath2FluxData2+os.path.sep+myPath2FluxData3
                if (os.path.sep!=myPath2FluxData[-1]):     # Does the path end in a directory separator (forward or back-slash depending on OS)?
                    myPath2FluxData=myPath2FluxData+os.path.sep
                myarchivePseudoDict='rclone:lumia:'+myPath2FluxData
                prefix = os.path.join(myPath2FluxData, rcf.get(f'emissions.{tr}.prefix') + origin + '.')
                logger.debug("prefix= "+prefix)
                # If the location in emissions.{tr}.location.{cat} is REMOTE, then we read that file directly from the carbon 
                # portal, else we assume it is available on the local system in the user-stated path.
                # if origin.startswith('@'): is now obsolete, because it is incompatible with the yaml naming rules
                sLocation=rcf.get(f'emissions.{tr}.location.{cat}')
                if ('REMOTE' in sLocation):
                    # we attempt to locate and read that flux information directly from the carbon portal - given that this code is executed on the carbon portal itself
                    sFileName = os.path.join(rcf.get(f'emissions.{tr}.prefix') + origin)
                    # TODO: archive fails to construct. should be something like fluxes/nc/eurocom025x025/1h/
                    # Hint from rclone: The default way to instantiate the Rclone archive is to pass a path, with the format: "rclone:remote:path". In that case, __post_init__ will then 
                    #                              split this into three attributes: protocol, remote and path.
                    # # archive could contain something like rclone:lumia:fluxes/nc/eurocom025x025/1h/
                    # emis =  load_preprocessed(prefix, start, end, freq=freq,  grid=grid, archive=rcf.get(f'emissions.{tr}.archive'), \
                    # myarchiveDict={'protocol':'rclone', 'remote':'lumia', 'path':myarchive+'eurocom025x025/1h/' }
                    emis =  load_preprocessed(prefix, start, end, freq=freq,  grid=grid, archive=myarchivePseudoDict, \
                                                                sFileName=sFileName,  bFromPortal=True,  iVerbosityLv=2)
                    print(emis.shape,  flush=True)
                else:
                    # myarchiveDict={'protocol':'rclone', 'remote':'lumia', 'path':myarchive+'eurocom025x025/1h/' }
                    emis = load_preprocessed(prefix, start, end, freq=freq, archive=myarchivePseudoDict,  grid=grid)
                    # self contains in its dictionary #    'emissions.co2.archive': 'rclone:lumia:fluxes/nc/${emissions.co2.region}/${emissions.co2.interval}/'
                    print(emis.shape,  flush=True)
                # emis is a Data object containing the emisions values in a lat-lon-timestep cube for one category
                em[tr].add_cat(cat, emis)  # collects the individual emis objects for biosphere, fossil, ocean into one data structure 'em'
        return em

def ensureCorrectGrid(sExistingFile,  grid: Grid = None):
    '''
    Function ensureCorrectGrid
    interpolate the spatial coordinates in sExistingFile if necessary
    
    We trust the climate data operator software CDO from Max Planck Hamburg to do a decent job on interpolation - and it is easy to use
    CDO Homepaqe:  https://code.mpimet.mpg.de/projects/cdo
    First we need the reference grid onto which to map. For this we extract it from any existing ICOS flux file with the correct grid, e.g.
    cdo griddes flux_co2.VPRM.2018.nc >cdo-icos-quarter-degree.grid
    Then we call cdo with the conservative remapping command (see below and study the cdo user guide). 
    We also double checked the output to make sure it was mapped correctly onto the provided grid.
    
        - if the resolution in sExistingFile is the desired one, then just return sExistingFile as the file name
        - use the existing matched grid file in case cdo has been called previously for the requested input file (see below) and return the name of that file 
        - else call cdo to interpolate to the user requested lat/lon grid, save its output to a predetermined location and put the dLat-dLon into the file extension and hand that output file back
    @param sExistingFile an existing netcdf data file like a co2 flux file or other with a lat/lon grid that cdo understands
    @type string
    @param grid required parameter that defines the extent and spatial resolution of the lat/lon rectangle requested (defaults to None)
    @type Grid (optional)
    '''
    if(grid is None) or (sExistingFile is None) :
        print("Fatal error in xr.ensureCorrectGrid(): no grid provided or no existing file provided.")
        sys.exit(1)
    # step 1: check if a file with the right spatial resolution already exist. If yes, return that file name and we are done
    # grid may look something like Grid(lon0=-15, lon1=35, lat0=33, lat1=73, dlon=0.25, dlat=0.25, nlon=200, nlat=160)
    # create the file name extension: lat and lon in degrees*1000
    sdlat=str(int(grid.dlat*1000))
    sdlon=str(int(grid.dlon*1000))
    fnameOut="."+os.path.sep+"regridded"+os.path.sep+sdlat+'x'+sdlon+os.path.sep+sExistingFile.split(os.path.sep)[-1] +".dLat"+sdlat+"dLon"+sdlon
    print('Hunting for flux input file '+fnameOut,  flush=True)
    try:
        # Have we created this file previously so we could simply read it instead of creating it first?
        f=open(fnameOut, 'rb')
        f.close()
    except:
        # No drama. We only need to create an interpolated version of the existing file
        # step 2: figure out the grid of the existing file sExistingFile
        # #   ncdump -h sExistingFile    or     cdo griddes sExistingFile
        # TODO: we cannot hard-wire the the name of the variable(s) to drop to "NEE" - either figure out how to read only the dimensions or how to 
        # determine the name(s) of the reported variable(s) so we can drop it/them
        xrExisting = xr.open_dataset(sExistingFile, drop_variables='NEE') # only read the dimensions
        fLats=xrExisting.lat
        fLons=xrExisting.lon
        d=dict(xrExisting.dims) # contains the shape of the existing file as {'lat':480, 'lon':400, 'time':8760}
        # print(fLats.values[d['lat'] - 1],  flush=True)
        LatWidth=fLats.values[d['lat'] - 1] - fLats.values[0]    # north-south-extent of the stored region in degrees latitude
        dLatExs=abs(LatWidth/(d['lat'] -1))                                      # stepsize or difference between nearest grid-points in degrees latitude
        LonWidth=fLons.values[d['lon'] - 1] - fLons.values[0]  # width/east-west extent of the stored region in degrees longitude
        dLonExs=abs(LonWidth/(d['lon'] - 1))                                    # stepsize or difference between nearest grid-points in degrees longitude
        
        # step 3: Then compare the two grids, that is to say the desired grid and the one extracted from the existing file
        if ((abs(grid.dlat - dLatExs) < 0.002) and (abs(grid.dlon - dLonExs) < 0.002)):
            if ((grid.nlat==d['lat']) and (grid.nlon==d['lon'])):
                if ((abs((grid.lat0+0.5*grid.dlat) - fLats.values[0]) < 0.01) and (abs((grid.lon0+0.5*grid.dlon) - fLons.values[0]) < 0.01)):
                    return(sExistingFile)  # The original file already matches the user-requested grid. Thus, just hand that name back.
        # step 4: call cdo and write the interpolated output file into pre-determined hierarchies and append an extension to the PID based on spatial resolution aka 
        #             unique output file name. Upon success, the new file name is then returned by this function.
        # Example for calling cdo: cdo remapcon,cdo-icos-quarter-degree.grid  /data/dataAppStorage/netcdf/xLjxG3d9euFZ9SOUj69okhaU ./250/xLjxG3d9euFZ9SOUj69okhaU.dLat250dLon250
        fRefGridFile="."+os.path.sep+"regridded"+os.path.sep+sdlat+'x'+sdlon+os.path.sep+'cdo-icos-'+"dLat"+sdlat+"dLon"+sdlon+'-reference.grid'
        try:
            # Have we created this file previously so we could simply read it instead of creating it first?
            f=open(fRefGridFile, 'rb')
            f.close()
        except:
            print('Fatal error: Cannot find the grid file '+fRefGridFile+' below your working folder. Either copy it there or create the file with')
            print('cdo griddes YOUR_ANY_NETCDFFILE_ON_DESIRED_GRID >'+fRefGridFile)
            print('Next time Lumia automatically creates the regridded data file by executing:')
            print('cdo remapcon,'+fRefGridFile+' '+sExistingFile+'  '+"."+os.path.sep+"regridded"+os.path.sep+sdlat+'x'+sdlon+os.path.sep+os.path.basename(sExistingFile)+'.'+"dLat"+sdlat+"dLon"+sdlon, flush=True)
            # print('cdo  remapcon,cdo-icos-quarter-degree.grid  /data/dataAppStorage/netcdf/xLjxG3d9euFZ9SOUj69okhaU ./250/xLjxG3d9euFZ9SOUj69okhaU.dLat250dLon250', flush=True)
            sys.exit(-1)
        os.system("mkdir -p 250")  # We may need to create the folder as well.
        cdoCmd='cdo remapcon,cdo-icos-quarter-degree.grid  '+sExistingFile+' '+fnameOut
        try:
            # Call CDO in an external subprocess
            # The Eric7 remote debugger does not like the subprocess command and does weird stuff.
            # I got the error: cdo (Abort): Operator missing, /opt/conda/envs/lumia/lib/python3.9/site-packages/eric7/DebugClients/Python/DebugClient.py is a file on disk!
            #     subprocess.run(cdoCmd)
            os.system(cdoCmd)
        except:
            print("Fatal error: Calling cdo failed. Please make sure cdo is installed and working for you. Try running >>"+cdoCmd+"<< yourself in your working directory before running Lumia again.")
            sys.exit(-1)
        try:
            # Did cdo create the re-gridded flux file as expected?
            f=open(fnameOut, 'rb')
            f.close()
        except:
            print("Fatal error: cdo did not create the re-gridded output file "+fnameOut+" as expected from the command >>cdo "+cdoCmd+"<<.")
        else:
            return(fnameOut)
    else:
        return(fnameOut)
    return(fname)
    
def load_preprocessed(prefix: str, start: datetime, end: datetime, freq: str = None, grid: Grid = None, archive: str = None,  sFileName: str=None,  bFromPortal =False, iVerbosityLv=1) -> ndarray:
    #
    # The pre-processed data used by Lumia (as a-priori) is described e.g. here:
    # https://meta.icos-cp.eu/objects/sNkFBomuWN94yAqEXXSYAW54
    # There you can find links to the anthropogenic (EDGARv4.3), ocean (Mikaloff-Fletcher 2007) and to the diagnostic biosphere model VPRM
    #
    # archive could contain something like rclone:lumia:fluxes/nc/eurocom025x025/1h/
    archive = Rclone(archive)
    # archive is now a structure with main values: Rclone(protocol='rclone', remote='lumia', path='fluxes/nc/eurocom025x025/1h/' )
    # in case of reading from the carbon portal the archive.path variable is not used, so no need to get fancy here 
    
    # It is helpful to know how the data is organised. 
    # Downloaded files are already sliced to the area needed at a quarter degree resolution: Grid(lon0=-15, lon1=35, lat0=33, lat1=73, dlon=0.25, dlat=0.25, nlon=200, nlat=160)
    # but the carbon portal files are different: npts\(time:8760 (1yr hourly), lat:480 , lon:400) and need to be harmonised
    # The ranges and step sizes of the dimensions time, lat, lon are NOT contained in the netcdf header (as perhaps they should be).
    # Looking at the 16 Gbyte  ncdump of the VPRM data set I can see that the same lat/lon area is stored within, but with a stepsize 
    # of 1/8 of a degree (as opposed to a 1/4 degree). So we need to interpolate and reduce the number of data points.
    
    # Import a file for each year at least partially covered:
    years = unique(date_range(start, end, freq='MS', inclusive='left').year)
    data = []
    for year in years :
        if(bFromPortal):
            # sSearchMask='flux_co2.VPRM' 
            words = sFileName.split('.')
            sKeyWord=words[-1]
            # co2 fluxes could be local file names like flux_co2.EDGARv4.3_BP2019.2018.nc, flux_co2.VPRM.2018.nc and 
            # flux_co2.mikaloff01.2018.nc for anthropogenic, vegetation model and ocean model co2 fluxes
            # VPRM is straight forward to find with SPARQL. EDGAR and mikaloff are not
            # Hunting on the carbon portal I eventually found the EDGAR 2019 data at
            # https://www.icos-cp.eu/data-products/GFNT-5Y47
            # Which offered a link to "View in the data portal" (big turquoise button to the right):
            # https://data.icos-cp.eu/portal/#%7B%22filterCategories%22%3A%7B%22project%22%3A%5B%22misc%22%5D%2C%22type%22%3A%5B%22co2EmissionInventory%22%5D%2C%22submitter%22%3A%5B%22oCP%22%5D%2C%22level%22%3A%5B3%5D%7D%7D
            # 
            sScndKeyWord=None
            if (('co2' in sFileName)and(sKeyWord=='VPRM')):  # TODO or if it is LPJGUESS...
                sScndKeyWord='NEE' # we want the net exchange of carbon
            if (('co2' in sFileName)and(sKeyWord[:8]=='3_BP2019')):  # TODO: This needs to become smarter.....
                if(words[-2]=='EDGARv4'):
                    sKeyWord='anthropogenic' 
                    sScndKeyWord='EDGARv4.3' 
            fname=fromICP.readLv3NcFileFromCarbonPortal(sKeyWord, None, None, year,  sScndKeyWord,  iVerbosityLv=2)
            if(fname is None):
                print('Abort in lumia/formatter/xr.py: '+sKeyWord+' '+sScndKeyWord+' file '+fname+' is not found at the given path.',  flush=True)
                sys.exit(1)
        else:
            fname = f'{prefix}{year}.nc'
            try:
                archive.get(fname)
            except:
                print('Abort in lumia/formatters/xr.py: Unable to obtain archive.get(fname) with fname='+fname,  flush=True)
                sys.exit(1)
        # issue: downloaded files are already sliced to the area needed: Grid(lon0=-15, lon1=35, lat0=33, lat1=73, dlon=0.25, dlat=0.25, nlon=200, nlat=160)
        # but some carbon portal files (like VPRM fluxes)  are at a higher spatial resolution
        fname=ensureCorrectGrid(fname,  grid)  # interpolate if necessary and return the name of the file with the user requested lat/lon grid resolution  
        try:
            data.append(xr.load_dataarray(fname))
        except:
            print('Abort in lumia/formatters/xr.py: Unable to xr.load_dataarray(fname) with fname='+fname,  flush=True)
            sys.exit(1)
        # TODO: Issue: files on the carbon portal may have their time axis apparently shifted by one time step, because I found netcdf
        # co2 flux files that use the END of the time interval for the observation times reported: time:long_name = "time at end of interval" ;
        # cdo shifttime,-1hour xLjxG3d9euFZ9SOUj69okhaU.dLat250dLon250.eots xLjxG3d9euFZ9SOUj69okhaU.dLat250dLon250
        # TODO: This needs to be made smarter so we can call CDO and fix the time axis no matter what.....
        # print(slice(start, end),  flush=True)
    data = xr.concat(data, dim='time').sel(time=slice(start, end))
    # print(data.time)
    # print(data['time'][0])
    # print('start=')
    # print(start,  flush=True)
    # print('xr=')
    # print(xr,  flush=True)
    print('data.time=')
    print(data.time,  flush=True)

    # Resample if needed
    if freq is not None :
        times_dest = date_range(start, end, freq=freq, inclusive='left')  # starts correctly with the left boundary and excludes the right boundary
        print('times_dest=')
        print(times_dest,  flush=True)
        tres1 = Timestamp(data.time.data[1])-Timestamp(data.time.data[0])
        tres2 = times_dest[1]-times_dest[0]
        if tres1 != tres2 :
            assert tres1 > tres2, f"Temporal resolution can only be upscaled (resolution in the data files: {tres1}; requested resolution: {tres2})"
            assert (tres1 % tres2).total_seconds() == 0
            logger.info(f"Increase the resolution of the emissions from {tres1.total_seconds()/3600:.0f}h to {tres2.total_seconds()/3600:.0f}h")
            data = data.reindex(time=times_dest).ffill('time')

    times = data.time.to_pandas()  
    # print('times=')
    # print(times,  flush=True) 
    data = data[(times >= start) * (times < end), :, :]
    # print(data.time)

    # Coarsen if needed
    # if grid is not None :
    # obsolete - that's what ensureCorrectGrid() is for....    raise NotImplementedError

    return data.data


# Interfaces:
def WriteStruct(data: Data, path: str, prefix=None, zlib=False, complevel=1, only_transported=False):
    if prefix is None :
        filename, path = path, os.path.dirname(path)
    else :
        filename = os.path.join(path, f'{prefix}.nc')
    data.to_netcdf(filename, zlib=zlib, complevel=complevel, only_transported=only_transported)
    return filename


def ReadStruct(path, prefix=None, categories=None):
    if categories is not None :
        logger.warning(f"categories argument ignored (not implemented yet)")
    filename = path
    if prefix is not None :
        filename = os.path.join(path, f'{prefix}.nc')
    return Data.from_file(filename)
