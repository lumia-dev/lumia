import os
from typing import Union
from pint import Quantity
import xarray as xr
from dataclasses import dataclass, field, asdict
from numpy import isin, ndarray, unique, array, zeros, nan
from gridtools import Grid
from rctools import RcFile
from datetime import datetime
from pandas import Timestamp, DatetimeIndex
from loguru import logger
from lumia.units import units_registry as ureg
from gridtools import grid_from_rc
from pandas import date_range
from pandas.tseries.frequencies import DateOffset, to_offset
from lumia.tracers import species, Unit
from lumia.Tools.time_tools import periods_to_intervals
from netCDF4 import Dataset
import numbers


@dataclass
class Category:
    name      : str
    optimized : bool = False
    optimization_interval : DateOffset = None
    apply_lsm : bool = True
    is_ocean  : bool = False
    n_optim_points : int = None
    tracer    : str = None
    horizontal_correlation : str = None
    temporal_correlation   : str = None
    total_uncertainty : float = nan
    unit_emis : Quantity = None
    unit_mix : Quantity = None
    unit_budget : Quantity = None
    unit_optim  : Quantity = None

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
    __slots__ = 'shape', 'grid', '_mapping'

    def __init__(self, tracer_name, grid, time: DatetimeIndex, units: Quantity, timestep: DateOffset, attrs=None, categories: dict=None):
        # Ensure we have the correct data types:
        time = DatetimeIndex(time)
        timestep = to_offset(timestep)

        super().__init__(
            coords=dict(time=time, lat=grid.latc, lon=grid.lonc),
            attrs=attrs
        )
        #self.categories = []
        self.attrs['tracer'] = tracer_name
        self.attrs['categories'] = []
        self._mapping = {'time':None, 'space':None} #TODO: replace by a dedicated class?
        self.shape = self.dims['time'], self.dims['lat'], self.dims['lon']
        self.grid = grid
        self['area'] = xr.DataArray(data=grid.area, dims=['lat', 'lon'], attrs={'units': ureg('m**2').units})
        # timestep stores the time step, in time units (seconds, days, months, etc.)
        # while dt stores the time interval in nanoseconds (pandas Timedelta).
        self.attrs['timestep'] = timestep 
        self['timestep_length'] = xr.DataArray((time + timestep - time).total_seconds().values, dims=['time',], attrs={'units': ureg.s})
        self.attrs['units'] = units

        # If any field has been passed to the constructor, add it here:
        if categories is not None :
            for cat, value in categories.items() :
                if isinstance(value, dict) :
                    self.add_cat(cat, value['data'], value.get('attrs', None))
                else :
                    self.add_cat(cat, value)

    def iter_cats(self):
        for cat in self.attrs['categories'] :
            yield Category.from_dict(cat, {**self[cat].attrs, **self.attrs, **species[self.tracer].__dict__})

    @property
    def optimized_categories(self):
        return [c for c in self.iter_cats() if c.optimized]

    @property
    def period_index(self):
        """
        Provides a pandas "PeriodIndex" view of the time coordinate
        """
        return self.time.to_index().to_period(self.attrs['timestep'])

    @property
    def intervals(self):
        return periods_to_intervals(self.period_index)

    @property
    def timestep(self):
        return offset_to_pint(self.attrs['timestep'])

    @property
    def period(self):
        return self.attrs['timestep']

    @property
    def timestamp(self):
        return array([Timestamp(t) for t in self.time.data])

    @property
    def start(self):
        return Timestamp(self.time.min().values)

    @property
    def end(self):
        return Timestamp(self.time.max().values) + self.attrs['timestep']

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

    def add_cat(self, name: str, value: ndarray, attrs: dict = None):
        if isinstance(value, numbers.Number):
            value = zeros(self.shape) + value
        assert isinstance(value, ndarray), logger.error(f"The value provided is not a numpy array ({type(value) = }")
        assert value.shape == self.shape, logger.error(f"Shape mismatch between the value provided ({value.shape}) and the rest of the dataset ({self.shape})")
        if attrs is None:
            attrs = {'tracer': self.tracer}
        self[name] = xr.DataArray(value, dims=['time', 'lat', 'lon'], attrs=attrs)
        self.attrs['categories'].append(name)

    def print_summary(self, units=None):
        if units is None :
            units = species[self.tracer].unit_budget
        original_unit = self.units
        self.convert(units)
        
        for cat in self.categories :
            monthly_emis = self[cat].resample(time='M').sum(['lat', 'lon', 'time'])
            logger.info("===============================")
            logger.info(f"{cat}:")
            for year in unique(monthly_emis.time.dt.year):
                logger.info(f'{year}:')
                monthly_emis_year = monthly_emis.where(monthly_emis.time.dt.year == year)
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

    def convert(self, destunit: Unit):
        dest = destunit
        if isinstance(destunit, str):
            dest = ureg(destunit).units

        for cat in self.categories :
            # Check if we need to multiply or divide by time and area:

            power_t = (dest / self.units).dimensionality.get('[time]')
            power_s = (dest / self.units).dimensionality.get('[length]')

            catunits = self.units

            # from units/m2 to units/gricell
            if power_s == 2 :
                self[cat].data *= self.area.data
                catunits *= self.area.units
            # from units/gridcell to units/m2
            elif power_s == -2 :
                self[cat].data /= self.area.data
                catunits /= self.area.units
            elif power_s != 0 :
                raise RuntimeError(f"Unexpected units conversion request: {self[cat].data.unit} to {dest} ({power_s = })")

            # From units/s to units/tstep
            if power_t == 1 :
                self[cat].data = (self[cat].data.swapaxes(0, -1) * self.timestep_length.data).swapaxes(0, -1)
                catunits *= self.timestep_length.units
            # From units/tstep to units/s
            elif power_t == -1 :
                self[cat].data = (self[cat].data.swapaxes(0, -1) / self.timestep_length.data).swapaxes(0, -1)
                catunits /= self.timestep_length.units
            elif power_t != 0 :
                raise RuntimeError(f"Unexpected units conversion request: {self[cat].data.units} to {dest} ({power_t =})")

            # Finally, convert:
            self[cat].data = (self[cat].data * catunits).to(dest).magnitude
            
        self.attrs['units'] = dest

    def to_netcdf(self, *args, **kwargs):
        # Convert attributes in problematic format
        self.attrs['timestep'] = self.period.freqstr
        self.attrs = attrs_to_nc(self.attrs)
        for var in self.data_vars :
            self[var].attrs = attrs_to_nc(self[var].attrs)
        
        # write ncfile
        super().to_netcdf(*args, **kwargs)

        # revert the attributes
        self.attrs = nc_to_attrs(self.attrs)
        for var in self.data_vars :
            self[var].attrs = nc_to_attrs(self[var].attrs)
        self.attrs['timestep'] = to_offset(self.attrs['timestep'])


def attrs_to_nc(attrs: dict) -> dict:
    to_bool = []
    to_units = []
    for k, v in attrs.items():
        if isinstance(v, bool):
            attrs[k] = int(v)
            to_bool.append(k)
        if isinstance(v, Unit):
            attrs[k] = str(v)
            to_units.append(k)

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
        Convert the data to intensive units (e.g. umol/m2/s, PgC/m2/s)
        """
        for tr in self._tracers :
            self[tr].to_intensive_adj()

    def to_netcdf(self, filename, zlib=True, complevel=1):
        if not zlib :
            complevel = 0.
        encoding = dict(zlib=zlib, complevel=complevel)
        for tracer in self._tracers :
            self[tracer].to_netcdf(filename, group=tracer, encoding={var: encoding for var in self[tracer].data_vars}, engine='h5netcdf')

    @property
    def tracers(self):
        return self._tracers.keys()
    
    @property
    def optimized_categories(self) -> Category :
        """ Returns an iterable with each existing combination of tracer and optimized categories.
        This just avoids the nested loops "for tracer in self.tracers: for cat in self.tracers[tracer].optimized_categories ..."
        """
        cats = []
        for tracer in self.tracers :
            for cat in self[tracer].optimized_categories :
                cats.append(cat)
        return cats

    @property
    def categories(self) -> Category :
        """ Returns an iterable with each existing combination of tracer and categories.
        This just avoids the nested loops "for tracer in self.tracers: for cat in self.tracers[tracer].categories ..."
        """
        cats = []
        for tracer in self.tracers :
            for cat in self[tracer].iter_cats() :
                cats.append(cat)
        return cats

    def new(self, copy_emis=True, copy_attrs=True):
        """
        This returns a copy of the object, possibly without all the attributes
        """
        new = Data()
        for tr in self._tracers.values() :
            new.add_tracer(TracerEmis(tr.tracer, tr.grid, tr.timestamp, tr.units, tr.period))
        
        if copy_emis :
            for cat in self.categories :
                new[cat.tracer].add_cat(cat.name, self[cat.tracer][cat.name].data.copy(), attrs=self[cat.tracer][cat.name].attrs)
        
        return new

    @classmethod
    def from_file(cls, filename : str):
        em = cls()
        with Dataset(filename, 'r') as fid :
            for tracer in fid.groups:
                with xr.open_dataset(filename, group=tracer) as ds :
                    grid = Grid(latc=ds.lat, lonc=ds.lon)
                    em.add_tracer(TracerEmis(tracer, grid, ds.time, ureg(ds.units), to_offset(ds.timestep)))
                    if isinstance(ds.categories, str):
                        ds.attrs['categories'] = [ds.categories]
                    for cat in ds.categories :
                        em[tracer].add_cat(cat, ds[cat].data, attrs=ds[cat].attrs)
        return em

    @classmethod
    def from_rc(cls, rcf:RcFile, start: Union[datetime, Timestamp, str], end: Union[datetime, str, Timestamp]) -> "Data":
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
        for tr in rcf.get('tracers', tolist='force'):

            # Create spatial grid
            grid = grid_from_rc(rcf, name=rcf.get(f'emissions.{tr}.region'))

            # Create temporal grid:
            freq = rcf.get(f'emissions.{tr}.interval')
            time = date_range(start, end, freq=freq, inclusive='left')

            # Get tracer characteristics
            unit_emis = species[tr].unit_emis

            # Add new tracer to the emission object
            em.add_tracer(TracerEmis(tr, grid, time, unit_emis, freq))#.seconds * ur('s')))

            if rcf.get(f'emissions.{tr}.resample'):
                freq_src = rcf.get(f'emissions.{tr}.convert_from')
            else :
                freq_src = freq

            # Import emissions for each category of that tracer
            for cat in rcf.get(f'emissions.{tr}.categories'):
                origin = rcf.get(f'emissions.{tr}.{cat}.origin')
                prefix = os.path.join(rcf.get('emissions.{tr}.path'), rcf.get(f'emissions.{tr}.region'), freq_src, rcf.get(f'emissions.{tr}.prefix') + origin + '.')
                emis = load_preprocessed(prefix, start, end, freq=freq)
                em[tr].add_cat(cat, emis)
        return em


def load_preprocessed(prefix: str, start: datetime, end: datetime, freq: str = None, grid: Grid = None) -> ndarray:
    # Import a file for each year at least partially covered:
    years = unique(date_range(start, end, freq='YS', inclusive='left').year)
    data = []
    for year in years :
        fname = f'{prefix}{year}.nc'
        data.append(xr.load_dataarray(fname))
    data = xr.concat(data, dim='time').sel(time=slice(start, end))

    # Resample if needed
    if freq is not None :
        times_dest = date_range(start, end, freq=freq, inclusive='left')
        tres1 = Timestamp(data.time.data[1])-Timestamp(data.time.data[0])
        tres2 = times_dest[1]-times_dest[0]
        if tres1 != tres2 :
            assert tres1 > tres2, f"Temporal resolution can only be upscaled (resolution in the data files: {tres1}; requested resolution: {tres2})"
            assert (tres1 % tres2).total_seconds() == 0
            logger.info(f"Increase the resolution of the emissions from {tres1.total_seconds()/3600:.0f}h to {tres2.total_seconds()/3600:.0f}h")
            data = data.reindex(time=times_dest).ffill('time')

    times = data.time.to_pandas()
    data = data[(times >= start) * (times < end), : ,:]

    # Coarsen if needed
    if grid is not None :
        raise NotImplementedError

    return data.data


# Interfaces:
def WriteStruct(data:Data, path:str, prefix=None, zlib=False, complevel=1):
    if prefix is None :
        filename, path = path, os.path.dirname(path)
    else :
        filename = os.path.join(path, f'{prefix}.nc')
    data.to_netcdf(filename, zlib=zlib, complevel=complevel)
    return filename


def ReadStruct(path, prefix=None, categories=None):
    if categories is not None :
        logger.warning(f"categories argument ignored (not implemented yet)")
    filename = path
    if prefix is not None :
        filename = os.path.join(path, f'{prefix}.nc')
    return Data.from_file(filename)