from re import I
from sqlite3 import Date
from pint import Quantity
import xarray as xr
from numpy import ndarray, unique
from dataclasses import field, dataclass
from gridtools import Grid, grid_from_rc
from rctools import RcFile
from datetime import datetime
from pandas import date_range, Timestamp
from loguru import logger
from lumia.tracers import tracers
from lumia.units import units_registry as ur
from pandas.tseries.frequencies import to_offset, DateOffset
from netCDF4 import Dataset
from lumia.Tools.time_tools import periods_to_intervals


def offset_to_pint(offset: DateOffset):
    try :
        return (offset.nanos * 1.e-9) * ur.s
    except ValueError :
        if offset.freqstr in ['M', 'MS'] :
            return offset.n * ur.month
        elif offset.freqstr in ['A', 'AS', 'Y', 'YS']:
            return offset.n * ur.year
        elif offset.freqstr == 'W':
            return offset.n * ur.week


class TracerEmis(xr.Dataset):
    __slots__ = 'shape', 'categories', 'grid'
    def __init__(self, tracer_name, grid, time, units, timestep, attrs=None):
        super().__init__(
            coords=dict(time=time, lat=grid.latc, lon=grid.lonc),
            attrs=attrs
        )
        self.attrs['tracer'] = tracer_name
        self.shape = self.dims['time'], self.dims['lat'], self.dims['lon']
        self.grid = grid
        self['area'] = xr.DataArray(data=grid.area * ur('m**2'), dims=['lat', 'lon'])

        # Time units:
        # * timestep attribute: stores the timestep unit and length
        # * nsec variable: stores the timestep length, in seconds (or nanoseconds?)
        # * time: store the time period

        # timestep stores the time step, in time units (seconds, days, months, etc.)
        # while dt stores the time interval in nanoseconds (pandas Timedelta).
        self.attrs['timestep'] = timestep
        self['timestep_length'] = (time + timestep - time).total_seconds() * ur('s')
        self.categories = []
        self.attrs['units'] = units

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

    def add_cat(self, name: str, value: ndarray):
        assert isinstance(value, ndarray), logger.error(f"The value provided is not a numpy array ({type(value) = }")
        assert value.shape == self.shape, logger.error(f"Shape mismatch between the value provided ({value.shape}) and the rest of the dataset ({self.shape})")
        self[name] = xr.DataArray(value * self.units, dims=['time', 'lat', 'lon'])
        self.categories.append(name)

    def print_summary(self, units='PgC'):
        original_unit = self.units
        self.convert(units)
        
        for cat in self.categories :
            monthly_emis = self[cat].resample(time='M').sum(['lat', 'lon', 'time'])
            logger.info(f"===============================")
            logger.info(f"{cat}:")
            for year in unique(monthly_emis.time.dt.year):
                logger.info(f'{year}:')
                monthly_emis_year = monthly_emis.where(monthly_emis.time.dt.year == year)
                for em in monthly_emis_year :
                    logger.info(f'  {em.time.dt.strftime("%B").data}: {em.data.m:7.2f} {units}')
                logger.info("    --------------------------")
                logger.info(f"   Total : {monthly_emis_year.sum().data.magnitude:7.2f} {units}")
        self.convert(original_unit)

    def to_extensive(self):
        new_unit = self.units * ur('s') * ur('m**2')
        self.convert(str(new_unit.u))
        assert self.unit.dimensionality('[time]') == 0
        assert self.unit.dimensionality('[length]') == 0

    def to_intensive(self):
        new_unit = self.units / ur.s / ur.m**2
        self.convert(str(new_unit.u))
        assert self.unit.dimensionality('[time]') == -1
        assert self.unit.dimensionality('[length]') == -2

    def convert(self, destunit):
        dest = destunit
        if isinstance(destunit, str):
            dest = ur(destunit)

        for cat in self.categories :
            # Check if we need to multiply or divide by time and area:

            power_t = (dest / self[cat].data.units).dimensionality.get('[time]')
            power_s = (dest / self[cat].data.units).dimensionality.get('[length]')

            if power_s == 2 :
                self[cat] = self[cat] * self.area 
            elif power_s == -2 :
                self[cat] = self[cat] / self.area
            elif power_s != 0 :
                raise RuntimeError(f"Unexpected units conversion request: {self[cat].data.unit} to {dest} ({power_s = })")

            if power_t == 1 :
                self[cat] = self[cat] * self.timestep
            elif power_t == -1 :
                self[cat] = self[cat] / self.timestep
            elif power_t != 0 :
                raise RuntimeError(f"Unexpected units conversion request: {self[cat].data.unit} to {dest} ({power_t =})")

            self[cat].data = self[cat].data.to(destunit)
        self.attrs['units'] = dest

    def to_netcdf(self, *args, **kwargs):
        for var in self.variables :
            if isinstance(self[var].data, Quantity):
                u = self[var].data.units
                self[var].data = self[var].data.magnitude
                self[var].attrs['units'] = str(u)
        self.attrs['dt'] = str(self.dt)
        self.attrs['units'] = str(self.units)
        super().to_netcdf(*args, **kwargs)

    
@dataclass
class Emis:
    tracers : dict = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.tracers, TracerEmis):
            self.tracers = {self.tracers.name: self.tracers}
        else :
            for tr in self.tracers :
                self.tracers.append(tr)
        for tr in self.tracers :
            setattr(self, tr, self.tracers[tr])
    
    def add_tracer(self, tracer: TracerEmis):
        self.tracers[tracer.tracer] = tracer
        setattr(self, tracer.tracer, self.tracers[tracer.tracer])

    def print_summary(self):
        for tracer in self.tracers :
            self[tracer].print_summary()

    def __getitem__(self, item):
        return self.tracers[item]

    def to_extensive(self):
        """
        Convert the data to extensive units (e.g. umol, PgC)
        """
        for tr in self.tracers :
            self[tr].to_extensive()

    def to_intensive(self):
        """
        Convert the data to intensive units (e.g. umol/m2/s, PgC/m2/s)
        """
        for tr in self.tracers :
            self[tr].to_intensive()


def CreateEmis(rcf: RcFile, start: datetime, end: datetime) -> Emis:
    em = Emis()
    for tracer in rcf.get('tracers', tolist='force'):

        # Create spatial grid
        grid = grid_from_rc(rcf, name=rcf.get(f'emissions.{tracer}.region'))

        # Create temporal grid:
        #freq = Timedelta(rcf.get(f'emissions.{tracer}.interval'))
        freq = to_offset(rcf.get(f'emissions.{tracer}.interval'))
        time = date_range(start, end, freq=freq, inclusive='left')
        #time = period_range(start, end, freq=freq)[:-1]

        # Get tracer characteristics
        tr = tracers.get(tracer)

        # Add new tracer to the emission object
        em.add_tracer(TracerEmis(tracer, grid, time, tr.unit_emis, freq))#.seconds * ur('s')))

        # Import emissions for each category of that tracer
        for cat in rcf.get(f'emissions.{tracer}.categories'):
            origin = rcf.get(f'emissions.{tracer}.{cat}.origin')
            prefix = rcf.get(f'emissions.{tracer}.prefix') + origin + '.'
            emis = load_preprocessed(prefix, start, end, freq=freq)
            em[tracer].add_cat(cat, emis)
    return em


def load_preprocessed(prefix: str, start: datetime, end: datetime, freq: str=None, grid: Grid=None) -> ndarray:
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


def read_nc(filename):
    em = Emis()
    with Dataset(filename, 'r') as fid :
        for tracer in fid.groups:
            with xr.open_dataset(filename, group=tracer) as ds :
                grid = Grid(latc=ds.lat, lonc=ds.lon)
                unit = ur(ds.unit)
                dt = ur(ds.dt)
                em.add_tracer(TracerEmis(tracer, grid, ds.time, unit, dt))
                em[tracer]._variables = ds._variables
                for var in em[tracer].variables :
                    if 'units' in em[tracer][var].attrs :
                        em[tracer][var].data = em[tracer][var].data * ur(em[tracer][var].units)
    return em