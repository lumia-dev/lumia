import os
from netCDF4 import Dataset
from numpy import array, zeros, arange, array_equal
from datetime import datetime
from lumia.Tools.system_tools import checkDir
import logging
from tqdm import tqdm
from numpy import unique, append
import xarray as xr
from pandas import Timestamp
from lumia.Tools.regions import region
from archive import Archive

logger = logging.getLogger(__name__)


class Emissions:
    def __init__(self, rcf, start, end):
        self.start = start
        self.end = end
        self.rcf = rcf

        self.tracers = {}
        self.data = {}

        for tr in list(rcf.get('obs.tracers')):
            self.tracers[tr] = {}
            self.tracers[tr]['categories'] = dict.fromkeys(rcf.get(f'emissions.{tr}.categories'))
            for cat in self.tracers[tr]['categories']:
                self.tracers[tr]['categories'][cat] = rcf.get(f'emissions.{tr}.{cat}.origin')
            self.data[tr] = ReadArchive(rcf.get(f'emissions.{tr}.prefix'), self.start, self.end, categories=self.tracers[tr]['categories'], archive=rcf.get('emissions.archive'))
            if rcf.get('optim.unit.convert', default=False):
                self.data[tr].to_extensive()   # Convert to umol
            

class Struct(dict):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def between_times(self, tbeg, tend):
        """
        Return a structure with only the data that fall within the specified time interval (must be within
        the time interval of the initial Struct).
        This is mainly for postprocessing purposes.
        """
        outstruct = Struct()
        for tracer in self.keys():
            outstruct[tracer] = {}
            for cat in self[tracer].keys():
                beg = self[tracer][cat]['time_interval']['time_start']
                end = self[tracer][cat]['time_interval']['time_end']
                assert tbeg in beg and tend in end, f"The requested time boundaries ({tbeg} to {tend}) are not available for category {cat}."
                select = (beg >= tbeg) & (end <= tend)
                outstruct[tracer][cat] = {
                    'emis': self[tracer][cat]['emis'][select, :, :],
                    'time_interval':{
                        'time_start':self[tracer][cat]['time_interval']['time_start'][select],
                        'time_end':self[tracer][cat]['time_interval']['time_end'][select]
                    },
                    'lats':self[tracer][cat]['lats'],
                    'lons':self[tracer][cat]['lons']
                }
        return outstruct

    def to_extensive(self):
        # assert self.unit_type == 'intensive'
        for tr in self.tracers.keys():
            for cat in self.tracers[tr]['categories'].keys():
                dt = self[tr][cat]['time_interval']['time_end']-self[tr][cat]['time_interval']['time_start']
                dt = array([t.total_seconds() for t in dt])
                area = region(longitudes=self[tr][cat]['lons'], latitudes=self[tr][cat]['lats']).area
                self[tr][cat]['emis'] *= area[None, :, :]
                self[tr][cat]['emis'] *= dt[:, None, None]
            self.unit_type = 'extensive'

    def to_intensive(self):
        #assert self.unit_type == 'extensive'
        for tr in self.tracers.keys():
            for cat in self.tracers[tr]['categories'].keys():
                dt = self[tr][cat]['time_interval']['time_end']-self[tr][cat]['time_interval']['time_start']
                dt = array([t.total_seconds() for t in dt])
                area = region(longitudes=self[tr][cat]['lons'], latitudes=self[tr][cat]['lats']).area
                self[tr][cat]['emis'] /= area[None, :, :]
                self[tr][cat]['emis'] /= dt[:, None, None]
            self.unit_type = 'intensive'


def WriteStruct(data, path, prefix=None): ### Add tracers
    """
    Write the model input (control parameters)
    """

    # Create the filename and directory (if needed)
    if prefix is None :
        filename, path = path, os.path.dirname(path)
    else :
        filename = os.path.join(path, '%s.nc' % prefix)
    checkDir(path)

    # Write to a netCDF format
    with Dataset(filename, 'w') as ds:
        ds.createDimension('time_components', 6)
        tracers = data.keys()
        for tr in tracers:
            ds.createGroup(tr)
            for cat in [c for c in data[tr].keys() if 'cat_list' not in c]:
                gr = ds[tr].createGroup(cat)
                gr.createDimension('nt', data[tr][cat]['emis'].shape[0])
                gr.createDimension('nlat', data[tr][cat]['emis'].shape[1])
                gr.createDimension('nlon', data[tr][cat]['emis'].shape[2])
                gr.createVariable('emis', 'd', ('nt', 'nlat', 'nlon'))
                gr['emis'][:] = data[tr][cat]['emis']
                gr.createVariable('times_start', 'i', ('nt', 'time_components'))
                gr['times_start'][:] = array([x.timetuple()[:6] for x in data[tr][cat]['time_interval']['time_start']])
                gr.createVariable('times_end', 'i', ('nt', 'time_components'))
                gr['times_end'][:] = array([x.timetuple()[:6] for x in data[tr][cat]['time_interval']['time_end']])
                gr.createVariable('lats', 'f', ('nlat',))
                gr['lats'][:] = data[tr][cat]['lats']
                gr.createVariable('lons', 'f', ('nlon',))
                gr['lons'][:] = data[tr][cat]['lons']
    logger.debug(f"Model parameters written to {filename}")
    return filename


def ReadStruct(path, prefix=None, structClass=Struct):
    if prefix is None :
        filename = path
    else :
        filename = os.path.join(path, '%s.nc' % prefix)
    with Dataset(filename) as ds:
        tracers = list(ds.groups.keys())
        data = structClass()
        for tr in tracers:
            categories = list(ds[tr].groups.keys())
            data[tr] = {}
            for cat in categories:
                data[tr][cat] = {
                    'emis': ds[tr][cat]['emis'][:],
                    'time_interval': {
                        'time_start': array([datetime(*x) for x in ds[tr][cat]['times_start'][:]]),
                        'time_end': array([datetime(*x) for x in ds[tr][cat]['times_end'][:]]),
                    },
                    'lats': ds[tr][cat]['lats'][:],
                    'lons': ds[tr][cat]['lons'][:]
                }
    logger.debug(f"Model parameters read from {filename}")
    return data


def CreateStruct(tracers, region, start, end, dt):
    times = arange(start, end, dt, dtype=datetime)
    data = Struct()
    for tr in tracers.keys():
        data[tr] = {}
        for cat in tracers[tr]['categories']:
            data[tr][cat] = {
                'emis':zeros((len(times), region.nlat, region.nlon)),
                'time_interval': {
                    'time_start': times,
                    'time_end':array(times)+dt
                },
                'lats':region.lats,
                'lons':region.lons,
                'region':region.name
            }
    
    return data


def ReadArchive(prefix, start, end, **kwargs):
    """
    Create an internal model data structure (i.e. Struct() instance) from a set of netCDF files.
    The files are loaded using xarray, the file name follows the format {prefix}{field}.{year}.nc, with prefix provided
    as argument, and field provided within the mandatory **kw arguments (see below)
    :param prefix: prefix used to construct the file name. Typically absolute or relative path + beginning of the file
    :param start: beginning of the first flux interval
    :param end: end of the last flux interval
    :param **: either a "category" keyword mapping to a dictionary containing pairs of {category_name : field_name}
    values, or a list of extra category_name = field_name arguments. This allows mapping data from a specific dataset
    (identified by field_name) to a user-specified flux category.
    :return:
    """

    # TODO: remove the dependency to xarray
    data = Struct()
    if kwargs.get('categories',False):
        categories = kwargs.get('categories')
    else :
        categories = kwargs

    if kwargs.get('archive', False):
        archive = Archive(kwargs['archive'])
    else :
        archive = None
    localArchive = Archive(os.path.dirname(f'local:{prefix}'), parent=archive, mkdir=True)

    dirname, prefix = os.path.split(prefix)

    for cat in tqdm(categories, leave=False) :
        field = categories[cat]
        ds = []

        # Import a file for every year at least partially covered (avoid trying to load a file if the end of the simulation is a 1st january).
        end_year = end.year
        if datetime(end_year, 1, 1) < end :
            end_year += 1

        for year in tqdm(range(start.year, end_year), desc=f"Importing data for category {cat}"):
            fname = f"{prefix}{field}.{year}.nc"
            tqdm.write(f"Emissions from category {cat} will be read from file {fname}")
            # Make sure that the file is here:
            localArchive.get(fname, dirname)
            ds.append(xr.load_dataset(os.path.join(dirname, fname)))
        ds = xr.concat(ds, dim='time').sel(time=slice(start, end))
        times = array([Timestamp(x).to_pydatetime() for x in ds.time.values])

        # The DataArray.sel command includes the time step starting at "end", so we normally would need to trim the last time step. But if it is a 1st january at 00:00, then the corresponding file
        # hasn't been loaded, so there is nothing to trim
        if times[-1] == end :
            times = times[:-1]
            emis = ds.co2flux.values[:-1,:,:]
        else :
            emis = ds.co2flux.values
            
        data[cat] = {
            'emis':emis,
            'time_interval':{
                'time_start':times,
                'time_end':times+(times[1]-times[0])
            },
            'lats':ds.lat[:],
            'lons':ds.lon[:]
        }

    if kwargs.get('extensive_units', False) : 
        data.to_extensive()
    return data