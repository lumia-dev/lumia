import os
from netCDF4 import Dataset
from numpy import array, zeros, arange, array_equal
from datetime import datetime
from lumia.Tools.system_tools import checkDir
import logging
from tqdm.autonotebook import tqdm
from numpy import *
import xarray as xr
from pandas import Timestamp
logger = logging.getLogger(__name__)

class Struct(dict):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __add__(self, other):
        allcats = set(list(self.keys())+list(other.keys()))
        for cat in allcats :
            if cat in self.keys():
                # Add the category if the time intervals are matching!
                ts = array_equal(self[cat]['time_interval']['time_start'], other[cat]['time_interval']['time_start'])
                te = array_equal(self[cat]['time_interval']['time_end'], other[cat]['time_interval']['time_end'])
                if ts and te :
                    self[cat]['emis'] += other[cat]['emis']
                else :
                    logger.error("Cannot add the two structures, time dimensions non conforming")
                    raise ValueError
            else :
                self[cat] = other[cat]
        return self

def WriteStruct(data, path, prefix=None):
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
        for cat in [c for c in data.keys() if not 'cat_list' in c]:
            gr = ds.createGroup(cat)
            try :
                gr.createDimension('nt', data[cat]['emis'].shape[0])
                gr.createDimension('nlat', data[cat]['emis'].shape[1])
                gr.createDimension('nlon', data[cat]['emis'].shape[2])
                gr.createVariable('emis', 'd', ('nt', 'nlat', 'nlon'))
                gr['emis'][:] = data[cat]['emis']
                gr.createVariable('times_start', 'i', ('nt', 'time_components'))
                gr['times_start'][:] = array([x.timetuple()[:6] for x in data[cat]['time_interval']['time_start']])
                gr.createVariable('times_end', 'i', ('nt', 'time_components'))
                gr['times_end'][:] = array([x.timetuple()[:6] for x in data[cat]['time_interval']['time_end']])
            except :
                import pdb; pdb.set_trace()
    logger.debug(f"Model parameters written to {filename}")
    return filename


def ReadStruct(path, prefix=None):
    if prefix is None :
        filename = path
    else :
        filename = os.path.join(path, '%s.nc' % prefix)
    with Dataset(filename) as ds:
        categories = ds.groups.keys()
        data = Struct()
        for cat in categories:
            data[cat] = {
                'emis': ds.groups[cat].variables['emis'][:],
                'time_interval': {
                    'time_start': array([datetime(*x) for x in ds.groups[cat].variables['times_start'][:]]),
                    'time_end': array([datetime(*x) for x in ds.groups[cat].variables['times_end'][:]]),
                }
            }
    logger.debug(f"Model parameters read from {filename}")
    return data


def CreateStruct(categories, region, start, end, dt):
    times = arange(start, end, dt, dtype=datetime)
    #data = {'cat_list': categories}
    data = Struct()
    for cat in categories :
        data[cat] = {
            'emis':zeros((len(times), region.nlat, region.nlon)),
            'time_interval': {
                'time_start': times,
                'time_end':array(times)+dt
            }
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
    data = Struct()
    if kwargs.get('categories',False):
        categories = kwargs.get('categories')
    else :
        categories = kwargs

    for cat in tqdm(categories, leave=False) :
        field = categories[cat]
        logger.debug(f"Emissions from category {cat} will be read using source {field}")
        ds = []
        for year in tqdm(range(start.year, end.year+1), desc=f"Importing data for category {cat}"):
            ds.append(xr.load_dataset(f'{prefix}.{field}.{year}.nc'))
        ds = xr.concat(ds, dim='time').sel(time=slice(start, end))
        times = array([Timestamp(x).to_pydatetime() for x in ds.time.values])
        data[cat] = {
            'emis':ds.co2flux[:-1,:,:],
            'time_interval':{
                'time_start':times[:-1],
                'time_end':times[1:]
            }
        }
    return data