import os
from netCDF4 import Dataset
from numpy import array, zeros, arange, array_equal
from datetime import datetime
from lumia.Tools.system_tools import checkDir
import logging
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
    logging.debug(f"Model parameters written to {filename}")
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
    logging.debug(f"Model parameters read from {filename}")
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

