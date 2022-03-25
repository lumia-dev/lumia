from email.policy import default
import os
from netCDF4 import Dataset
from numpy import array, zeros, arange, array_equal
from datetime import datetime, timedelta
from lumia.Tools.system_tools import checkDir
import logging
from tqdm import tqdm
from numpy import unique, append, ones
import xarray as xr
from pandas import Timestamp, date_range
from lumia.Tools.regions import region
from archive import Archive
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)


class Emissions:
    def __init__(self, rcf, start, end):
        self.start = start
        self.end = end
        self.rcf = rcf

        self.tracers = {}
        self.data = {}

        try:
            self.atmos_del = self.rcf.get('atmospheric.D14C.prefix')
        except:
            self.atmos_del = None

        for tr in list(rcf.get('obs.tracers')):
            self.tracers[tr] = {}
            self.tracers[tr] = dict.fromkeys(rcf.get(f'emissions.{tr}.categories'))
            for cat in self.tracers[tr].keys():
                self.tracers[tr][cat] = rcf.get(f'emissions.{tr}.{cat}.origin')
            self.data[tr] = ReadArchive(self.rcf.get(f'emissions.{tr}.prefix'), self.start, self.end, tracer=tr, categories=self.tracers[tr], archive=self.rcf.get('emissions.archive'), freq=self.rcf.get('emissions.interval'))
            if self.rcf.get('optim.unit.convert', default=False):
                self.data[tr].to_extensive()   # Convert to umol
                self.print_summary()

        if self.atmos_del is not None:
            self.Del_14C = ReadArchive(self.rcf.get('atmospheric.D14C.prefix'), self.start, self.end, freq=self.rcf.get('emissions.interval'), atmos_del=True)
    
    def print_summary(self, unit='PgC'):
        scaling_factor = {
            'PgC':12 * 1.e-21,
            'PgCO2': 44 * 1.e-21,
        }[unit]
        for tr in self.data.keys():
            for cat in self.data[tr].keys():
                tstart = self.data[tr][cat]['time_interval']['time_start']
                years = unique([t.year for t in tstart])
                logger.info("===============================")
                logger.info(f"{cat}:")
                logger.info('')
                for year in years :
                    logger.info(f'{year}:')
                    for month in unique([t.month for t in tstart if t.year == year]):
                        tot = self.data[tr][cat]['emis'][[t.year == year and t.month == month for t in tstart]].sum()*scaling_factor
                        logger.info(f"    {datetime(2000, month, 1).strftime('%B'):10s}: {tot:7.2f} {unit}")
                    tot = self.data[tr][cat]['emis'][[t.year == year for t in tstart]].sum()*scaling_factor
                    logger.info("    --------------------------")
                    logger.info(f"   Total : {tot:7.2f} {unit}")
                    logger.info('')
            

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
        for cat in self.keys():
            dt = self[cat]['time_interval']['time_end']-self[cat]['time_interval']['time_start']
            dt=array([t.total_seconds() for t in dt])
            area = region(longitudes=self[cat]['lons'], latitudes=self[cat]['lats']).area
            self[cat]['emis'] *= area[None, :, :]
            self[cat]['emis'] *= dt[:, None, None]
        self.unit_type = 'extensive'

    def to_intensive(self):
        #assert self.unit_type == 'extensive'
        for cat in self.keys():
            dt = self[cat]['time_interval']['time_end']-self[cat]['time_interval']['time_start']
            dt=array([t.total_seconds() for t in dt])
            area = region(longitudes=self[cat]['lons'], latitudes=self[cat]['lats']).area
            self[cat]['emis'] /= area[None, :, :]
            self[cat]['emis'] /= dt[:, None, None]
        self.unit_type = 'intensive'


def WriteStruct(data, path, prefix=None, atmos_del=False):
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

    if atmos_del:
        with Dataset(filename, 'w') as ds:
            ds.createDimension('time_components', 6)
            ds.createGroup('Del_14C')
            tr = 'Del_14C'
            ds['Del_14C'].createDimension('nt', data[tr]['obs'].shape[0])
            ds['Del_14C'].createVariable('obs', 'd', 'nt')
            ds['Del_14C']['obs'][:] = data[tr]['obs']
            ds['Del_14C'].createVariable('times_start', 'i', ('nt', 'time_components'))
            ds['Del_14C']['times_start'][:,:] = array([x.timetuple()[:6] for x in data[tr]['time_interval']['time_start']])
            ds['Del_14C'].createVariable('times_end', 'i', ('nt', 'time_components'))
            ds['Del_14C']['times_end'][:,:] = array([x.timetuple()[:6] for x in data[tr]['time_interval']['time_end']])
    else:
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
                    gr['emis'][:,:,:] = data[tr][cat]['emis']
                    gr.createVariable('times_start', 'i', ('nt', 'time_components'))
                    gr['times_start'][:,:] = array([x.timetuple()[:6] for x in data[tr][cat]['time_interval']['time_start']])
                    gr.createVariable('times_end', 'i', ('nt', 'time_components'))
                    gr['times_end'][:,:] = array([x.timetuple()[:6] for x in data[tr][cat]['time_interval']['time_end']])
                    gr.createVariable('lats', 'f', ('nlat',))
                    gr['lats'][:] = data[tr][cat]['lats']
                    gr.createVariable('lons', 'f', ('nlon',))
                    gr['lons'][:] = data[tr][cat]['lons']
    logger.debug(f"Model parameters written to {filename}")
    return filename


def ReadStruct(path, atmos_del=False, prefix=None, structClass=Struct, tracers=None):
    if prefix is None :
        filename = path
    else :
        filename = os.path.join(path, '%s.nc' % prefix)

    if atmos_del:
        with Dataset(filename) as ds:
            data = {}
            data['Del_14C'] = {
                        'obs': ds['Del_14C']['obs'][:],
                        'time_interval': {
                            'time_start': array([datetime(*x) for x in ds['Del_14C']['times_start'][:]]),
                            'time_end': array([datetime(*x) for x in ds['Del_14C']['times_end'][:]]),
                        }
                    }
    else:
        with Dataset(filename) as ds:
            if tracers is None:
                tracers = {}
                for tr in list(ds.groups.keys()):
                    tracers[tr] = list(ds[tr].groups.keys())
            data = structClass()
            for tr in tracers.keys():
                data[tr] = {}
                for cat in tracers[tr]:
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
        for cat in tracers[tr]:
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

    if kwargs.get('tracer',False):
        tracer = kwargs.get('tracer')
    else :
        tracer = kwargs

    if kwargs.get('archive', False):
        archive = Archive(kwargs['archive'])
    else :
        archive = None

    if kwargs.get('freq', False):
            freq = kwargs.get('freq')
    else :
        freq = kwargs

    if kwargs.get('atmos_del', False):
        atmos_del = kwargs.get('atmos_del')
    else :
        atmos_del = None

    if atmos_del is None:

        localArchive = Archive(os.path.dirname(f'local:{prefix}'), parent=archive, mkdir=True)

        dirname, prefix = os.path.split(prefix)

        for cat in tqdm(categories, leave=False) :
            field = categories[cat]

            if not field:
                pass
            else:
                # Import a file for every year at least partially covered (avoid trying to load a file if the end of the simulation is a 1st january).
                end_year = end.year
                if datetime(end_year, 1, 1) < end :
                    end_year += 1

                emis = []
                times = []

                for year in tqdm(range(start.year, end_year), desc=f"Importing data for category {cat}"):
                    fname = f"{prefix}{field}.{year}.nc"
                    tqdm.write(f"Emissions from tracer {tracer}, category {cat}, year {year}, will be read from file {fname}")
                    # Make sure that the file is here:
                    localArchive.get(fname, dirname)
                    with Dataset(os.path.join(dirname, fname), 'r') as ds:
                        emis.extend(ds[f'{tracer}_flux'][:])
                        units = ds['time'].units.split()
                        start_file = datetime.strptime(units[2]+' '+units[3], '%Y-%m-%d %H:%M:%S')
                        times.extend(date_range(start=start_file, periods=len(ds['time'][:]), freq=freq).to_pydatetime().tolist())
                        lat = ds['lat'][:]
                        lon = ds['lon'][:]

                emis = array(emis)
                times = array(times)
                emis = emis[(times >= start) & (times < end), :, :]
                times = times[(times >= start) & (times < end)]
                    
                data[cat] = {
                    'emis': emis,
                    'time_interval': {
                        'time_start': times,
                        'time_end': times+(times[1]-times[0])
                    },
                    'lats': lat,
                    'lons': lon
                }
        if kwargs.get('extensive_units', False) : 
            data.to_extensive()
            
        return data

    if atmos_del:
        end_year = end.year
        if datetime(end_year, 1, 1) < end :
            end_year += 1
        
        obs = []
        times = []

        for year in tqdm(range(start.year, end_year), desc=f"Importing data for atmospheric delta"):
            fname = f"{prefix}{year}.nc"
            tqdm.write(f"Atmospheric delta for year {year}, will be read from file {fname}")
            with Dataset(fname, 'r') as ds:
                obs.extend(ds['Del_14C']['obs'][:])
                # units = ds['time'].units.split()
                start_file = datetime(*ds['Del_14C']['times_start'][:][0])
                times.extend(date_range(start=start_file, periods=len(ds['Del_14C']['times_start'][:]), freq=freq).to_pydatetime().tolist())

                # start_tr = datetime(start.year, start.month, 1)
                # end_tr = datetime(end.year, end.month, 1)

                # dates = []
                # for i in ds['dates']:
                #     dt = []
                #     for j in i:
                #         dt.append(j)
                #     dt.append(1)
                #     dates.append(datetime(*dt))
        
        obs = array(obs)
        times = array(times)
        obs = obs[(times >= start) & (times < end)]
        times = times[(times >= start) & (times < end)]
        
        # obs = ones(len(times))
        # for i in range(len(dates)):
        #     obs[(times >= dates[i]) & (times < dates[i] + relativedelta(months=1))] = obs[(times >= dates[i]) & (times < dates[i] + relativedelta(months=1))] * Del_14C[i]

        data = {'Del_14C': {
            'obs': obs,
            'time_interval': {
                'time_start': times,
                'time_end': times+(times[1]-times[0])
                }
            }
        }
        return data
