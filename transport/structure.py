import os
import pdb

from netCDF4 import Dataset
from numpy import array, zeros, arange, array_equal
from datetime import datetime
from lumia.Tools.system_tools import checkDir
from tqdm import tqdm
from numpy import unique, append
import xarray as xr
from pandas import Timestamp
from lumia.Tools.regions import region
from archive import Archive
from lumia.Tools.geographical_tools import GriddedData
from loguru import logger
from pandas import date_range


class Emissions:
    def __init__(self, rcf, start, end):
        self.start = start
        self.end = end
        self.rcf = rcf

        self.tracers = {}

        try:
            self.atmos_del = self.rcf.get('atmospheric.D14C.prefix')
        except:
            self.atmos_del = None

        # trlist = rcf.get('obs.tracers') if isinstance(rcf.get('obs.tracers'), list) else [rcf.get('obs.tracers')]
        for tr in list(rcf.get('obs.tracers')):
            self.tracers[tr] = {}
            self.tracers[tr] = dict.fromkeys(rcf.get(f'emissions.{tr}.categories'))
            self.tracers[tr]['prefix'] = self.rcf.get(f'emissions.{tr}.prefix')
            for cat in list(self.tracers[tr].keys()):
                if cat == 'prefix':
                    pass
                else:
                    self.tracers[tr][cat] = rcf.get(f'emissions.{tr}.{cat}.origin')

        resample = None
        if rcf.get('emissions.resample', default=False):
            resample = rcf.get('emissions.interval')

        self.data = ReadArchive(self.start, self.end, tracers=self.tracers, archive=self.rcf.get('emissions.archive'), freq=self.rcf.get('emissions.interval'))

        if self.rcf.get('optim.unit.convert', default=False):
            logger.info("Trying to convert fluxes to umol (from umol/m2/s")
            self.data.to_extensive()   # Convert to umol
            self.print_summary(unit=self.rcf.get(f'emissions.{tr}.{cat}.unit'))
            # self.data.to_intensive()

        if self.atmos_del is not None:
            self.Del_14C = ReadArchive(self.start, self.end, prefix=self.rcf.get('atmospheric.D14C.prefix'), freq=self.rcf.get('emissions.interval'), atmos_del=True)

        # Coarsen the data if needed:
        reg = region(
            lon0=self.rcf.get('region.lon0'),
            lon1=self.rcf.get('region.lon1'),
            lat0=self.rcf.get('region.lat0'),
            lat1=self.rcf.get('region.lat1'),
            dlat=self.rcf.get('region.dlat'),
            dlon=self.rcf.get('region.dlon')
        )
        self.data.coarsen(reg)
        self.data.print_summary()
    
    def print_summary(self, unit='PgC'):
        self.data.print_summary(unit)
            

class Struct(dict):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.unit_type = 'intensive'

    def __add__(self, other):
        #TODO: needs development for multitracer
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

    def between_times(self, tbeg, tend):
        """
        Return a structure with only the data that fall within the specified time interval (must be within
        the time interval of the initial Struct).
        This is mainly for postprocessing purposes.
        """
        outstruct = Struct()
        for tr in self.keys():
            outstruct[tr] = {}
            for cat in self[tr].keys():
                beg = self[tr][cat]['time_interval']['time_start']
                end = self[tr][cat]['time_interval']['time_end']
                assert tbeg in beg and tend in end, f"The requested time boundaries ({tbeg} to {tend}) are not available for category {cat}."
                select = (beg >= tbeg) & (end <= tend)
                outstruct[tr][cat] = {
                    'emis': self[tr][cat]['emis'][select, :, :],
                    'time_interval':{
                        'time_start':self[tr][cat]['time_interval']['time_start'][select],
                        'time_end':self[tr][cat]['time_interval']['time_end'][select]
                    },
                    'lats':self[tr][cat]['lats'],
                    'lons':self[tr][cat]['lons']
                }
        return outstruct

    def coarsen(self, destreg):
        """
        Coarsen the data to a lower resolution
        """
        for tr in self.keys():
            for cat in self[tr].keys():
                sourcereg = region(latitudes=self[tr][cat]['lats'], longitudes=self[tr][cat]['lons'])
                if sourcereg != destreg :
                    data = GriddedData(self[tr][cat]['emis'], sourcereg)
                    self[tr][cat]['emis'] = data.regrid(destreg, weigh_by_area=self[tr].unit_type == 'intensive')
                    self[tr][cat]['lats'] = destreg.lats
                    self[tr][cat]['lons'] = destreg.lons

    def to_extensive(self):
        assert self.unit_type == 'intensive'
        for tr in self.keys():
            for cat in self[tr].keys():
                dt = self[tr][cat]['time_interval']['time_end']-self[tr][cat]['time_interval']['time_start']
                dt=array([t.total_seconds() for t in dt])
                area = region(longitudes=self[tr][cat]['lons'], latitudes=self[tr][cat]['lats']).area
                self[tr][cat]['emis'] *= area[None, :, :]
                self[tr][cat]['emis'] *= dt[:, None, None]
        self.unit_type = 'extensive'
        logger.info("Converted fluxes to extensive units (i.e. umol)")

    def to_intensive(self):
        # assert self.unit_type == 'extensive', pdb.set_trace()
        for tr in self.keys():
            for cat in self[tr].keys():
                dt = self[tr][cat]['time_interval']['time_end']-self[tr][cat]['time_interval']['time_start']
                dt=array([t.total_seconds() for t in dt])
                area = region(longitudes=self[tr][cat]['lons'], latitudes=self[tr][cat]['lats']).area
                self[tr][cat]['emis'] /= area[None, :, :]
                self[tr][cat]['emis'] /= dt[:, None, None]
        self.unit_type = 'intensive'
        logger.info("Converted fluxes to intensive units (i.e. umol/m2/s)")
        

    def append(self, other, overwrite=True):
        """
        Extend the data with the data from another structure.
        The other structure must have the same spatial boundaries. If the temporal boundaries of the two structures overlap, the data from "other"
        overwrite the original data, unless the "overwrite" optional attribute is set to False.
        """
        #TODO: needs development for multitracer
        new = Struct()
        for cat in self.keys():

            # Construct the new time coordinates
            beg_self = self[cat]['time_interval']['time_start']
            end_self = self[cat]['time_interval']['time_end']
            beg_other = other[cat]['time_interval']['time_start']
            end_other = other[cat]['time_interval']['time_end']
            beg_final = unique(append(beg_self, beg_other))
            end_final = unique(append(end_self, end_other))
            diff_t = end_final-beg_final

            # In theory there's nothing wrong with having irregular spacing, but this is not implemented and is probably an error (and it makes
            # what comes after more difficult to code). So I disable it for now.
            assert all([dt == diff_t[0] for dt in diff_t]), f"append results in irregular time intervals for category {cat}. Edit the source code if this is intentional"

            # Construct the new data
            nt = len(beg_final)
            nlat = len(self[cat]['lats'])
            nlon = len(self[cat]['lons'])
            data = zeros((nt, nlat, nlon))
            if overwrite :
                data[[t in beg_self for t in beg_final], :, :] = self[cat]['emis']
                data[[t in beg_other for t in beg_final], :, :] = other[cat]['emis']
            else :
                data[[t in beg_other for t in beg_final], :, :] = other[cat]['emis']
                data[[t in beg_self for t in beg_final], :, :] = self[cat]['emis']

            # Store
            new[cat] = {
                'emis': data,
                'time_interval' : {
                    'time_start':beg_final,
                    'time_end':end_final
                },
                'lats':self[cat]['lats'],
                'lons':self[cat]['lons']
            }
        return new

    def _get_boundaries_t(self):
        #TODO: needs development for multitracer
        beg = [self[cat]['time_interval']['time_start'].min() for cat in self.keys()]
        end = [self[cat]['time_interval']['time_end'].max() for cat in self.keys()]
        assert all([b == beg[0] for b in beg] + [e == end[0] for e in end]), f"All categories do not share the same time boundaries, cannot append {beg}, {end}"
        return beg[0], end[0]

    def print_summary(self, unit='PgC'):
        unit_type = self.unit_type + ''
        if self.unit_type == 'intensive':
            self.to_extensive()
        scaling_factor = {
            'PgC':12 * 1.e-21,
            'PgCO2': 44 * 1.e-21,
            'TgCH4': 16.0425 * 1.e-18
        }[unit]
        for tr in self.keys() :
            for cat in self[tr].keys() :
                tstart = self[tr][cat]['time_interval']['time_start']
                unit_source = self[tr][cat].get('unit', 'umol/m2/s')
                scf = scaling_factor * {
                    'umol/m2/s':1.,
                    'nmol/m2/s':1.e-3
                }[unit_source]
                years = unique([t.year for t in tstart])
                logger.info("===============================")
                logger.info(f"{cat}:")
                logger.info('')
                for year in years :
                    logger.info(f'{year}:')
                    for month in unique([t.month for t in tstart if t.year == year]):
                        tot = self[tr][cat]['emis'][[t.year == year and t.month == month for t in tstart]].sum()*scf
                        logger.info(f"    {datetime(2000, month, 1).strftime('%B'):10s}: {tot:7.2f} {unit}")
                    tot = self[tr][cat]['emis'][[t.year == year for t in tstart]].sum()*scf
                    logger.info("    --------------------------")
                    logger.info(f"   Total : {tot:7.2f} {unit}")
                    logger.info('')
        if unit_type != self.unit_type :
            self.to_intensive()

def WriteStruct(data, path, prefix=None, atmos_del=False, zlib=False, complevel=1):
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
        with Dataset(filename, 'r') as ds:
            data = {}
            data['Del_14C'] = {
                        'obs': ds['Del_14C']['obs'][:],
                        'time_interval': {
                            'time_start': array([datetime(*x) for x in ds['Del_14C']['times_start'][:]]),
                            'time_end': array([datetime(*x) for x in ds['Del_14C']['times_end'][:]]),
                        }
                    }
    else:
        with Dataset(filename, 'r') as ds:
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


def ReadArchive(start, end, **kwargs):
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

    if kwargs.get('tracers',False):
        tracers = kwargs.get('tracers')
    else :
        tracers = kwargs

    if kwargs.get('archive', False):
        archive = Archive(kwargs['archive'])
    else :
        archive = None

    if kwargs.get('freq', False):
        freq = kwargs.get('freq')
    else :
        freq = kwargs

    if kwargs.get('prefix', False):
        prefix = kwargs.get('prefix')
    else :
        prefix = kwargs

    if kwargs.get('atmos_del', False):
        atmos_del = kwargs.get('atmos_del')
    else :
        atmos_del = None

    if atmos_del is None:

        for tr in tracers.keys():

            data[tr] = {}

            localArchive = Archive(os.path.dirname(f"local:{tracers[tr]['prefix']}"), parent=archive, mkdir=True)

            dirname, prefix = os.path.split(tracers[tr]['prefix'])

            for cat in tqdm(tracers[tr].keys(), leave=False) :
                if cat == 'prefix':
                    pass
                else:
                    field = tracers[tr][cat]

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
                            tqdm.write(f"Emissions from tracer {tr}, category {cat}, year {year}, will be read from file {fname}")
                            # Make sure that the file is here:
                            localArchive.get(fname, dirname)
                            with Dataset(os.path.join(dirname, fname), 'r') as ds:
                                emis.extend(ds[f'{tr}flux'][:])
                                units = ds['time'].units.split()
                                try:
                                    start_file = datetime.strptime(units[2]+' '+units[3], '%Y-%m-%d %H:%M:%S')
                                except:
                                    try:
                                        start_file = datetime.strptime(units[2]+' '+'00:00:00', '%Y-%m-%d %H:%M:%S')
                                    except:
                                        units = units[2].split('T')
                                        start_file = datetime.strptime(units[0]+' '+units[1], '%Y-%m-%d %H:%M:%S')
                                times.extend(date_range(start=start_file, periods=len(ds['time'][:]), freq=freq).to_pydatetime().tolist())
                                lat = ds['lat'][:]
                                lon = ds['lon'][:]

                        emis = array(emis)
                        times = array(times)
                        emis = emis[(times >= start) & (times < end), :, :]
                        times = times[(times >= start) & (times < end)]
                            
                        data[tr][cat] = {
                            'emis': emis,
                            'time_interval': {
                                'time_start': times,
                                'time_end': times+(times[1]-times[0])
                            },
                            'lats': lat,
                            'lons': lon
                        }

                        try :
                            data[tr][cat]['unit'] = ds[f'{tr}flux'].units
                        except AttributeError :
                            data[tr][cat]['unit'] = 'umol/m2/s'
                            logger.warning(f'"unit" attribute missing in files {prefix}{field}.{year}.nc. Assuming {data[tr][cat]["unit"]}')

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
                obs.extend(ds['obs'][:])
                # units = ds['time'].units.split()
                start_file = datetime(*ds['times_start'][:][0])
                times.extend(date_range(start=start_file, periods=len(ds['times_start'][:]), freq=freq).to_pydatetime().tolist())

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
