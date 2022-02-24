import os
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


class Emissions:
    def __init__(self, rcf, start, end):
        self.start = start
        self.end = end
        self.rcf = rcf
        self.categories = dict.fromkeys(rcf.get('emissions.categories'))
        for cat in self.categories :
            self.categories[cat] = rcf.get(f'emissions.{cat}.origin')
        self.data = ReadArchive(rcf.get('emissions.prefix'), self.start, self.end, categories=self.categories, archive=rcf.get('emissions.archive'))

        if rcf.get('optim.unit.convert', default=False):
            logger.info("Trying to convert fluxes to umol (from umol/m2/s")
            self.data.to_extensive()   # Convert to umol
            self.print_summary()

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

    def print_summary(self, unit='PgC'):
        self.data.print_summary(unit)


class Struct(dict):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.unit_type = 'intensive'

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

    def between_times(self, tbeg, tend):
        """
        Return a structure with only the data that fall within the specified time interval (must be within
        the time interval of the initial Struct).
        This is mainly for postprocessing purposes.
        """
        outstruct = Struct()
        for cat in self.keys():
            beg = self[cat]['time_interval']['time_start']
            end = self[cat]['time_interval']['time_end']
            assert tbeg in beg and tend in end, f"The requested time boundaries ({tbeg} to {tend}) are not available for category {cat}."
            select = (beg >= tbeg) & (end <= tend)
            outstruct[cat] = {
                'emis': self[cat]['emis'][select, :, :],
                'time_interval':{
                    'time_start':self[cat]['time_interval']['time_start'][select],
                    'time_end':self[cat]['time_interval']['time_end'][select]
                },
                'lats':self[cat]['lats'],
                'lons':self[cat]['lons']
            }
        return outstruct

    def coarsen(self, destreg):
        """
        Coarsen the data to a lower resolution
        """
        for cat in self.keys():
            sourcereg = region(latitudes=self[cat]['lats'], longitudes=self[cat]['lons'])
            if sourcereg != destreg :
                data = GriddedData(self[cat]['emis'], sourcereg)
                self[cat]['emis'] = data.regrid(destreg, weigh_by_area=self.unit_type == 'intensive')
                self[cat]['lats'] = destreg.lats
                self[cat]['lons'] = destreg.lons

    def to_extensive(self):
        assert self.unit_type == 'intensive'
        for cat in self.keys():
            dt = self[cat]['time_interval']['time_end']-self[cat]['time_interval']['time_start']
            dt=array([t.total_seconds() for t in dt])
            area = region(longitudes=self[cat]['lons'], latitudes=self[cat]['lats']).area
            self[cat]['emis'] *= area[None, :, :]
            self[cat]['emis'] *= dt[:, None, None]
        self.unit_type = 'extensive'
        logger.info("Converted fluxes to extensive units (i.e. umol)")

    def to_intensive(self):
        #assert self.unit_type == 'extensive'
        for cat in self.keys():
            dt = self[cat]['time_interval']['time_end']-self[cat]['time_interval']['time_start']
            dt=array([t.total_seconds() for t in dt])
            area = region(longitudes=self[cat]['lons'], latitudes=self[cat]['lats']).area
            self[cat]['emis'] /= area[None, :, :]
            self[cat]['emis'] /= dt[:, None, None]
        self.unit_type = 'intensive'
        logger.info("Converted fluxes to intensive units (i.e. umol/m2/s)")

    def append(self, other, overwrite=True):
        """
        Extend the data with the data from another structure.
        The other structure must have the same spatial boundaries. If the temporal boundaries of the two structures overlap, the data from "other"
        overwrite the original data, unless the "overwrite" optional attribute is set to False.
        """
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
        }[unit]
        for cat in self.keys() :
            tstart = self[cat]['time_interval']['time_start']
            years = unique([t.year for t in tstart])
            logger.info("===============================")
            logger.info(f"{cat}:")
            logger.info('')
            for year in years :
                logger.info(f'{year}:')
                for month in unique([t.month for t in tstart if t.year == year]):
                    tot = self[cat]['emis'][[t.year == year and t.month == month for t in tstart]].sum()*scaling_factor
                    logger.info(f"    {datetime(2000, month, 1).strftime('%B'):10s}: {tot:7.2f} {unit}")
                tot = self[cat]['emis'][[t.year == year for t in tstart]].sum()*scaling_factor
                logger.info("    --------------------------")
                logger.info(f"   Total : {tot:7.2f} {unit}")
                logger.info('')
        if unit_type != self.unit_type :
            self.to_intensive()



def WriteStruct(data, path, prefix=None, zlib=False, complevel=1):
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
        for cat in [c for c in data.keys() if 'cat_list' not in c]:
            gr = ds.createGroup(cat)
            gr.createDimension('nt', data[cat]['emis'].shape[0])
            gr.createDimension('nlat', data[cat]['emis'].shape[1])
            gr.createDimension('nlon', data[cat]['emis'].shape[2])
            gr.createVariable('emis', 'd', ('nt', 'nlat', 'nlon'), zlib=zlib, complevel=complevel)
            gr['emis'][:] = data[cat]['emis']
            gr.createVariable('times_start', 'i', ('nt', 'time_components'))
            gr['times_start'][:] = array([x.timetuple()[:6] for x in data[cat]['time_interval']['time_start']])
            gr.createVariable('times_end', 'i', ('nt', 'time_components'))
            gr['times_end'][:] = array([x.timetuple()[:6] for x in data[cat]['time_interval']['time_end']])
            gr.createVariable('lats', 'f', ('nlat',))
            gr['lats'][:] = data[cat]['lats']
            gr.createVariable('lons', 'f', ('nlon',))
            gr['lons'][:] = data[cat]['lons']
    logger.debug(f"Model parameters written to {filename}")
    return filename


def ReadStruct(path, prefix=None, structClass=Struct):
    if prefix is None :
        filename = path
    else :
        filename = os.path.join(path, '%s.nc' % prefix)
    with Dataset(filename) as ds:
        categories = ds.groups.keys()
        data = structClass()
        for cat in categories:
            data[cat] = {
                'emis': ds[cat]['emis'][:],
                'time_interval': {
                    'time_start': array([datetime(*x) for x in ds[cat]['times_start'][:]]),
                    'time_end': array([datetime(*x) for x in ds[cat]['times_end'][:]]),
                },
                'lats': ds[cat]['lats'][:],
                'lons': ds[cat]['lons'][:]
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