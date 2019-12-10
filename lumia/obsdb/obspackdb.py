#!/usr/bin/env python

from lumia import obsdb as obsdb_base
import logging
import glob
from netCDF4 import Dataset
from lumia import tqdm
from datetime import datetime
from pandas import DataFrame
import os
from numpy import *

logger = logging.getLogger(__name__)

class obsdb(obsdb_base):
    def importFromPath(self, path, pattern='data/nc/co2_*.nc', date_range=(datetime(1000,1,1),datetime(3000,1,1)), lat_range=[-inf,inf], lon_range=(-inf,inf), exclude_mobile=True):
        """
        Import all the observations from an obspack file (in netcdf format). 
        The space/time domain can be limited using the date_range, lat_range and lon_range arguments.
        by default the observations from mobile platform are skipped (this method works with them but is not adapted, as it would create one "site" entry for each observation.
        """

        files = sorted(glob.glob(os.path.join(path, pattern)))
        for file in tqdm(files, desc=f'Import obs files from {os.path.join(path, pattern)}'):

            # Import data
            with Dataset(file) as ds:
                if ds.dataset_parameter == 'co2' : scale = 1.e6
                if exclude_mobile :
                    platform = ds.dataset_platform
                    continue_import = platform in ['fixed']
                if continue_import :
                    if platform in ['fixed'] :
                        continue_import = lat_range[0] <= ds.site_latitude <= lat_range[1] and lon_range[0] <= ds.site_longitude <= lon_range[1]
                        selection_ll = None
                    else :
                        lons = ds['longitude'][:]
                        lats = ds['latitude'][:]
                        selection_ll = (lon_range[0] <= lons) * (lons <= lon_range[1])
                        selection_ll *= (lat_range[0] <= lats) * (lats  <= lat_range[1])
                        continue_import = any(selection)
                else :
                    logger.debug(f'File {os.path.basename(file)} skipped because of platform {platform}')
                if continue_import :
                    time = array([datetime(*x) for x in ds['time_components'][:]])
                    try :
                        selection = (date_range[0] <= time) * (time <= date_range[1])
                    except :
                        import pdb; pdb.set_trace()
                    continue_import = any(selection)
                else :
                    logger.debug(f'No data imported from file {os.path.basename(file)}, because of lat/lon range')


                if continue_import :
                    if selection_ll is not None : selection *= selection_ll
                    nobs = len(time[selection])
                    if 'value_unc' in ds.variables.keys():
                        err = ds.variables['value_unc'][selection]*scale
                    else :
                        err = zeros(sum(selection))
                    try :
                        observations = {
                            'time':time[selection],
                            'lat':ds['latitude'][selection],
                            'lon':ds['longitude'][selection],
                            'alt':ds['altitude'][selection],
                            'height':ds['altitude'][selection]-ds.site_elevation,
                            'obs':ds['value'][selection]*scale,
                            'err':err,
                            'file':array([file]*nobs),
                            'code':array([ds.site_code]*nobs),
                            'name':array([ds.site_name]*nobs)
                        }
                    except :
                        logger.error("Import failed for file %s"%file)
                        import pdb; pdb.set_trace()
                else :
                    logger.debug(f'No data imported from file {os.path.basename(file)}, because of time range')


            if continue_import :
                logger.info(f"{nobs} observations imported from {os.path.basename(file)}")

                # Fill in dataframes :
                observations = DataFrame.from_dict(observations)
                sites = observations.loc[:, ['lat', 'lon', 'alt', 'height', 'file', 'code', 'name']].drop_duplicates()

                for dummy, row in sites.iterrows():
                    # Check if a corresponding site already exists in the database :
                    if self.sites.loc[(self.sites.reindex(columns=sites.columns) == row).all(axis=1)].shape[0] == 0 :
                        self.sites = self.sites.append(row, ignore_index=True)

                    # Retrieve the index of the site (there should be only one!):
                    site = self.sites.loc[(self.sites.loc[:, sites.columns] == row).all(axis=1)]
                    assert site.shape[0] == 1, logger.error(f"Error importing data from {os.path.basename(file)}, {site.shape[0]} entries found for site, instead of exactly 1 expected")
                    isite = site.index[0]

                    # Create the rows to be added to the self.observations dataframe:
                    obs = observations.loc[
                            (observations.loc[:, sites.columns] == row).all(axis=1), 
                            ['time', 'lat', 'lon', 'alt', 'height', 'obs', 'err', 'code']
                    ]
                    obs.loc[:, 'site'] = isite

                    # Append to self.observations:
                    self.observations = self.observations.append(obs, sort=False)
