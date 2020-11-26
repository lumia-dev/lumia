#!/usr/bin/env python

import os
import logging
import subprocess
from multiprocessing import Pool
import h5py
from xarray import DataArray, open_dataarray
from numpy import unique, array, size, zeros
from lumia.obsdb import obsdb as obsdb_base
from lumia import tqdm
from lumia.Tools import system_tools

logger = logging.getLogger(__name__)


def concat_footprints(file):
    with h5py.File(file, 'r') as ds :
        observations = [k for k in ds.keys() if k not in ['latitudes', 'longitudes']]
        lats = ds['latitudes'][:]
        lons = ds['longitudes'][:]
        field = zeros((len(lats), len(lons)))
        for obs in observations:
            for fp in ds[obs].keys():
                try :
                    ilats = ds[obs][fp]['ilats'][:]
                    ilons = ds[obs][fp]['ilons'][:]
                except KeyError :
                    ilats = ds[obs][fp]['ilat'][:]
                    ilons = ds[obs][fp]['ilon'][:]
                field[ilats, ilons] += ds[obs][fp]['resp'][:]
    data = DataArray(field, coords=[lats, lons], dims=['lats', 'lons'])
    return data


class obsdb(obsdb_base):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.footprints_path = kwargs.get('footprints_path', None)

    def setupFootprints(self, path=None, names=None, cache=None):
        self.footprints_path = path if path is not None else self.footprints_path
        if self.footprints_path is None :
            logger.error("Unspecified footprints path")

        self.observations.loc[:, 'footprint'] = self._genFootprintNames(names)
        self._checkFootprints(cache=cache)
        self.setup = True

    def setupUncertainties(self, errvec):
        self.observations.loc[:, 'err'] = errvec

    def calcSensitivityMap(self, recompute=False):
        if not hasattr(self, 'sensi_map') or recompute :
            footprint_files = unique(self.observations.footprint)
            with Pool() as p :
                fields = list(tqdm(p.imap(concat_footprints, footprint_files), total=len(footprint_files), desc="Computing network sensitivity map"))
            #import pdb; pdb.set_trace()
            field = DataArray(array([f.data for f in fields]).sum(0), coords=[fields[0].lats, fields[0].lons], dims=['lats', 'lons'])
            self.sensi_map = field
            self.add_sensiMapIO()
#        if save is not None :
#            field.to_netcdf(save)
        return self.sensi_map

    def add_sensiMapIO(self):
        self.io['sensi_map'] = {
            'write':[self.sensi_map.to_netcdf.__func__, {'path':'sensi_map.nc'}],
            'read':[open_dataarray, {}],
            'filename':'sensi_map.nc'
        }

    def _genFootprintNames(self, fnames=None, leave_pbar=False):
        """
        Deduct the names of the footprint files based on their sitename, sampling height and observation time
        Optionally, a user-specified list (for example following a different pattern) can be speficied here.
        :param fnames: A list of footprint file names.
        :return: A list of footprint file names, or the optional "fnames" argument (if it isn't set to None)
        """
        if fnames is None :
            # Create the footprint theoretical filenames :
            for isite, site in tqdm(self.sites.iterrows(), leave=leave_pbar, desc='Generate footprint file names (step 1/3)', total=self.sites.shape[0]):
                self.observations.loc[self.observations.site == isite, 'code'] = site.code
#            codes = [self.sites.loc[s].code for s in tqdm(self.observations.site, leave=False, desc='Generate footprint file names (step 1/3)')]
            fnames = array(
                ['%s.%im.%s.h5'%(c.lower(), z, t.strftime('%Y-%m')) for (c, z, t) in tqdm(zip(
                    self.observations.code, self.observations.height, self.observations.time
                ), leave=leave_pbar, desc='Generate footprint file names (step 2/3)', total=self.observations.shape[0])]
            )
        fnames = [os.path.join(self.footprints_path, f) for f in tqdm(fnames, leave=leave_pbar, desc='Generate footprint file names (step 2/3)')]
        return fnames

    def _checkCacheFile(self, filename, cache):
        if cache in [None, False] :
            if os.path.exists(filename) :
                return filename
            else :
                return None
        file_in_cache = filename.replace(self.footprints_path, cache)
        system_tools.checkDir(cache)
        if not os.path.exists(file_in_cache):
            if not os.path.exists(filename):
                logger.warning('File %s not found! no footprints will be read from it',filename)
                file_in_cache = None
            elif cache != self.footprints_path:
                logger.debug(f'File <s>{os.path.basename(filename)}</s> found in <s>{self.footprints_path}</s>')
                subprocess.check_call(['rsync', '--copy-links', filename, file_in_cache])
        else :
            logger.debug(f'File <s>{os.path.basename(filename)}</s> already in <s>{cache}</s>')
        self.observations.loc[self.observations.footprint == filename, 'footprint'] = file_in_cache
        return file_in_cache

    def _checkFootprints(self, cache=None):
        footprint_files = unique(self.observations.footprint)

        # Loop over the footprint files (not on the obs, for efficiency)
        for fpf in tqdm(footprint_files, desc='Checking footprints'):

            # 1st, check if the footprint exists, and migrate it to cache if needed:
            fpf = self._checkCacheFile(fpf, cache)

            # Then, look if the file has all the individual obs footprints it's supposed to have
            if fpf is not None :
                fp = h5py.File(fpf, mode='r')

                # Times of the obs that are supposed to be in this file
                times = [x.to_pydatetime() for x in self.observations.loc[self.observations.footprint == fpf, 'time']]

                # Check if a footprint exists, for each time
                fp_exists = array([x.strftime('%Y%m%d%H%M%S') in fp for x in times])

                # Some footprints may exist but be empty, get rid of them
                fp_exists[fp_exists] = [size(fp[x.strftime('%Y%m%d%H%M%S')].keys()) > 0 for x in array(times)[fp_exists]]

                # Store that ...
                self.observations.loc[self.observations.footprint == fpf, 'footprint_exists'] = fp_exists.astype(bool)
