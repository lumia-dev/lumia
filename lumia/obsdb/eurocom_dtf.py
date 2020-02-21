#!/usr/bin/env python

import os
import glob
import logging
from datetime import datetime
from numpy import inf, loadtxt, array, nan, unique, count_nonzero
from tqdm import tqdm
from pandas import DataFrame
from lumia.obsdb import obsdb as obsdb_base

logger = logging.getLogger(__name__)

class obsdb(obsdb_base):
    def importFromPath(self, path, pattern='*.co2',
                       date_range=(datetime(1000,1,1),datetime(3000,1,1)),
                       lat_range=(-inf,inf), lon_range=(-inf,inf),
                       exclude_mobile=True):
        files = sorted(glob.glob(os.path.join(path, pattern)))
        for file in tqdm(files, desc='Import obs files from {os.path.join(path, pattern)}'):
            obs = self.importASCII(file, date_range, lat_range, lon_range, exclude_mobile)
            if obs is not None :
                self.addObs(obs)
        
        self.fixDuplicatedSiteCodes()

    def fixDuplicatedSiteCodes(self):
        sitecodes = unique(self.sites.code)
        for code in sitecodes :
            if count_nonzero(self.sites.code.values == code) > 1 :
                heights = self.sites.loc [self.sites.code == code, 'height'].values
                oldcodes = self.sites.loc[self.sites.code == code, 'code'].values
                newcodes = [f'{s.upper():3s}{h:03.0f}' for (s, h) in zip(oldcodes, heights)]
                self.sites.loc[self.sites.code == code, 'code'] = newcodes

    def importASCII(self, filename, date_range=(datetime(1000,1,1), datetime(3000,1,1)),
                    lat_range=(-inf, inf), lon_range=(-inf, inf), exclude_mobile=True):

        header = self.read_header(filename)

        # Import the data (if needed):
        observations = None
        continue_import = False
        if lat_range[0] <= header['site_latitude'] <= lat_range[1] :
            if lon_range[0] <= header['site_longitude'] <= lon_range[1]:
                continue_import = True
        if continue_import :
            observations = self.readASCIIData_fixedPlatform(filename, header, date_range[0], date_range[1])
        return observations

    def addObs(self, observations):
        # Convert to DataFrame:
        observations = DataFrame.from_dict(observations)
        sites = observations.loc[:, ['lat', 'lon', 'alt', 'height', 'file', 'code', 'name']].drop_duplicates()
        for dummy, row in sites.iterrows():
            # Check if a corresponding site already exists in the database :
            if self.sites.loc[(self.sites.reindex(columns=sites.columns) == row).all(axis=1)].shape[0] == 0 :
                self.sites = self.sites.append(row, ignore_index=True)

            # Retrieve the index of the site (there should be only one!):
            site = self.sites.loc[(self.sites.loc[:, sites.columns] == row).all(axis=1)]
            assert site.shape[0] == 1, logger.error(f"Error importing data from {os.path.basename(site.file)}, {site.shape[0]} entries found for site, instead of exactly 1 expected")
            isite = site.index[0]

            # Create the rows to be added to the self.observations dataframe:
            obs = observations.loc[
                    (observations.loc[:, sites.columns] == row).all(axis=1),
                    ['time', 'lat', 'lon', 'alt', 'height', 'obs', 'err', 'code']
            ]
            obs.loc[:, 'site'] = isite

            # Append to self.observations:
            self.observations = self.observations.append(obs, sort=False)


    def readASCIIData_fixedPlatform(self, filename, header, tmin=datetime(1000,1,1), tmax=datetime(3000,1,1)):
        data = loadtxt(filename, skiprows=header['nlines_header'], dtype=str,delimiter=';')
        ihh = header['columns'].index('Year')
        imn = header['columns'].index('Minute')
        time = array([datetime(yy, mm, dd, hh, mn) for (yy, mm, dd, hh, mn) in data[:, ihh:imn+1].astype(int)])
        selection = (time >= tmin) * (time <= tmax)
        nobs = len(selection)
        obs = data[:, header['columns'].index(header['param'])]
        obs[obs == ''] = 'nan'
        obs = obs.astype(float)
        obs[obs < 0] = nan
        err = data[:, header['columns'].index('Stdev')]
        err[err == ''] = 'nan'
        err = err.astype(float)
        err[err < 0] = nan
        err[err == 999.999] = nan
        if nobs > 0 :
            try :
                observations = {
                    'time':time[selection],
                    'lat':array([header['site_latitude']] * nobs),
                    'lon': array([header['site_longitude']] * nobs),
                    'alt': array([header['site_altitude']] * nobs),
                    'height': data[:, header['columns'].index('SamplingHeight')].astype(float),
                    'obs':obs.astype(float),
                    'err':err.astype(float),
                    'file':array([filename]*nobs),
                    'code':array([header['site_code'].lower()]*nobs),
                    'name':array([header['name']]*nobs)
                }
                return observations
            except :
                import pdb; pdb.set_trace()
        return None

    def read_header(self, filename):
        header = {}
        with open(filename, 'r') as fid :
            lines = [l.strip('#').strip() for l in fid.readlines() if l.startswith('#')]
        for line in lines :
            if ':' in line :
                try :
                    key, value = line.split(':',1)
                    if 'LATITUDE' in key: header['site_latitude'] = float(value.strip().split()[0])
                    if 'LONGITUDE' in key: header['site_longitude'] = float(value.strip().split()[0])
                    if 'ALTITUDE' in key: header['site_altitude'] = float(value.strip().split()[0])
                    if 'HEADER LINES' in key: header['nlines_header'] = int(value.strip())
                    if 'CODE' in key: header['site_code'] = value.strip()
                    if 'STATION NAME' in key: header['name'] = value.strip()
                    if key == 'PARAMETER': header['param'] = value.strip().lower()
                    if key == 'SAMPLING HEIGHTS' : header['sampling_heights'] = [float(v.strip().split(' ')[0]) for v in value.split(',')]
                except :
                    import pdb; pdb.set_trace()
        header['columns'] = lines[-1].strip('#').split(';')
        return header
