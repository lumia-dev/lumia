import os
import glob
import re
from lumia.obsdb import obsdb
import h5py
from datetime import datetime
from pandas import DataFrame, concat
from loguru import logger
from tqdm import tqdm
from typing import List


class obsdb(obsdb):
    def __init__(self, footprints_path_or_pattern, start, end, tracers: List[str] = None, **kwargs):
        super().__init__(**kwargs)
        obs, sit = self.read_footprintFiles(footprints_path_or_pattern)
        observations = []
        sites = []

        # Add the tracers:
        for tracer in tracers:
            obs_tracer = obs.copy()
            obs_tracer.loc[:, 'tracer'] = tracer
            observations.append(obs_tracer)
            sites_tracer = sit.copy()
            sites_tracer.loc[:, 'tracer'] = tracer
            sites.append(sites_tracer)

        self.observations = concat(observations)
        self.sites = concat(sites)

        self.SelectTimes(start, end, copy=False)
        logger.info(f'Done importing {self.observations.shape[0]} observations from {self.sites.shape[0]} sites')

    def read_footprintFiles(self, path_or_pattern) -> (DataFrame, DataFrame):
        if os.path.isdir(path_or_pattern):
            pattern = f'{path_or_pattern}/*.*m.????-??.hdf'
        else :
            pattern = path_or_pattern
        files = glob.glob(pattern)

        if len(files) == 0:
             logger.error(f"No footprint files matching pattern {path_or_pattern} found")
             raise RuntimeError

        dfs = []
        for file in tqdm(files, desc=f"Defining obs from footprint files in {path_or_pattern}") :
            df = self.read_footprintFile(file)
            df.loc[:, 'footprint'] = file
            dfs.append(df)
        observations = concat(dfs, ignore_index=True)
        observations.loc[:, 'obs'] = 0.
        observations.loc[:, 'err'] = 1.
        observations.loc[:, 'code'] = observations.site
        sites = observations.drop_duplicates(subset=['site', 'lat', 'lon', 'height']).copy()
        sites.loc[:, 'code'] = sites.site
        sites.loc[:, 'name'] = sites.site
        sites.loc[:, 'site'] = sites.site
        sites.loc[:, 'lat'] = sites.lat
        sites.loc[:, 'lon'] = sites.lon
        sites.loc[:, 'alt'] = sites.alt
        sites.loc[:, 'height'] = sites.height
        sites.set_index('site', inplace=True)
        return observations, sites

    def read_footprintFile(self, fname):
        with h5py.File(fname) as fid :
            items = fid.keys()
            obs = [o for o in items if re.match('^[0-9a-zA-Z]*.[0-9]*m.[0-9]{8}-[0-9]{4}', o)]

            # For each obs, we want to extract :
            # - the site code
            # - the time
            # - the lat/lon coordinates
            # - the release height
            # ==> more flags would be helpful but unavailable

            data = dict()
            data['site'] = [o.split('.')[0] for o in obs]
            data['time'] = [datetime.strptime(fid[o].attrs['release_end'], '%Y-%m-%d %H:%M:%S') for o in obs]
            data['lat'] = [float(fid[o].attrs['release_lat1']) for o in obs]
            data['lon'] = [float(fid[o].attrs['release_lon1']) for o in obs]
            data['alt'] = [float(fid[o].attrs['release_z1']) for o in obs]
            data['obsid'] = [fid[o].attrs['release_name'] for o in obs]
            data['kindz'] = [int(fid[o].attrs['release_kindz']) for o in obs]
            data['height'] = [int(o.split('.')[1][:-1]) for o in data['obsid']]

        return DataFrame.from_dict(data)