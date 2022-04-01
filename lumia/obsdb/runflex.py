import os
import glob
import re
from lumia.obsdb import obsdb
import h5py
from datetime import datetime
from pandas import DataFrame
from loguru import logger
from tqdm import tqdm


class obsdb(obsdb):
    def __init__(self, footprints_path_or_pattern, start, end, **kwargs):
        super().__init__(**kwargs)
        self.read_footprintFiles(footprints_path_or_pattern)
        self.SelectTimes(start, end, copy=False)
        logger.info(f'Done importing {self.observations.shape[0]} observations from {self.sites.shape[0]} sites')

    def read_footprintFiles(self, path_or_pattern):
        if os.path.isdir(path_or_pattern):
            pattern = f'{path_or_pattern}/*.*m.????-??.hdf'
        else :
            pattern = path_or_pattern
        files = glob.glob(pattern)
        for file in tqdm(files, desc=f"Defining obs from footprint files in {path_or_pattern}") :
            df = self.read_footprintFile(file)
            df.loc[:, 'footprint'] = file
            self.observations = self.observations.append(df, ignore_index=True)
        self.observations.loc[:, 'obs'] = 0.
        self.observations.loc[:, 'err'] = 1.
        sites = self.observations.drop_duplicates(subset=['site', 'lat', 'lon', 'height'])
        self.sites.loc[:, 'code'] = sites.site
        self.sites.loc[:, 'name'] = sites.site
        self.sites.loc[:, 'site'] = sites.site
        self.sites.loc[:, 'lat'] = sites.lat
        self.sites.loc[:, 'lon'] = sites.lon
        self.sites.loc[:, 'alt'] = sites.alt
        self.sites.loc[:, 'height'] = sites.height
        self.sites.set_index('site', inplace=True)

    def read_footprintFile(self, fname):
        with h5py.File(fname) as fid :
            items = fid.keys()
            obs = [o for o in items if re.match('^[0-9a-zA-Z]*.[0-9]*m.[0-9]{8}-[0-9]{6}', o)]

            # For each obs, we want to extract :
            # - the site code
            # - the time
            # - the lat/lon coordinates
            # - the release height
            # ==> more flags would be helpful but unavailable

            data = dict()
            data['site'] = [o.split('.')[0] for o in obs]
            data['time'] = [datetime.strptime(fid[o].attrs['release_time'], '%Y-%m-%d %H:%M:%S') for o in obs]
            data['lat'] = [float(fid[o].attrs['release_lat']) for o in obs]
            data['lon'] = [float(fid[o].attrs['release_lon']) for o in obs]
            data['alt'] = [float(fid[o].attrs['release_height']) for o in obs]
            data['obsid'] = [fid[o].attrs['release_id'] for o in obs]
            data['kindz'] = [int(fid[o].attrs['release_kindz']) for o in obs]
            data['height'] = [int(o.split('.')[1][:-1]) for o in data['obsid']]

        return DataFrame.from_dict(data)