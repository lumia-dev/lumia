#!/usr/bin/env python

# from lumia.obsdb.footprintdb import obsdb as FootprintDB
# from datetime import *
# import logging
# logger = logging.getLogger(__name__)

# class RunFlexDB(FootprintDB):
#     def configure(self, config_file):
#         """
#         Read settings from a station list file
#         """
#         slist = self.parse_config_file(config_file)
#         for isite, site in self.sites.iterrows():
#             self.observations.loc[self.observations.site == isite, 'code'] = site.code
#             self.observations.loc[self.observations.site == isite, 'siteAlt'] = site.alt
#             self.observations.loc[self.observations.site == isite, 'kindz'] = slist[site.code]['kindz']

#     def export(self, filename):
#         self.observations.to_hdf(filename, 'obs', mode='w')

#     def parse_config_file(self, file):
#         sites = {}
#         with open(file, 'r') as fid :
#             lines = [l for l in fid.readlines() if not l.startswith('!')]
#         for line in lines:
#             line = line.split('#')[0]
#             fields = [x.strip() for x in line.split(';')]
#             sites[fields[0]] = {
#                 'interval' : fields[1],
#                 'selected' : True,
#                 'restricted' : None
#             }
#             if len(fields) > 2 :
#                 if fields[2] == 'X' :
#                     sites[fields[0]]['selected'] = False
#             if len(fields) > 3 and fields[3] != '':
#                 interval = fields[3].replace('S', self.start.strftime('%Y%m%d')).replace('E', self.end.strftime('%Y%m%d'))
#                 s, e = interval.split('-')
#                 sites[fields[0]]['restricted'] = (datetime.strptime(s, '%Y%m%d'), datetime.strptime(e, '%Y%m%d'))
#             if len(fields) > 4 and fields[4] != '':
#                 sites[fields[0]]['transport_error'] = float(fields[4])
#             if len(fields) > 5 and fields[5] != '' :
#                 sites[fields[0]]['kindz'] = int(fields[5])
#         return sites

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