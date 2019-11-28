#!/usr/bin/env python

from lumia import obsdb as Obsdb_base
from datetime import *
import logging
logger = logging.getLogger(__name__)

class RunFlexDB(Obsdb_base):
    def configure(self, config_file):
        """
        Read settings from a station list file
        """
        slist = self.parse_config_file(config_file)
        for isite, site in self.sites.iterrows():
            self.observations.loc[self.observations.site == isite, 'code'] = site.code
            self.observations.loc[self.observations.site == isite, 'siteAlt'] = site.alt
            self.observations.loc[self.observations.site == isite, 'kindz'] = slist[site.code]['kindz']

    def export(self, filename):
        self.observations.to_hdf(filename, 'obs', mode='w')

    def parse_config_file(self, file):
        sites = {}
        with open(file, 'r') as fid :
            lines = [l for l in fid.readlines() if not l.startswith('!')]
        for line in lines:
            line = line.split('#')[0]
            fields = [x.strip() for x in line.split(';')]
            sites[fields[0]] = {
                'interval' : fields[1],
                'selected' : True,
                'restricted' : None
            }
            if len(fields) > 2 :
                if fields[2] == 'X' :
                    sites[fields[0]]['selected'] = False
            if len(fields) > 3 and fields[3] != '':
                interval = fields[3].replace('S', self.start.strftime('%Y%m%d')).replace('E', self.end.strftime('%Y%m%d'))
                s, e = interval.split('-')
                sites[fields[0]]['restricted'] = (datetime.strptime(s, '%Y%m%d'), datetime.strptime(e, '%Y%m%d'))
            if len(fields) > 4 and fields[4] != '':
                sites[fields[0]]['transport_error'] = float(fields[4])
            if len(fields) > 5 and fields[5] != '' :
                sites[fields[0]]['kindz'] = int(fields[5])
        return sites