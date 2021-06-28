#!/usr/bin/env python

from lumia.obsdb import obsdb as base


class obsdb(base):
    def to_stationlist(self, filename):
        with open(filename, 'w') as fid :
            fid.write(' NUM  ID    LAT     LON     ALT TP STATIONNAME\n')
            for isite, site in enumerate(self.sites.itertuples()):
                fid.write(f' {isite:3.0f} {site.code} {site.lat:6.2f} {site.lon:7.2f} {site.alt:7.1f} FM {site.name}\n')