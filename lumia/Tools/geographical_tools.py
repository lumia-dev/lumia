#!/usr/bin/env python

from .regions import region

class Region(region):
    def __init__(self, rcf):
        rname = rcf.get('region')
        region.__init__(
            self,
            name=rname,
            lat0=rcf.get('region.%s.lat0'%rname), lat1=rcf.get('region.%s.lat1'%rname), dlat=rcf.get('region.%s.dlat'%rname),
            lon0=rcf.get('region.%s.lon0'%rname), lon1=rcf.get('region.%s.lon1'%rname), dlon=rcf.get('region.%s.dlon'%rname)
        )
