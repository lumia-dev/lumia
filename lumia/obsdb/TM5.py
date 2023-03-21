#!/usr/bin/env python

from numpy.core.numeric import array_equal
from lumia.obsdb import obsdb as base
from netCDF4 import Dataset
from numpy import unique
from collections import defaultdict
from pandas import DataFrame
import logging


logger = logging.getLogger(__name__)


class obsdb(base):
    def to_stationlist(self, filename):
        if 'station_id_TM5' not in self.sites.columns :
            self.sites.loc[:, 'station_id_TM5'] = range(self.sites.shape[0])
            for isite, site in enumerate(self.sites.itertuples()):
                self.observations.loc[self.observations.site == site.code, 'station_id_TM5'] = site.station_id_TM5

        with open(filename, 'w') as fid :
            fid.write(' NUM  ID    LAT     LON     ALT TP STATIONNAME\n')
            for isite, site in enumerate(self.sites.itertuples()):
                fid.write(f' {isite:3.0f} {site.code} {site.lat:6.2f} {site.lon:7.2f} {site.alt:7.1f} FM {site.name}\n')

    def to_departures(self, filename, regions):
        logger.info(f"Entering TM5.to_departures() with self={self}")
        tracers = unique(self.observations.tm5_tracer.values)
        with Dataset(filename, 'w') as ds :
            ds.createDimension('tracer', len(tracers))
            ds.createDimension('idate', 6)
            for region in regions :
                ds.createGroup(region)
                dbr = self.observations.loc[self.observations.tm5_region == region]
                nobs = dbr.shape[0]
                logger.debug(f"Write {nobs} departures for region {region}")
                if nobs > 0 :
                    for tracer in tracers :
                        gr = ds[region].createGroup(tracer)
                        gr.createDimension('samples', nobs)
                        gr.createVariable('date_components', 'i', ('samples', 'idate'))
                        gr.createVariable('lat', 'd', ('samples',))
                        gr.createVariable('lon', 'd', ('samples',))
                        gr.createVariable('alt', 'd', ('samples',))
                        gr.createVariable('total_weight', 'd', ('samples',))
                        gr.createVariable('forcing', 'd', ('samples',))              # (mod-obs)/variance
                        gr.createVariable('nsamples', 'i', ('samples',))
                        gr.createVariable('sampling_strategy', 'i', ('samples',))
                        gr.createVariable('time_window_length', 'i', ('samples',))

                        gr['lat'][:] = dbr.lat.values
                        gr['lon'][:] = dbr.lon.values
                        gr['alt'][:] = dbr.alt.values
                        gr['total_weight'][:] = dbr.tm5_total_weight.values
                        gr['forcing'][:] = dbr.dy.values
                        gr['nsamples'][:] = dbr.tm5_nsamples.values
                        gr['sampling_strategy'][:] = dbr.tm5_sampling_strategy.values
                        gr['time_window_length'][:] = 0.
                        gr['date_components'][:] = [t.timetuple()[:6] for t in dbr.time]

    def to_point_input(self, filename):
        """
        Dimensions:
        - id

        Variables
        - int   :: id(id), date_components(id, 6), sampling_strategy(id), station_id(id)
        - float :: lat(id), lon(id), alt(id)
        """
        logger.info(f"Entering TM5.to_point_input() with self={self}")
        nobs = self.observations.shape[0]

        if 'uid_TM5' not in self.observations.columns :
            self.checkIndex(reindex=True)
            self.observations.loc[:, 'uid_TM5'] = self.observations.index.values

        if 'station_id_TM5' not in self.sites.columns :
            self.sites.loc[:, 'station_id_TM5'] = range(self.sites.shape[0])
            for isite, site in enumerate(self.sites.itertuples()):
                self.observations.loc[self.observations.site == site.code, 'station_id_TM5'] = site.station_id_TM5

        with Dataset(filename, 'w') as ds :
            ds.createDimension('idate', 6)
            gr = ds.createGroup('CO2')
            gr.createDimension('id', nobs)
            gr.createVariable('id', 'i', ('id',))
            gr.createVariable('lon', 'd', ('id',))
            gr.createVariable('lat', 'd', ('id',))
            gr.createVariable('alt', 'd', ('id',))
            gr.createVariable('date_components', 'i2', ('id', 'idate'))
            gr.createVariable('station_id', 'i', ('id',))
            gr.createVariable('sampling_strategy', 'i2', ('id',))

            gr['lon'][:] = self.observations.lon.values
            gr['lat'][:] = self.observations.lat.values
            gr['alt'][:] = self.observations.alt.values
            gr['date_components'][:] = [t.to_pydatetime().timetuple()[:6] for t in self.observations.time]
            gr['id'][:] = self.observations.uid_TM5.values
            gr['station_id'][:] = self.observations.station_id_TM5
            gr['sampling_strategy'][:] = 2

    def read_point_output(self, filename):
        logger.info(f"Entering TM5.to_point_input() with self={self}")
        logger.info(f"Entering TM5.to_point_input() with filename={filename}")
        logger.debug(filename)
        with Dataset(filename, 'r') as ds :
            data = defaultdict(list)
            for region in ds.groups :
                logger.debug(region)
                for tracer in ds[region].groups:
                    logger.debug(tracer)
                    nobs = ds[region][tracer].dimensions['samples'].size
                    data['tm5_region'].extend([region]*nobs)
                    data['tm5_tracer'].extend([tracer]*nobs)
                    for var in ds[region][tracer].variables :
                        logger.debug(var)
                        data[f'tm5_{var}'].extend(ds[region][tracer][var][:])
                    if 'meteo' in ds[region][tracer].groups :
                        for var in ds[region][tracer]['meteo'].variables :
                            logger.debug(var)
                            data[f'tm5_{var}'].extend(ds[region][tracer]['meteo'][var][:])
            df = DataFrame(data).set_index('tm5_id')

        if 'station_id_TM5' not in self.sites.columns :
            # Normally, the "point_output" file is based on a "point_input" that was generated using the "to_point_input" method above, so the "station_id_TM5" column should already be present in the file. If this is not the case, we create it here.

            self.sites.loc[:, 'station_id_TM5'] = range(self.sites.shape[0])
            for isite, site in enumerate(self.sites.itertuples()):
                self.observations.loc[self.observations.site == site.code, 'station_id_TM5'] = site.station_id_TM5

                tm5 = df.loc[df.tm5_station_id == site.station_id_TM5]
                assert tm5.shape[0] == self.observations.loc[self.observations.site == site.code].shape[0]
                for var in tm5.columns :
                    self.observations.loc[self.observations.site == site.code, var] = tm5.loc[:, var].values

        else :
            for var in df.columns :
                logger.debug(var)
                try :
                    self.observations.loc[df.index, var] = df.loc[:, var].values
                except :
                    import pdb; pdb.set_trace()
