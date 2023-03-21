#!/usr/bin/env python
from rctools import RcFile
import logging
from lumia.Tools.regions import region
from lumia.Tools.geographical_tools import GriddedData
from numpy import array, unique, zeros, zeros_like
from netCDF4 import Dataset
from datetime import timedelta, datetime
import os
from lumia.Tools.system_tools import runcmd
from copy import deepcopy
from lumia.formatters.lagrange import Struct

logger = logging.getLogger(__name__)

class transport(object):
    name = 'tm5'

    def __init__(self, rcf, obs=None):
        logger.info(f"Entering tm5__init__() with self={self}")
        self.rcf = rcf
        self.tm5rc = RcFile(self.rcf.get('model.tm5.rcfile'))
        self.struct = Struct()
        self.start = self.rcf.get('time.start', todate=True, fmt='%Y,%m,%d', tolist=False)
        self.end = self.rcf.get('time.end', todate=True, fmt='%Y,%m,%d', tolist=False)

        if obs is not None :
            self.setupObs(obs)

    def setupObs(self, obsdb):
        self.db = obsdb

    def calcDepartures(self, struct, step=None, serial=False):
        # tmpdir = self.rcf.get('run.path3.temp')
        logger.info(f"Entering tm5.calcDepartures() with self={self}")
        logger.info(f"Entering tm5.calcDepartures() with struct={struct}")
        tmpdir = self.rcf['run']['paths']['temp']        

        # Make sure emissions are in umol:
        struct.to_extensive()

        # Write model inputs:
        self.writeEmissions(struct, tmpdir, writecycles=step not in ['var4d', 'apos'])
        self.db.to_point_input(os.path.join(tmpdir, 'point_input.nc4'))
        self.db.to_stationlist(os.path.join(tmpdir, 'stationlist.tm5'))
        rcf = self.rcf.write(os.path.join(tmpdir, f'forward.{step}.rc'))

        # Run TM5:
        cmd = ['singularity', 'exec',
               '-B', f'{os.environ["TM5_METEO"]}:/meteo',
               '-B', f'{self.rcf.get("path.run")}:/input',
               '-B', f'{self.rcf.get("path.run")}:/output',
               '--home', self.rcf.get('tm5.path'),
               os.path.join(self.rcf.get('tm5.path'), 'singularity/tm5env.sif'),
               '/opt/intel/oneapi/intelpython/latest/envs/tm5/bin/python', '-u',
               os.path.join(self.rcf.get('tm5.path'), 'forward.py'),
               '--start', self.start.strftime('%Y%m%d'),
               '--end', self.end.strftime('%Y%m%d'),
               '--rc', self.rcf.get('model.tm5.rcfile')
        ]

        #executable = self.rcf.get("model.transport.exec")
        #cmd = [sys.executable, '-u', executable, '--rc', rcf, '--forward']
        runcmd(cmd)

        # Import results:
        self.db.read_point_output(os.path.join(tmpdir, 'point/point_output.nc4'))
        self.db.observations.loc[:, 'mix_foreground'] = self.db.observations.loc[:, 'tm5_mixing_ratio'][:]
        self.db.observations.loc[:, f'mix_{step}'] = self.db.observations.mix_foreground + self.db.observations.mix_background
        self.db.observations.loc[:, 'mismatch'] = self.db.observations.mix_foreground + self.db.observations.mix_background - self.db.observations.obs

        return self.db.observations.loc[:, ['mismatch', 'err']]

    def runAdjoint(self, departures):
        # tmpdir = self.rcf.get('run.paths.temp')
        logger.info(f"Entering tm5.runAdjoint() with self={self}")
        logger.info(f"Entering tm5.departures() with self={departures}")
        tmpdir = self.rcf['run']['paths']['temp']

        # Write model inputs
        self.db.observations.loc[:, 'dy'] = departures
        tm5regions = ['glb600x400', 'eur300x200', 'eur100x100']
        self.db.to_departures(os.path.join(tmpdir, 'point/point_departures.nc4'), tm5regions)
        rcf = self.rcf.write(os.path.join(tmpdir, f'adjoint.rc'))

        # Run the adjoint
        cmd = [
            'singularity', 'exec',
            '-B', f'{os.environ["TM5_METEO"]}:/meteo',
            '-B', f'{self.rcf.get("path.run")}:/input',
            '-B', f'{self.rcf.get("path.run")}:/output',
            '--home', self.rcf.get('tm5.path'),
            os.path.join(self.rcf.get('tm5.path'), 'singularity/tm5env.sif'),
            '/opt/intel/oneapi/intelpython/latest/envs/tm5/bin/python',  '-u',
            os.path.join(self.rcf.get('tm5.path'), 'adjoint.py'),
            '--start', self.start.strftime('%Y%m%d'),
            '--end', self.end.strftime('%Y%m%d'),
            '--rc', self.rcf.get('model.tm5.rcfile')
        ]

        #executable = self.rcf.get("model.transport.exec")
        #cmd = [sys.executable, '-u', executable, '--rc', rcf, '--adjoint']
        runcmd(cmd)

        # Import the adjoint emissions
        adjstruct = self.readAdjEmis(os.path.join(tmpdir, 'adj_emissions.nc4'))
        adjstruct.to_extensive()
        logger.info(f"tm5.runAdjoint() return adjstruct={adjstruct}")
        return adjstruct

    def readAdjEmis(self, fname):
        # This does three things:
        # 1) Re-introduce the high resolution temporality.
        # 2) Re-introduce the high spatial resolution
        # 3) Apply the unit conversions

        adjemis = deepcopy(self.struct)

        for cat in adjemis.keys():
            lumreg = adjemis[cat]['region']
            nt = len(adjemis[cat]['time_interval']['time_start'])
            adjemis[cat]['emis'] = zeros((nt, lumreg.nlat, lumreg.nlon))

        with Dataset(fname, 'r') as ds :
            for regname in ds.groups :
                logger.debug(regname)
                tmreg = self.get_tm5_reg(regname)
                for tracer in ds[regname].groups :
                    logger.debug(tracer)
                    for cat in ds[regname][tracer].groups :
                        logger.debug(cat)
                        if self.rcf.get(f'emissions.{cat}.optimize', default=False):
                            data = ds[regname][tracer][cat]['adj_emis'][:]
                            # adjoint of split emis (re-introduce high temporal resolution)
                            data = self.splitEmis_adj(adjemis[cat]['time_interval']['time_start'], data)

                            lumreg = adjemis[cat]['region']

                            # adjoint of regrid (re-introduce high spatial resolution)
                            logger.debug(f'{tracer} : {regname} : {cat} : {data.sum()} : {data.shape} Adjoint emis')
                            data = GriddedData(data, tmreg, padding=0).regrid(lumreg)
                            logger.debug(f'{tracer} : {regname} : {cat} : {data.sum()} : {data.shape} Adjoint emis')
                            logger.debug("The latter should be larger than the former by a factor corresponding to the ratio of grid cell numbers")

                            # adjoint of unit conversions
                            data *= 44.01e-9 / 3600

                            # add to the final adjoint emission structure
                            adjemis[cat]['emis'] += data

        return adjemis

    def writeEmissions(self, emis, path, writecycles=True):
        logger.info(f"Entering tm5.writeEmissions() with emis={emis}")
        tm5emis = {'regions':{}}
        for regname in self.tm5rc.get('regions').split():
            tmreg = self.get_tm5_reg(regname)
            tm5emis[tmreg.name] = {}
            tm5emis['regions'][tmreg.name] = tmreg
            self.tracers = self.tm5rc.get('my.tracer').split()
            for tracer in self.tracers:
                tm5emis[tmreg.name][tracer] = {}
                for cat in emis.keys():
                    # Coarsen the data to the desired zoom region
                    lumreg = region(longitudes=emis[cat]['lons'], latitudes=emis[cat]['lats'])

                    # Convert from umol to kg.tracer/gridbox/s
                    emcat = emis[cat]['emis'] * 44.01e-9 / 3600 #kg/s

                    logger.debug(f'{tracer} : {regname} : {cat} : {emcat.sum() * 3600 * 1.e-12} PgCO2')
                    emcat = GriddedData(emcat, lumreg, padding=0).regrid(tmreg)
                    logger.debug(f'{tracer} : {regname} : {cat} : {emcat.sum() * 3600 * 1.e-12} PgCO2')

                    # Split between emissions and "daily cycles"
                    emtimes = emis[cat]['time_interval']['time_start']
                    tm5emis[tmreg.name][tracer][cat] = self.splitEmis(emtimes, emcat)

                    # Save for the adjoint step
                    self.struct[cat] = {
                        'time_interval' : emis[cat]['time_interval'],
                        'lats' : emis[cat]['lats'],
                        'lons' : emis[cat]['lons'],
                        'region' : lumreg
                    }

        self.writeEmFile(tm5emis, path)
        logger.info(f"Writing tm5emis={tm5emis} to file tm5emis.nc")
        if writecycles :
            self.writeDailyCycles(emtimes, tm5emis, path)

    def writeEmFile(self, emis, path):
        with Dataset(os.path.join(path, 'tm5emis.nc'), 'w') as ds :
            ds.createDimension('itime', 6)
            for reg in emis['regions'].values():
                rgrp = ds.createGroup(reg.name)
                rgrp.createDimension('latitude', reg.nlat)
                rgrp.createDimension('longitude', reg.nlon)
                rgrp.latmin = reg.latmin
                rgrp.latmax = reg.latmax
                rgrp.lonmin = reg.lonmin
                rgrp.lonmax = reg.lonmax
                for tracer in emis[reg.name].keys() :
                    tgrp = rgrp.createGroup(tracer)
                    for cat in emis[reg.name][tracer].keys() :

                        cgrp = tgrp.createGroup(cat)
                        cgrp.createDimension('nt', emis[reg.name][tracer][cat]['emis'].shape[0], )

                        cgrp.createVariable('time_start', 'i2', ('nt', 'itime'))
                        cgrp.createVariable('time_mid', 'i2', ('nt', 'itime'))
                        cgrp.createVariable('time_end', 'i2', ('nt', 'itime'))
                        cgrp.createVariable('emission', 'd', ('nt', 'latitude', 'longitude'))
                        cgrp.createVariable('emission_total', 'd', ('nt',))
                        cgrp.createVariable('time_step_length', 'd', ('nt',))

                        cgrp['time_start'][:] = [t.timetuple()[:6] for t in emis[reg.name][tracer][cat]['times_start']]
                        cgrp['time_end'][:] = [t.timetuple()[:6] for t in emis[reg.name][tracer][cat]['times_end']]
                        cgrp['time_mid'][:] = [t.timetuple()[:6] for t in emis[reg.name][tracer][cat]['times_mid']]
                        cgrp['emission'][:] = emis[reg.name][tracer][cat]['emis']
                        cgrp['emission'].unit = f"Kg {tracer}/grid box/second"
                        cgrp['emission_total'][:] = emis[reg.name][tracer][cat]['emtot']
                        cgrp['emission_total'].comment = f"Total emission for region {reg.name} and category {cat}"
                        cgrp['emission_total'].unit = f"Kg {tracer}/time step"
                        cgrp['time_step_length'][:] = emis[reg.name][tracer][cat]['tstep']
                        cgrp['time_step_length'].comment = "Length of time period (seconds)"

                        cgrp.time_resolution = 'daily'
                        cgrp.optimize = 0

    def writeDailyCycles(self, times, emis, path):
        days = array([t.date() for t in times])
        for tracer in self.tracers :
            for day in unique(days):
                folder = os.path.join(path, day.strftime(f'dailycycles/%Y/%m'))
                if not os.path.exists(folder):
                    os.makedirs(folder)
                with Dataset(os.path.join(folder, day.strftime(f'dailycycle.{tracer}_%Y%m%d.nc4')), 'w') as ds :
                    for reg in emis['regions'].values() :
                        rgrp = ds.createGroup(reg.name)
                        rgrp.createDimension('latitude', reg.nlat)
                        rgrp.createDimension('longitude', reg.nlon)
                        for cat in emis[reg.name][tracer].keys():
                            cgrp = rgrp.createGroup(cat)
                            cgrp.createDimension('timesteps', 24)
                            cgrp.createVariable('emission_anomaly', 'd', ('timesteps', 'latitude', 'longitude'))
                            cgrp['emission_anomaly'][:] = emis[reg.name][tracer][cat]['anomalies'][days == day, :, :]

    def get_tm5_reg(self, regname):
        # 1) Get the TM5 region characteristics:
        x0 = self.tm5rc.get(f'region.{regname}.xbeg')
        y0 = self.tm5rc.get(f'region.{regname}.ybeg')
        x1 = self.tm5rc.get(f'region.{regname}.xend')
        y1 = self.tm5rc.get(f'region.{regname}.yend')
        nx = self.tm5rc.get(f'region.{regname}.im')
        ny = self.tm5rc.get(f'region.{regname}.jm')
        return region(lon0=x0, lon1=x1, nlon=nx, lat0=y0, lat1=y1, nlat=ny, name=regname)

    def splitEmis(self, times, emfine):
        #TODO: this enforces hourly time step in TM5 ... maybe this is not a good idea?
        days = array([t.date() for t in times])
        ndays = len(unique(days))
        emcoarse = zeros((ndays, emfine.shape[1], emfine.shape[2]))
        anomalies = zeros_like(emfine)
        for iday, day in enumerate(unique(days)):
            emday = emfine[days == day, :, :]
            emcoarse[iday, :, :] = emday.mean(0)
            anomalies[days == day, :, :] = emfine[days == day, :, :] - emday.mean(0)


        #assert emfine.mean() == 0
        tstart = array([datetime(d.year, d.month, d.day) for d in unique(days)])
        tend = tstart+timedelta(days=1)
        tmid = tstart+timedelta(hours=12)
        tstep = [dt.total_seconds() for dt in tend-tstart]
        return {
            'times_start':tstart, 'times_end':tend, 'times_mid':tmid, 'tstep':tstep, 'emis':emcoarse,
            'anomalies':anomalies, 'emtot':emcoarse.sum((1,2))*tstep
        }

    def splitEmis_adj(self, times, emcoarse):
        """
        Just increase the temporality of the emissions to their original resolution
        The "vec2struct_adj" code will re-coarsen it later to whatever temporal resolution
        is used by the inversion
        """
        _, nlat, nlon = emcoarse.shape
        nt = len(times)
        days = array([t.date() for t in times])
        emfine = zeros((nt, nlat, nlon))
        for iday, day in enumerate(unique(days)):
            emfine[days == day, :, :] = emcoarse[iday, :, :]
        return emfine
