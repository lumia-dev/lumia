#!/usr/bin/env python
import sys
import os
import shutil
from numpy import ones, array, prod, append
from pandas.api.types import is_float_dtype
from lumia.Tools import checkDir
from lumia.obsdb import obsdb
from lumia.Tools.system_tools import runcmd
from loguru import logger


class transport(object):
    name = 'lagrange'

    def __init__(self, rcf, obs=None, formatter=None):
        self.rcf = rcf

        # Set paths :
        self.outputdir = self.rcf.get('model.path.output')
        self.tempdir = self.rcf.get('model.path.temp', self.outputdir)
        self.executable = self.rcf.get("model.exec")
        self.serial = self.rcf.get("model.options.serial", default=False)
        self.footprint_path = self.rcf.get('model.path.footprints')

        # Initialize the obs if needed
        if obs is not None : 
            self.setupObs(obs)
            
        if formatter is not None :
            self.writeStruct = formatter.WriteStruct
            self.readStruct = formatter.ReadStruct
            #self.createStruct = formatter.CreateStruct

    def setupObs(self, obsdb):
        self.db = obsdb

        # Just to ensure that calcDepartures work even if no obs err has been provided
        if ('err' not in self.db.observations) :
            logger.warning("The observations provided do not contain an 'err' column. You may want to check this.")
            self.db.observations.loc[:, 'err'] = None

    def save(self, path=None, tag=None, structf=None):
        """
        This copies the last model I/O to "path", with an optional tag to identify it
        """
        tag = '' if tag is None else tag.strip('.')+'.'
        if path is None :
            path = self.outputdir
        checkDir(path)

        rcfile = self.rcf.write(os.path.join(path, f'transport.{tag}rc'))
        obsfile = self.db.save_tar(os.path.join(path, f'observations.{tag}tar.gz'))
        if structf is not None :
            try :
                shutil.copy(structf, path)
            except shutil.SameFileError :
                pass
        return rcfile, obsfile

    def run_forward(self, struct, observations: obsdb = None, serial: bool = False, step: str = 'forward') -> obsdb:
        struct.to_intensive()
        emf, dbf = self.runForward(struct, step=step, serial=serial, observations=observations)
        db = obsdb.from_hdf(dbf)
        db.save_tar(os.path.join(self.outputdir, f'observations.{step}.tar.gz'))

    def calcDepartures(self, struct, step=None, serial=False):
        logger.info(f"Dbg: self.db.observations ENTERING_calcDepartures: {self.db.observations}")
        self.db.observations.to_csv('obsoperator_init_ENTERING_calcDepartures_self-db-observations.csv', encoding='utf-8', sep=',', mode='w')
        emf, dbf = self.runForward(struct, step, serial)
        logger.info(f'in calcDepartures() reading db from file dbf={dbf}')
        db = obsdb.from_hdf(dbf)
        print(db)
        #logger.info("db.columns=")
        #print(db.columns)
        logger.info(f"Dbg: db_obsdb.from_hdf() AfterFWD: {self.db.observations}")
        self.db.observations.to_csv('obs_hdf_init_calcDepartures_AfterFWD_self-db-observations.csv', encoding='utf-8', sep=',', mode='w')
        logger.info(f"Dbg: self.db.observations AfterFWD: {self.db.observations}")
        self.db.observations.to_csv('obsoperator_init_calcDepartures_AfterFWD_self-db-observations.csv', encoding='utf-8', sep=',', mode='w')
        # db = db.reset_index(drop=True)
        logger.info('in calcDepartures() db=')
        print(db)
        logger.info(f'in calcDepartures() emf={emf}')
        logger.info('in calcDepartures() self.db.observations=')
        print(self.db.observations)
        #  We need to ensure that all columns containing float values are perceived as such and not as object or string dtypes -- or havoc rages down the line
        knownColumns=['stddev', 'obs','err_obs', 'err', 'lat', 'lon', 'alt', 'height', 'background', 'mix_fossil', 'mix_biosphere', 'mix_ocean', 'mix_background', 'mix']
        for col in knownColumns:
            if col in db.sites.columns: # db.columns:
                logger.info(f'in calcDepartures() db.sites.col={col}')
                if(is_float_dtype(db.sites[col])==False):
                    db.sites[col]=db.sites[col].astype(float)
        logger.debug(f"Dbg: self.db.observations: {self.db.observations}")
        # if DEBUG: self.db.observations.to_csv('obsoperator_init_calcDepartures_AfterFWD_self-db-observations.csv', encoding='utf-8', sep=',', mode='a')
        if self.rcf.get('model.split_categories', default=True):
            import time
            time.sleep(5)
            logger.info("obsoperator._init_.calcDepartures() L74 self=")
            print(self)
            logger.info("obsoperator._init_.calcDepartures() L74 self.db.observations=")
            print(self.db.observations)
            logger.info("obsoperator._init_.calcDepartures() L74 self.db.observations.columns=")
            print(self.db.observations.columns)
            for cat in struct.transported_categories:
                self.db.observations.loc[:, f'mix_{cat.name}'] = db.observations.loc[:, f'mix_{cat.name}'].values

        if(is_float_dtype(db.observations.mix.values)==False):
            db.observations.mix.values=db.observations.mix.values.astype(float)
        if(is_float_dtype(db.observations.mix_background.values)==False):
            db.observations.mix_background.values=db.observations.mix_background.values.astype(float)
        if(is_float_dtype(self.db.observations.obs)==False):
            self.db.observations.obs=self.db.observations.obs.astype(float)

        self.db.observations.loc[:, f'mix_{step}'] = db.observations.mix.values
        self.db.observations.loc[:, 'mix_background'] = db.observations.mix_background.values
        self.db.observations.loc[:, 'mix_foreground'] = db.observations.mix.values-db.observations.mix_background.values
        self.db.observations.loc[:, 'mismatch'] = db.observations.mix.values-self.db.observations.loc[:,'obs']

        # Optional: store extra columns that the transport model may have written (to pass them again to the transport model in the following steps)
        for key in list(self.rcf.get('model.store_extra_fields', default=[])) :
            if(is_float_dtype(db.observations.loc[:, key].values)==False):
                db.observations.loc[:, key].values=db.observations.loc[:, key].values.astype(float)
            self.db.observations.loc[:, key] = db.observations.loc[:, key].values

        self.db.observations.dropna(subset=['mismatch'], inplace=True)

        # Output if needed:
        if step not in self.rcf.get('model.no_output', ['var4d']):
            self.save(tag=step, structf=emf)

        # Return model-data mismatches
        return self.db.observations.loc[:, ('mismatch', 'err')]

    def runForward(self, struct, step=None, serial=False, observations: obsdb = None):
        """
        Prepare input data for a forward run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly. struct can hold the emissions on a lat/lon grid for ocean, anthropogenic and biosphere-model       
        """

        logger.info('Entering obsoperator.init.runForward()')
        # Write model inputs:
        if observations is None :
            observations = self.db
            logger.info('obsoperator.init.runForward(): observations set to self.db')

        compression = step in self.rcf.get('model.output.steps', default=[]) # Do not compress during 4DVAR loop, for better speed.
        emf = self.writeStruct(struct, path=os.path.join(self.tempdir, 'emissions.nc'), zlib=compression, only_transported=True)
        del struct
        dbf = observations.to_hdf(os.path.join(self.tempdir, 'observations.hdf'))
        
        logger.info(f'obsoperator.init.runForward(): dbf={dbf}')

        # Run the model
        cmd = [sys.executable, '-u', self.executable, '--forward', '--obs', dbf, '--emis', emf, '--footprints', self.footprint_path, '--tmp', self.tempdir]

        if self.serial or serial:
            cmd.append('--serial')
            
        logger.info('obsoperator.init.runForward(): cmd=')
        print(cmd)
        
        cmd.extend(self.rcf.get('model.transport.extra_arguments', default=[]))
        # TODO: uncomment the next line  (runcmd multitracer.py) when done debugging 
        runcmd(cmd)

        logger.info('obsoperator.init.runForward(): emf2=')
        if(emf is None):
            logger.info('Error: obsoperator.init.runForward(): emf is None. That is a problem...')
        else:
            print(emf)
        logger.info('obsoperator.init.runForward(): dbf2=')
        if(dbf is None):
            logger.info('Error: obsoperator.init.runForward(): dbf is None. That is a problem...')
        else:
            print(dbf)

        # Retrieve results :
        return emf, dbf

    def runAdjoint(self, departures):
        """
        Prepare input for the adjoint run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly
        """
        
        self.db.observations.loc[:, 'dy'] = departures
        depout=os.path.join(self.tempdir, 'departures.hdf')
        logger.info(f"Writing departures to {depout}")
        dpf = self.db.to_hdf(os.path.join(self.tempdir, 'departures.hdf'))
        
        # Name of the adjoint output file
        adjf = os.path.join(self.tempdir, 'emissions.nc')

        # Run the adjoint transport:
        cmd = [sys.executable, '-u', self.executable, '--adjoint', '--obs', dpf, '--emis', adjf, '--footprints', self.footprint_path, '--tmp', self.tempdir]

        if self.serial :
            cmd.append('--serial')
        cmd.extend(list(self.rcf.get('model.transport.extra_arguments', default='')))
        runcmd(cmd)

        # Collect the results :
        return self.readStruct(path=adjf)

    def calcSensitivityMap(self, struct):
        departures = ones(self.db.observations.shape[0])
#        try :
#            adjfield = self.readStruct(self.tempdir, 'adjoint')
#        except :
        self.writeStruct(struct, os.path.join(self.tempdir, 'emissions.nc'), zlib=True)
        adjfield = self.runAdjoint(departures)

        sensi = {}
        for tracer in adjfield.tracers :
            sensi[tracer] = array([adjfield[tracer][cat].data.sum(0) for cat in adjfield[tracer].categories]).sum(0)
        return sensi

    # def adjoint_test_(self, struct):
    #     # Write model inputs:
    #     emf = self.writeStruct(struct, self.tempdir, 'modelData.adjtest', zlib=True)
    #     dbf = self.db.save_tar(os.path.join(self.tempdir, 'observations.adjtest.tar.gz'))
    #     rcf = self.rcf.write(os.path.join(self.tempdir, f'forward.adjtest.rc'))
    #
    #     # Run the model
    #     cmd = [sys.executable, '-u', self.executable, '--rc', rcf, '--adjtest', '--emis', emf, '--db', dbf]
    #     if self.serial :
    #         cmd.append('--serial')
    #     cmd.extend(self.rcf.get('model.transport.extra_arguments', default='').split(','))
    #     runcmd(cmd)

    def adjoint_test(self, struct):
        from numpy import dot, random

        # Set background to zero:
        #self.db.observations.loc[:, 'mix_background'] = 0.

        # 1) Do a first forward run with these emissions:
        #self.serial = True

        _, dbf = self.runForward(struct, step='adjtest1')
        db = obsdb.from_hdf(dbf)
        db.observations = db.observations.dropna(subset=['mix'])
        y1 = db.observations.mix.values

        # 2) Do a second forward run, with perturbed emissions :
        dx1 = array([])
        for cat in struct.categories :
            dx = random.randn(prod(struct[cat.tracer].shape))
            struct[cat.tracer][cat.name].data += dx.reshape(struct[cat.tracer].shape)
            dx1 = append(dx1, dx)

        _, dbf = self.runForward(struct, step='adjtest2')
        db = obsdb.from_hdf(dbf)
        db.observations = db.observations.dropna(subset=['mix'])
        y2 = db.observations.mix.values
        dy1 = y2-y1

        # 3) Do an adjoint run :
        db.observations.loc[:, 'dy'] = dy1
        adj = self.runAdjoint(db.observations.loc[:, 'dy'])

        # 4) Convert to vectors:
        dy2 = self.db.observations.loc[:, 'dy'].dropna().values
        dx2 = array([])
        for cat in struct.categories :
            dx2 = append(dx2, adj[cat.tracer][cat.name].data.reshape(-1))
        
        logger.info(f"Adjoint test value: { 1 - dot(dy1, dy2) / dot(dx1, dx2) = }")
        import pdb; pdb.set_trace()
