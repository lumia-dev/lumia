#!/usr/bin/env python
import sys
import os
import shutil
from numpy import ones, array
from lumia import paths
from lumia.Tools import checkDir
from lumia.obsdb import obsdb
from lumia.Tools.system_tools import runcmd
from loguru import logger


class transport(object):
    name = 'lagrange'

    def __init__(self, rcf, obs=None, formatter=None):
        self.rcf = rcf

        # Set paths :
        self.outputdir = self.rcf.get('path.output')
        self.tempdir = self.rcf.get('path.temp', self.outputdir)
        self.executable = self.rcf.get("model.transport.exec")
        self.serial = self.rcf.get("model.transport.serial", default=False)

        # Initialize the obs if needed
        if obs is not None : 
            self.setupObs(obs)
            
        if formatter is not None :
            self.writeStruct = formatter.WriteStruct
            self.readStruct = formatter.ReadStruct
            #self.createStruct = formatter.CreateStruct

    def setupObs(self, obsdb):
        self.db = obsdb

    def save(self, path=None, tag=None, structf=None):
        """
        This copies the last model I/O to "path", with an optional tag to identify it
        """
        tag = '' if tag is None else tag.strip('.')+'.'
        if path is None :
            path = self.rcf.get('path.output')
        checkDir(path)

        rcfile = self.rcf.write(os.path.join(path, f'transport.{tag}rc'))
        obsfile = self.db.save_tar(os.path.join(path, f'observations.{tag}tar.gz'))
        if structf is not None :
            try :
                shutil.copy(structf, path)
            except shutil.SameFileError :
                pass
        return rcfile, obsfile

    def calcDepartures(self, struct, step=None, serial=False):
        emf, dbf = self.runForward(struct, step, serial)
        db = obsdb.from_hdf(dbf)
        if self.rcf.get('model.split.categories', default=True):
            for cat in struct.categories:
            #for cat in self.rcf.get('emissions.categories'):
                self.db.observations.loc[:, f'mix_{cat.name}'] = db.observations.loc[:, f'mix_{cat.name}'].values
        self.db.observations.loc[:, f'mix_{step}'] = db.observations.mix.values
        self.db.observations.loc[:, 'mix_background'] = db.observations.mix_background.values
        self.db.observations.loc[:, 'mix_foreground'] = db.observations.mix.values-db.observations.mix_background.values
        self.db.observations.loc[:, 'mismatch'] = db.observations.mix.values-self.db.observations.loc[:,'obs']

        # Optional: store extra columns that the transport model may have written (to pass them again to the transport model in the following steps)
        for key in self.rcf.get('model.obs.extra_keys', default=[], tolist=True) :
            self.db.observations.loc[:, key] = db.observations.loc[:, key].values

        self.db.observations.dropna(subset=['mismatch'], inplace=True)

        # Output if needed:
        if self.rcf.get('transport.output'):
            if step in self.rcf.get('transport.output.steps'):
                self.save(tag=step, structf=emf)

        # Return model-data mismatches
        return self.db.observations.loc[:, ('mismatch', 'err')]

    def runForward(self, struct, step=None, serial=False):
        """
        Prepare input data for a forward run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly.        
        """

        # Write model inputs:
        compression = step in self.rcf.get('transport.output.steps') # Do not compress during 4DVAR loop, for better speed.
        emf = self.writeStruct(struct, path=os.path.join(self.tempdir, 'emissions.nc'), zlib=compression)
        dbf = self.db.to_hdf(os.path.join(self.tempdir, 'observations.hdf'))
        
        # Run the model
        cmd = [sys.executable, '-u', self.executable, '--forward', '--obs', dbf, '--emis', emf, '--footprints', self.rcf.get('path.footprints'), '--tmp', paths.temp]
        if self.serial :
            cmd.append('--serial')
        cmd.extend(self.rcf.get('model.transport.extra_arguments', default='').split(' '))
        runcmd(cmd)

        # Retrieve results :
        return emf, dbf

    def runAdjoint(self, departures):
        """
        Prepare input for the adjoint run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly
        """
        
        self.db.observations.loc[:, 'dy'] = departures
        dpf = self.db.to_hdf(os.path.join(self.tempdir, 'departures.hdf'))
        
        # Name of the adjoint output file
        adjf = os.path.join(self.tempdir, 'emissions.nc')

        # Run the adjoint transport:
        cmd = [sys.executable, '-u', self.executable, '--adjoint', '--obs', dpf, '--emis', adjf, '--footprints', self.rcf.get('path.footprints'), '--tmp', paths.temp]
        if self.serial :
            cmd.append('--serial')
        cmd.extend(self.rcf.get('model.transport.extra_arguments', default='').split(' '))
        runcmd(cmd)

        # Collect the results :
        return self.readStruct(path=adjf)

    def calcSensitivityMap(self, struct):
        departures = ones(self.db.observations.shape[0])
#        try :
#            adjfield = self.readStruct(self.tempdir, 'adjoint')
#        except :
        self.writeStruct(struct, os.path.join(paths.temp, 'emissions.nc'), zlib=True)
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

        # 1) Do a first forward run with these emissions:
        _, dbf = self.runForward(struct, step='adjtest1')
        db = obsdb(filename=dbf)
        y1 = db.observations.loc[:, 'mix'].dropna().values

        # 2) Do a second forward run, with perturbed emissions :
        x1 = struct['biosphere']['emis'].reshape(-1)
        dx = random.randn(x1.shape[0])
        struct['biosphere']['emis'] += dx.reshape(*struct['biosphere']['emis'].shape)
        _, dbf = self.runForward(struct, step='adjtest2')
        db = obsdb(filename=dbf)
        y2 = db.observations.loc[:, 'mix'].dropna().values
        dy = y2-y1

        # 3) Do an adjoint run :
        adj = self.runAdjoint(db.observations.loc[:, 'mix_biosphere'])

        # 4) Convert to vectors:
        y2 = self.db.observations.loc[:, 'dy'].dropna().values
        x2 = adj['biosphere']['emis'].reshape(-1)
        logger.info(f"Adjoint test value: { 1 - dot(dy, y2) / dot(dx, x2) = }")