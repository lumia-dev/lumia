#!/usr/bin/env python
from email.policy import default
import sys
import os
import shutil
from numpy import ones, array
from lumia.Tools import checkDir, debug
from lumia.obsdb import obsdb
from lumia.Tools.system_tools import runcmd
from loguru import logger
import pdb


class transport(object):
    # name = 'lagrange'

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
            self.createStruct = formatter.CreateStruct

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

        rcfile = self.rcf.write(os.path.join(path, 'transport.%src'%tag))
        obsfile = self.db.save_tar(os.path.join(path, 'observations.%star.gz'%tag))
        if structf is not None :
            try :
                shutil.copy(structf, path)
            except shutil.SameFileError :
                pass
        return rcfile, obsfile

    # @debug.trace_call
    def runForward(self, struct, atmos_del=None, step=None, serial=False):
        """
        Prepare input data for a forward run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly.        
        """

        # Write model inputs:
        compression = True# step in self.rcf.get('transport.output.steps', default=True) # Do not compress during 4DVAR loop, for better speed. #TODO:
        emf = self.writeStruct(struct, self.tempdir, prefix='modelData.%s'%step, zlib=compression)
        if atmos_del is not None:
            self.atmdf = self.writeStruct(atmos_del, self.tempdir, prefix='atmosDelta', atmos_del=True)
        else:
            self.atmdf = None

        dbf = self.db.save_tar(os.path.join(self.tempdir, 'observations.%s.tar.gz'%step))
        rcf = self.rcf.write(os.path.join(self.tempdir, f'forward.{step}.rc'))

        # Run the model
        if self.atmdf is not None:
            cmd = [sys.executable, '-u',self.executable, '--rc', rcf, '--forward', '--db', dbf, '--emis', emf, '--atmdel', self.atmdf]#, '--serial']#, '--checkfile', checkf, '--serial']
        else:
            cmd = [sys.executable, '-u', self.executable, '--rc', rcf, '--forward', '--db', dbf, '--emis', emf]
        if self.serial :
            cmd.append('--serial')
        cmd.extend(self.rcf.get('model.transport.extra_arguments', default='').split(','))
        runcmd(cmd)

        # Retrieve results :
        return emf, dbf

    # @debug.trace_call
    def calcDepartures(self, struct, atmdel=None, step=None, serial=False):
        emf, dbf = self.runForward(struct, atmos_del=atmdel, step=step, serial=serial)
        db = obsdb(filename=dbf)

        if self.rcf.get('model.split.categories', default=True):
            for tr in self.rcf.get('obs.tracers'):
                tr_columns = [col for col in db.observations.columns if f'mix_{tr}' in col]
                self.db.observations.loc[:, db.observations[tr_columns].columns] = db.observations[tr_columns]

        self.db.observations.loc[:, f'mix_{step}'] = db.observations.mix.values
        self.db.observations.loc[:, 'mix_background'] = db.observations.mix_background.values
        self.db.observations.loc[:, 'mismatch'] = db.observations.mix.values-self.db.observations.loc[:,'obs']
        self.db.observations.loc[:, 'mix_foreground'] = db.observations.mix.values-db.observations.mix_background.values

        # Optional: store extra columns that the transport model may have written (to pass them again to the transport model in the following steps)
        for key in self.rcf.get('model.obs.extra_keys', default=[], tolist=True) :
            self.db.observations.loc[:, key] = db.observations.loc[:, key].values

        self.db.observations.dropna(subset=['mismatch'], inplace=True)

        # Output if needed:
        if self.rcf.get('transport.output'):
            if step in self.rcf.get('transport.output.steps', default=True):
                self.save(tag=step, structf=emf)

        # Return model-data mismatches
        return self.db.observations.loc[:, ('mismatch', 'err')]

    def runAdjoint(self, departures, atmdel=None):
        """
        Prepare input for the adjoint run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly
        """

        self.db.observations.loc[:, 'dy'] = departures
        dpf = self.db.save_tar(os.path.join(self.tempdir, 'departures.tar.gz'))
        
        # Create an adjoint rc-file
        rcadj = self.rcf.write(os.path.join(self.tempdir, 'adjoint.rc'))

        # Name of the adjoint output file
        adjf = os.path.join(self.tempdir, 'adjoint.nc')

        # Run the adjoint transport:
        if atmdel is not None:
            # Name of the atmospheric delta file
            atmdel = os.path.join(self.tempdir, 'atmosDelta.nc') # Usefull for the opt.4dvar, do not delete

            # Run the adjoint transport:
            cmd = [sys.executable, '-u', self.executable, '--adjoint', '--db', dpf, '--rc', rcadj, '--emis', adjf, '--atmdel', atmdel]#, '--serial']#, '--checkfile', checkf, '--serial']
        else:
            cmd = [sys.executable, '-u', self.executable, '--adjoint', '--db', dpf, '--rc', rcadj, '--emis', adjf]
        if self.serial :
            cmd.append('--serial')
        cmd.extend(self.rcf.get('model.transport.extra_arguments', default='').split(','))

        runcmd(cmd)

        # Collect the results :
        return self.readStruct(self.tempdir, prefix='adjoint')

    def calcSensitivityMap(self):
        departures = ones(self.db.observations.shape[0])
        try :
            adjfield = self.readStruct(self.tempdir, prefix='adjoint')
        except :
            if hasattr(self, 'atmdf') :
                adjfield = self.runAdjoint(departures, self.atmdf)
            else :
                adjfield = self.runAdjoint(departures)

        return array([adjfield[tr][cat]['emis'].sum(0) for tr in adjfield.keys() for cat in adjfield[tr].keys()]).sum(0)
    
    def calcSensitivityMapPerTracer(self):
        departures = ones(self.db.observations.shape[0])
        try :
            adjfield = self.readStruct(self.tempdir, prefix='adjoint')
        except :
            if hasattr(self, 'atmdf') :
                adjfield = self.runAdjoint(departures, self.atmdf)
            else :
                adjfield = self.runAdjoint(departures)

        sensi = {}
        for tracer in adjfield.keys():
            sensi[tracer] = {}
            for cat in adjfield[tracer].keys():
                sensi[tracer][cat] = adjfield[tracer][cat]['emis'].sum(0)

        return sensi