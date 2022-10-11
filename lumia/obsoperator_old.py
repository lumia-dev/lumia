#!/usr/bin/env python
from email.policy import default
import sys
import os
import shutil
import subprocess
import logging
from numpy import ones, array
from lumia.Tools import checkDir, colorize
from .obsdb import obsdb

logger = logging.getLogger(__name__)


class transport(object):
    name = 'lagrange'

    def __init__(self, rcf, obs=None, formatter=None):
        self.rcf = rcf
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

    def runForward(self, struct, atmos_del=None, step=None, serial=False):
        """
        Prepare input data for a forward run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly.        
        """
        # #if struct is None : struct = self.controlstruct
        # self.check_init()

        # read model-specific info
        tmpdir = self.rcf.get('path.temp')
        executable = self.rcf.get("model.transport.exec")
        if self.rcf.get("model.transport.serial", default=False) :
            serial = True

        # Write model inputs:
        emf = self.writeStruct(struct, tmpdir, 'modelData.%s'%step)
        if atmos_del is not None:
            self.atmdf = self.writeStruct(atmos_del, tmpdir, 'atmosDelta', atmos_del=True)
        else:
            self.atmdf = None
        dbf = self.db.save_tar(os.path.join(tmpdir, 'observations.%s.tar.gz'%step)) #TODO: clean
        rcf = self.rcf.write(os.path.join(tmpdir, f'forward.{step}.rc'))
        #checkf = os.path.join(tempfile.mkdtemp(dir=rundir), 'forward.ok')

        # Run the model
        if self.atmdf is not None:
            cmd = [sys.executable, executable, '--rc', rcf, '--forward', '--db', dbf, '--emis', emf, '--atmdel', self.atmdf]#, '--serial']#, '--checkfile', checkf, '--serial']
        else:
            cmd = [sys.executable, executable, '--rc', rcf, '--forward', '--db', dbf, '--emis', emf]#, '--atmdel', atmdf]#, '--serial']#, '--checkfile', checkf, '--serial']
        if serial :
            cmd.append('--serial')
        logger.info(colorize(' '.join([x for x in cmd]), 'g'))
        try :
            subprocess.run(cmd, close_fds=True)
        except subprocess.CalledProcessError :
            logger.error("Forward run failed, exiting ...")
            raise subprocess.CalledProcessError

        # Retrieve results :
        db = obsdb(filename=dbf)

        for tr in db.observations.tracer.unique():#list(self.rcf.get('obs.tracers')): #TODO: be careful, adding 14C forcings
            for cat in db.observations.columns:#self.rcf.get(f'emissions.{tr}.categories'):
                if tr in cat:
                    self.db.observations.loc[:, cat] = db.observations.loc[:, cat].values

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
            if step in self.rcf.get('transport.output.steps'):
                self.save(tag=step, structf=emf)

        # Return model-data mismatches
        return self.db.observations.loc[:, ('mismatch', 'err')]
    
    def runAdjoint(self, departures, atmdel=None):
        """
        Prepare input for the adjoint run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly
        """
        
        rundir = self.rcf.get('path.run')
        tmpdir = self.rcf.get('path.temp')
        executable = self.rcf.get("model.transport.exec")
        #fields = self.rcf.get('model.adjoint.obsfields')

        self.db.observations.loc[:, 'dy'] = departures

        dpf = self.db.save_tar(os.path.join(tmpdir, 'departures.tar.gz'))

        # Create an adjoint rc-file
        rcadj = self.rcf.write(os.path.join(rundir, 'adjoint.rc'))

        # Name of the adjoint output file
        adjf = os.path.join(tmpdir, 'adjoint.nc')

        if atmdel is not None:
            # Name of the atmospheric delta file
            atmdel = os.path.join(tmpdir, 'atmosDelta.nc')

            # Run the adjoint transport:
            cmd = [sys.executable, executable, '--adjoint', '--db', dpf, '--rc', rcadj, '--emis', adjf, '--atmdel', atmdel]#, '--serial']#, '--checkfile', checkf, '--serial']
        else:
            cmd = [sys.executable, executable, '--adjoint', '--db', dpf, '--rc', rcadj, '--emis', adjf]#, '--atmdel', atmdel]#, '--serial']#, '--checkfile', checkf, '--serial']
        logger.info(colorize(' '.join([x for x in cmd]), 'g'))
        try :
            subprocess.run(cmd, close_fds=True)
        except subprocess.CalledProcessError :
            logger.error("Adjoint run failed, exiting ...")
            raise subprocess.CalledProcessError

        # Collect the results :
        return self.readStruct(tmpdir, prefix='adjoint')

    def calcSensitivityMap(self):
        
        departures = ones(self.db.observations.shape[0])
        adjfield = self.runAdjoint(departures, self.atmdf)
        return array([adjfield[tr][cat]['emis'].sum(0) for tr in adjfield.keys() for cat in adjfield[tr].keys()]).sum(0)
