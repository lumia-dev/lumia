#!/usr/bin/env python
import sys
import os
import shutil
import subprocess
import logging
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

        self.rcf.write(os.path.join(path, 'transport.%src'%tag))
        self.db.save_tar(os.path.join(path, 'observations.%star.gz'%tag))
        if structf is not None :
            try :
                shutil.copy(structf, path)
            except shutil.SameFileError :
                pass

    def runForward(self, struct, step=None):
        """
        Prepare input data for a forward run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly.        
        """
        # #if struct is None : struct = self.controlstruct
        # self.check_init()

        # read model-specific info
        rundir = self.rcf.get('path.run')
        executable = self.rcf.get("model.transport.exec")
        
        # Write model inputs:
        emf = self.writeStruct(struct, rundir, 'modelData.%s'%step)
        dbf = self.db.save_tar(os.path.join(rundir, 'observations.%s.tar.gz'%step))
        rcf = self.rcf.write(os.path.join(rundir, f'forward.{step}.rc'))
        #checkf = os.path.join(tempfile.mkdtemp(dir=rundir), 'forward.ok')
        
        # Run the model
        cmd = [sys.executable, executable, '--rc', rcf, '--forward', '--db', dbf, '--emis', emf]#, '--serial']#, '--checkfile', checkf, '--serial']
        logger.info(colorize(' '.join([x for x in cmd]), 'g'))
        try :
            subprocess.run(cmd, close_fds=True)
        except subprocess.CalledProcessError :
            logger.error("Forward run failed, exiting ...")
            raise subprocess.CalledProcessError

        # Retrieve results :
        db = obsdb(filename=dbf)
        for cat in self.rcf.get('emissions.categories'):
            self.db.observations.loc[:, f'mix_{cat}'] = db.observations.loc[:, f'mix_{cat}']
        self.db.observations.loc[:, f'mix_{step}'] = db.observations.loc[:, 'mix']
        self.db.observations.loc[:, 'mix_background'] = db.observations.loc[:, 'mix_background']
        self.db.observations.loc[:, 'mix_foreground'] = db.observations.loc[:, 'mix']-db.observations.loc[:, 'mix_background']
        self.db.observations.loc[:, 'mismatch'] = db.observations.loc[:, 'mix']-self.db.observations.loc[:,'obs']

        self.db.observations.dropna(subset=['mismatch'], inplace=True)

        # Output if needed:
        if self.rcf.get('transport.output'):
            if step in self.rcf.get('transport.output.steps'):
                self.save(tag=step, structf=emf)

        # Return model-data mismatches
        return self.db.observations.loc[:, ('mismatch', 'err')]
    
    def runAdjoint(self, departures):
        """
        Prepare input for the adjoint run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly
        """
        
        rundir = self.rcf.get('path.run')
        executable = self.rcf.get("model.transport.exec")
        #fields = self.rcf.get('model.adjoint.obsfields')

        self.db.observations.loc[:, 'dy'] = departures
        dpf = self.db.save_tar(os.path.join(rundir, 'departures.tar.gz'))
        
        # Create an adjoint rc-file
        rcadj = self.rcf.write(os.path.join(rundir, 'adjoint.rc'))

        # Name of the adjoint output file
        adjf = os.path.join(rundir, 'adjoint.nc')

        # Run the adjoint transport:
        cmd = [sys.executable, executable, '--adjoint', '--db', dpf, '--rc', rcadj, '--emis', adjf]#, '--serial']#, '--checkfile', checkf, '--serial']
        logger.info(colorize(' '.join([x for x in cmd]), 'g'))
        try :
            subprocess.run(cmd, close_fds=True)
        except subprocess.CalledProcessError :
            logger.error("Adjoint run failed, exiting ...")
            raise subprocess.CalledProcessError

        # Collect the results :
        return self.readStruct(rundir, 'adjoint')
