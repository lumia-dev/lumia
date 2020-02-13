#!/usr/bin/env python
import os
import shutil
import subprocess
from .obsdb import obsdb
import inspect
from lumia.Tools import checkDir, colorize
import logging
import tempfile

logger = logging.getLogger(__name__)

class transport(object):
    name = 'lagrange'
    def __init__(self, rcf, obs=None, formatter=None):
        self.rcf = rcf
        # Initialize the obs if needed
        if obs is not None : self.setupObs(obs)
            
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
        tag = tag.strip('.')
        tag = '' if tag is None else tag+'.'
        if path is None :
            path = self.rcf.get('path.output')
        checkDir(path)

        self.rcf.write(os.path.join(path, 'transport.%src'%tag))
        self.db.save_tar(os.path.join(path, 'observations.%star.gz'%tag))
        shutil.copy(structf, path)

    def runForward(self, struct, step=None):
        """
        Prepare input data for a forward run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly.        
        """
        #if struct is None : struct = self.controlstruct
        self.check_init()

        # read model-specific info
        rundir = self.rcf.get('path.run')
        executable = self.rcf.get("model.transport.exec")
        
        # Write model inputs:
        emf = self.writeStruct(struct, rundir, 'modelData.%s'%step)
        dbf = self.db.save_tar(os.path.join(rundir, 'observations.%s.tar.gz'%step))
        rcf = self.rcf.write(os.path.join(rundir, f'forward.{step}.rc'))
        checkf = os.path.join(tempfile.mkdtemp(dir=rundir), 'forward.ok')
        
        # Run the model
        cmd = ['python', executable, '--rc', rcf, '--forward', '--db', dbf, '--emis', emf, '--checkfile', checkf]
        logger.info(colorize(' '.join([x for x in cmd]), 'g'))
        pid = subprocess.Popen(cmd, close_fds=True)
        pid.wait()

        # Check that the run was successful:
        self.check_success(checkf, "Forward run failed, exiting ...")

        # Retrieve results :
        db = obsdb(filename=dbf)
        self.db.observations.loc[:, 'foreground'] = db.observations.loc[:, 'foreground']
        self.db.observations.loc[:, 'model'] = db.observations.loc[:, 'model']
        self.db.observations.loc[:, 'mismatch'] = \
            self.db.observations.loc[:,'background'] + \
            self.db.observations.loc[:,'foreground'] - \
            self.db.observations.loc[:,'obs']
        self.db.observations.loc[:, step] = self.db.observations.loc[:, 'background']+self.db.observations.loc[:, 'foreground']

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

        # Create temporary file
        checkf = os.path.join(tempfile.mkdtemp(dir=rundir), 'adjoint.ok')

        # Run the adjoint transport:
        cmd = ['python', executable, '--adjoint', '--db', dpf, '--rc', rcadj, '--emis', adjf, '--checkfile', checkf]
        logger.info(colorize(' '.join([x for x in cmd]), 'g'))
        pid = subprocess.Popen(cmd, close_fds=True)
        pid.wait()

        self.check_success(checkf, 'Adjoint run failed, exiting ...')

        # Collect the results :
        return self.readStruct(rundir, 'adjoint')

    def check_success(self, checkf, msg):

        # Check that the run was successful
        if os.path.exists(checkf) :
            shutil.rmtree(os.path.dirname(checkf))
        else :
            logger.error(msg)
            raise

    def check_init(self):
        """
        Initial checks to avoid performing computations if some critical input is missing:
        - exits if no footprint file is present
        """
        if self.db.observations.footprint.count() == 0 :
            logger.critical("No valid footprint files in the database. Aborting ...")
            raise
