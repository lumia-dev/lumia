#!/usr/bin/env python
import os
import subprocess
from .obsdb import obsdb
import inspect
from lumia.Tools import checkDir, rctools
import logging

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

    def save(self, path=None, tag=None):
        """
        This copies the last model I/O to "path", with an optional tag to identify it
        """
        tag = '' if tag is None else tag+'.'
        if path is None :
            path = self.rcf.get('path.output')
        checkDir(path)

        self.rcf.write(os.path.join(path, 'transport.%src'%tag))
        self.db.save(os.path.join(path, 'observations.%shdf'%tag))
#        self.writeStruct(self.struct, path, 'transport_control.%s')

    def runForward(self, struct, step=None):
        """
        Prepare input data for a forward run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly.        
        """
        #if struct is None : struct = self.controlstruct

        # read model-specific info
        rundir = self.rcf.get('path.run')
        executable = self.rcf.get("model.transport.exec")
        
        # Write model inputs:
        emf = self.writeStruct(struct, rundir, 'modelData.%s'%step)
        dbf = self.db.save(os.path.join(rundir, 'observations.%s.hdf'%step))
        
        # Run the model
        cmd = ['python', executable, '--forward', '--db', dbf, '--emis', emf]
        logger.info(' '.join([x for x in cmd]))
        pid = subprocess.Popen(['python', executable, '--forward', '--db', dbf, '--emis', emf], close_fds=True)
        pid.wait()

        # Retrieve results :
        db = obsdb(filename=dbf)
        self.db.observations.loc[:, 'foreground'] = db.observations.loc[:, 'foreground']
        self.db.observations.loc[:, 'model'] = db.observations.loc[:, 'model']
        self.db.observations.loc[:, 'mismatch'] = \
            self.db.observations.loc[:,'background'] + \
            self.db.observations.loc[:,'foreground'] - \
            self.db.observations.loc[:,'obs']
        self.db.observations.loc[:, step] = self.db.observations.loc[:, 'background']+self.db.observations.loc[:, 'foreground']
        
        # Return model-data mismatches
        return self.db.observations.loc[:, ('mismatch', 'err')]
    
    
    def runAdjoint(self, departures):
        """
        Prepare input for the adjoint run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly
        """
        
        rundir = self.rcf.get('path.run')
        executable = self.rcf.get("model.transport.exec")
        fields = self.rcf.get('model.adjoint.obsfields')

        self.db.observations.loc[:, 'dy'] = departures
        dpf = self.db.save(os.path.join(rundir, 'departures.hdf'))
        
        # Create an adjoint rc-file
        rcadj = self.rcf.write(os.path.join(rundir, 'adjoint.rc'))

        # Name of the adjoint output file
        adjf = os.path.join(rundir, 'adjoint.nc')

        # Run the adjoint transport:
        cmd = ['python', executable, '--adjoint', '--db', dpf, '--rc', rcadj, '--emis', adjf]
        logger.info(' '.join([x for x in cmd]))
        pid = subprocess.Popen(cmd, close_fds=True)
        pid.wait()

        # Collect the results :
        return self.readStruct(rundir, 'adjoint')
