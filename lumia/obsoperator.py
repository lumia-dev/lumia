#!/usr/bin/env python
import os
from netCDF4 import Dataset
import subprocess
from .obsdb import obsdb
import shutil
import inspect

class transport(object):
    def __init__(self, rcf, interface=None, obs=None):
        self.rcf = rcf
        # Initialize the obs if needed
        if obs is not None : self.setupObs(obs)
            
        # Initialize the interface if needed
        if interface is not None : self.setupInterface(interface)
#        self.writeStruct = interface.mod2file
#        self.readEmis = interface.file2mod
#        self.readAdjoint = interface.file2mod
#        self.stateToStruct = interface.stateToStruct
        
    def setupObs(self, obsdb):
        self.db = obsdb
        
    def setupInterface(self, interface):
        # First, check if "interface" is instantiated or not
        if insect.isclass(interface):
            interface = interface(self.rcf)
            
    def setupControl(self, struct, info=False):
        self.interface.setup(struct, info=info)
        
    def save(self, path=None, tag=None):
        """
        This copies the last model I/O to "path", with an optional tag to identify it
        """
        tag = '' if step is None else tag+'.'
        if path is None :
            path = self.rcf.get('path.output')
        self.interface.archive(self.interface.emfile, path, tag)
        self.rcf.write(os.path.join(path, 'transport.%src'%tag))
        self.db.save(os.path.join(path, 'observations.%shdf'%tag))
        
        
    def runForward(self, step, controlvec=None, struct=None):
        """
        Prepare input data for a forward run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly.        
        """
        
        # Make sure that we have some data to run with #TODO: replace this by an assert?
        if controlvec is not None :
            struct, info = self.VecToStruct(controlvec)
            
        # Setup the data in the interface
        # self.interface.setup(struct, info)
        
        # read model-specific info
        rundir = self.rcf.get('path.run')
        executable = self.rcf.get("model.transport.exec")
        
        # Write model inputs:
        emf = self.interface.writeStruct(struct, rundir, 'modelData.%s'%step)
        dbf = self.db.save(os.path.join(rundir, 'observations.%s.hdf'%step))
        
        # Run the model
        pid = subprocess.Popen(['python', executable, '--forward', '--db', dbf, '--emis', emf], close_fds=True)
        pid.wait()
        
        # Retrieve results :
        db = obsdb(filename=dbf)
        self.db.observations.loc[:, 'foreground'] = db.loc[:, 'foreground']
        self.db.observations.loc[:, 'totals'] = db.loc[:, 'totals']
        self.db.observations.loc[:, 'model'] = db.loc[:, 'model']
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
        
        # Write the departures file:
        dpf = os.path.join(rundir, 'departures.json')
        dp = self.db.observations.loc[:, fields]
        dp.loc[:, 'dy'] = departures
        dp.to_json(dpf)
        
        # Create the adjoint filename:
        adjf = os.path.join(rundir, 'adjoint.hdf')
        
        # Run the adjoint transport:
        pid = subprocess.Popen(['python', executable, '--adjoint', '--db', dpf, '--emis', adjf], close_fds=True)
        pid.wait()
        
        # Collect the results :
        adj = self.readAdjoint(adjf)
        
        # Convert from adjoint structure to adjoint vector :
        return self.VecToStruct_adj(adj)