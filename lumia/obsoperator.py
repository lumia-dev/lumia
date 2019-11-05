#!/usr/bin/env python
import os
from netCDF4 import Dataset
import subprocess
from .obsdb import obsdb
import shutil

class transport(object):
    def __init__(self, rcf, observations, interface):
        self.rcf = rcf
        self.writeStruct = interface.mod2file
        self.readEmis = interface.file2mod
        self.readAdjoint = interface.file2mod
        self.stateToStruct = interface.stateToStruct
        self.setupObs(observations)
        
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
        
    def setupObs(self, obsdb):
        self.db = obsdb
        
    def setupControl(self, struct, info=None):
        self.control = struct
        self.showInfo(info)
        
    def runForward(self, step, controlvec=None):
        """
        Prepare input data for a forward run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly.        
        """
        if controlvec is not None :
            struct, info = self.stateToStruct(controlvec)
            self.setupControl(struct, info)
        
        rundir = self.rcf.get('path.run')
        executable = self.rcf.get("model.transport.exec")
        
        # Write emissions:
        emf = self.writeStruct(rundir, 'emissions.%s.nc4'%step))
        
        # Write observations:
        dbf = self.db.save(os.path.join(rundir, 'observations.%s.hdf'%step))
        
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
        return self.stateToStruct_adj(adj)