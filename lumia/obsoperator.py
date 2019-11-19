#!/usr/bin/env python
import os
import subprocess
from .obsdb import obsdb
import inspect
from lumia.Tools import checkDir

class transport(object):
    name = 'lagrange'
    def __init__(self, rcf, interface=None, obs=None, formatter=None, controlstruct=None):
        self.rcf = rcf
        # Initialize the obs if needed
        if obs is not None : self.setupObs(obs)
            
        # Initialize the interfaces if needed
        if interface is not None : self.setupInterface(interface)

        # In a strictly forward run, we don't actually need the whole interfaces
        # and we can just pass the control structure and the write function instead
        if formatter is not None :
            self.writeStruct = formatter

        if controlstruct is not None :
            self.controlstruct = controlstruct

    def setupObs(self, obsdb):
        self.db = obsdb

    def setupInterface(self, interface):
        # First, check if "interfaces" is instantiated or not
        if inspect.isclass(interface):
            interface = interface(self.rcf)
        self.interface = interface
        self.writeStruct = interface.writeStruct
        self.readStruct = interface.readStruct
            
    def setupControl(self, struct, info=False):
        self.interface.setup(struct, info=info)
        
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

    def runForward(self, struct=None, step=None):
        """
        Prepare input data for a forward run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly.        
        """
        if struct is None : struct = self.controlstruct

        # read model-specific info
        rundir = self.rcf.get('path.run')
        executable = self.rcf.get("model.transport.exec")
        
        # Write model inputs:
        emf = self.writeStruct(struct, rundir, 'modelData.%s'%step)
        dbf = self.db.save(os.path.join(rundir, 'observations.%s.hdf'%step))
        
        # Run the model
        pid = subprocess.Popen(['python', executable, '--forward', '--db', dbf, '--emis', emf], close_fds=True)
        pid.wait()
        
        # Retrieve results :
        db = obsdb(filename=dbf)
        self.db.observations.loc[:, 'foreground'] = db.observations.loc[:, 'foreground']
        self.db.observations.loc[:, 'totals'] = db.observations.loc[:, 'totals']
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
        return self.readStruct(adjf)
        
        ## Convert from adjoint structure to adjoint vector :
        #return self.VecToStruct_adj(adj)