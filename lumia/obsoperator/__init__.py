#!/usr/bin/env python
import sys
import os
import shutil
#import subprocess
#import logging
from numpy import ones, array
from lumia.Tools import checkDir, colorize
from lumia.obsdb import obsdb
from lumia.Tools.system_tools import runcmd
from loguru import logger

#logger = logging.getLogger(__name__)


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

    def calcDepartures(self, struct, step=None, serial=False):
        dbf = self.runForward(struct, step, serial)
        db = obsdb(filename=dbf)
        if self.rcf.get('model.split.categories', default=True):
            for cat in self.rcf.get('emissions.categories'):
                self.db.observations.loc[:, f'mix_{cat}'] = db.observations.loc[:, f'mix_{cat}'].values
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
        # #if struct is None : struct = self.controlstruct
        # self.check_init()

        # read model-specific info
        rundir = self.rcf.get('path.output')
        executable = self.rcf.get("model.transport.exec")
        if self.rcf.get("model.transport.serial", default=False) :
            serial = True
        
        # Write model inputs:
        emf = self.writeStruct(struct, rundir, 'modelData.%s'%step)
        dbf = self.db.save_tar(os.path.join(rundir, 'observations.%s.tar.gz'%step))
        rcf = self.rcf.write(os.path.join(rundir, f'forward.{step}.rc'))
        
        # Run the model
        cmd = [sys.executable, '-u', executable, '--rc', rcf, '--forward', '--db', dbf, '--emis', emf]#, '--serial']#, '--checkfile', checkf, '--serial']
        if serial :
            cmd.append('--serial')
        cmd.extend(self.rcf.get('model.transport.extra_arguments', default='').split(','))
        runcmd(cmd)

        # Retrieve results :
        return dbf

        # if self.rcf.get('model.split.categories', default=True):
        #     for cat in self.rcf.get('emissions.categories'):
        #         self.db.observations.loc[:, f'mix_{cat}'] = db.observations.loc[:, f'mix_{cat}'].values
        # self.db.observations.loc[:, f'mix_{step}'] = db.observations.mix.values
        # self.db.observations.loc[:, 'mix_background'] = db.observations.mix_background.values
        # self.db.observations.loc[:, 'mix_foreground'] = db.observations.mix.values-db.observations.mix_background.values
        # self.db.observations.loc[:, 'mismatch'] = db.observations.mix.values-self.db.observations.loc[:,'obs']
        #
        # # Optional: store extra columns that the transport model may have written (to pass them again to the transport model in the following steps)
        # for key in self.rcf.get('model.obs.extra_keys', default=[], tolist=True) :
        #     self.db.observations.loc[:, key] = db.observations.loc[:, key].values
        #
        # self.db.observations.dropna(subset=['mismatch'], inplace=True)
        #
        # # Output if needed:
        # if self.rcf.get('transport.output'):
        #     if step in self.rcf.get('transport.output.steps'):
        #         self.save(tag=step, structf=emf)
        #
        # # Return model-data mismatches
        # return self.db.observations.loc[:, ('mismatch', 'err')]
    
    def runAdjoint(self, departures):
        """
        Prepare input for the adjoint run, launch the actual transport model in a subprocess and retrieve the results
        The eventual parallelization is handled by the subprocess directly
        """
        
        outputdir = self.rcf.get('path.output')
        executable = self.rcf.get("model.transport.exec")
        #fields = self.rcf.get('model.adjoint.obsfields')

        self.db.observations.loc[:, 'dy'] = departures
        dpf = self.db.save_tar(os.path.join(outputdir, 'departures.tar.gz'))
        
        # Create an adjoint rc-file
        rcadj = self.rcf.write(os.path.join(outputdir, 'adjoint.rc'))

        # Name of the adjoint output file
        adjf = os.path.join(outputdir, 'adjoint.nc')

        # Run the adjoint transport:
        cmd = [sys.executable, '-u', executable, '--adjoint', '--db', dpf, '--rc', rcadj, '--emis', adjf]#, '--serial']#, '--checkfile', checkf, '--serial']
        cmd.extend(self.rcf.get('model.transport.extra_arguments', default='').split(','))
        runcmd(cmd)

        # Collect the results :
        return self.readStruct(outputdir, 'adjoint')

    def calcSensitivityMap(self):
        departures = ones(self.db.observations.shape[0])
        try :
            tmpdir = self.rcf.get('path.temp')
            adjfield = self.readStruct(tmpdir, 'adjoint')
        except :
            adjfield = self.runAdjoint(departures)
        return array([adjfield[cat]['emis'].sum(0) for cat in adjfield.keys()]).sum(0)
