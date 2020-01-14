#!/usr/bin/env python
from netCDF4 import Dataset
import subprocess, os
import shutil
import logging
from lumia.Tools.logging_tools import colorize

logger = logging.getLogger(__name__)

class Minimizer:
    def __init__(self, rcf, nstate=None):
        self.rcf = rcf
        self.nstate = nstate
        self.commfile = CommFile(self.rcf.get('var4d.communication.file'), self.rcf)
        self.file_initialized = True
        if nstate is not None :
            self.init(nstate)
        self.iter = 0
        self.converged = False
        self.finished = False
        self.read_eigsys = self.commfile.read_eigsys
        self.readState = self.commfile.readState

    def reset(self):
        self.init(self.nstate)
        self.iter = 0
        self.converged = False
        self.finished = False

    def init(self, nstate):
        self.nstate = nstate
        self.commfile.createFile(nstate)

    def calc_update(self, state_preco, gradient_preco, J_tot):
        if self.iter == 0 :
            self.commfile.write_state(state_preco)
        self.commfile.update(gradient_preco, J_tot)
        self.runMinimizer()
        status = self.commfile.checkUpdate()
        if status > 0 : self.finished = True
        if status == 2 : self.converged = True
        self.iter += 1
        return status

    def runMinimizer(self):
        exec_name = self.rcf.get('var4d.conGrad.exec', default='/home/lumia/var4d/bin/congrad.exe')
        cmd = [exec_name, '--write-traject', '--state-file', self.commfile.filepath]
        logger.info(colorize(' '.join([*cmd]), 'g'))
        subprocess.check_call(cmd)

    def update(self, gradient, J_tot):
        self.commfile.update(gradient, J_tot)
        
    def save(self, filename):
        if filename != self.commfile.filepath :
            shutil.copy(self.commfile.filepath, filename)

    def iter_states(self):
        traject = self.commfile.read_traject()
        for istate in range(traject.shape[1]):
            yield traject[:, istate]


class CommFile(object):
    def __init__(self, filename, rcf):
        self.filepath = filename
        self.rcf = rcf
        self.initialized = False
        self.len_x = 0
        self.len_g = 0
        self.debug = False

    def createFile(self, nstate):
        logger.info(self.filepath)
        if not os.path.exists(os.path.dirname(self.filepath)):
            os.makedirs(os.path.dirname(self.filepath))
        fid = Dataset(self.filepath, 'w')
        fid.createDimension('n_state', nstate)
        fid.createDimension('dim_x', 0)
        fid.createDimension('dim_g', 0)
        fid.iter_max = self.rcf.get('var4d.max_iter', default=1000)
        fid.iter_convergence = self.rcf.get('var4d.fixed_iterations', default=1000)
        fid.preduc = 1./self.rcf.get('var4d.gradient.norm.reduction', default=1.e12)
        fid.congrad_finished = 0
        fid.J_tot = 0
        fid.createVariable('x_c', 'd', ('n_state', 'dim_x'))
        fid.createVariable('g_c', 'd', ('n_state', 'dim_g'))
        fid.close()
        self.len_g = 0
        self.len_x = 0
        self.initialized = True

    def write_state(self, state_preco):
        with Dataset(self.filepath, 'a') as ds:
            ds['x_c'][:, self.len_x] = state_preco
        self.len_x += 1

    def update(self, gradient_preco, J_tot):
        with Dataset(self.filepath, 'a') as ds:
            ds['g_c'][:, self.len_g] = gradient_preco
            logger.info("Cost function updated to %.2e"%J_tot)
            ds.J_tot = float(J_tot)
        self.len_g += 1

    def readState(self):
        with Dataset(self.filepath, 'r') as ds:
            state = ds['x_c'][:, self.len_x-1]
        return state

    def checkUpdate(self):
        with Dataset(self.filepath, 'r') as ds:
            len_x = len(ds.dimensions['dim_x'])
            len_g = len(ds.dimensions['dim_g'])
            if len_x != self.len_x + 1 and not ds.congrad_finished :
                raise RuntimeError(
                    'The number of state vectors should have increased from %i to %i, instead it is %i'%(
                        self.len_x, self.len_x+1, len_x
                    )
                )
            if len_g != self.len_g and not ds.congrad_finished :
                raise RuntimeError(
                    'The number of gradients should have remained unchanged at %i, instead it is %i'%(
                    self.len_g, len_g
                    )
                )
            else :
                self.len_x = len_x
            status = ds.congrad_finished
        return status

    def read_eigsys(self):
        with Dataset(self.filepath, 'r') as ds:
            eigvals = ds['eigenvalues'][:]
            eigvecs = ds['eigenvectors'][:]
        return eigvals, eigvecs
    
    def read_traject(self):
        with Dataset(self.filepath, 'r') as ds :
            if 'xc_traject' in ds.variables :
                traject = ds['xc_traject'][:]
            else :
                raise RuntimeError(
                    "No state trajectory found. Either the inversion did not converge yet, or the computing of intermediate trajectories was not requested (see rcfile)"
                )
        return traject
