#!/usr/bin/env python

from netCDF4 import Dataset
import subprocess
import os
from distutils.file_util import copy_file
import shutil
from omegaconf import DictConfig
from loguru import logger
from numpy.typing import NDArray


class Minimizer:
    def __init__(self, dconf: DictConfig, filename: str = None):
        self.dconf = dconf
        if filename is None:
            filename = self.dconf.communication_file
        self.commfile = CommFile(filename, dconf)
        self.iter = 0
        self.read_eigsys = self.commfile.read_eigsys
        self.readState = self.commfile.readState

        # Compatiblity with the newer code:
        self._status: int = -1

    @property
    def status(self) -> int:
        return self._status

    @property
    def converged(self) -> bool:
        return self._status == 2

    @property
    def finished(self) -> bool:
        return self._status > 0

    @property
    def initialized(self) -> bool:
        return self.status != -1

    def init(self, state_preco: NDArray) -> None:
        self.commfile.create_file(len(state_preco))
        self.commfile.write_state(state_preco)

    def calc_update(self, state_preco, gradient_preco, J_tot) -> NDArray:
        if not self.initialized:
            self.init(state_preco)
        self.commfile.update(gradient_preco, J_tot)
        self.run_minimizer()
        self._status = self.commfile.checkUpdate()
        self.iter += 1
        return self.readState()

    def run_minimizer(self):
        exec_name = self.dconf.executable
        cmd = [exec_name, '--write-traject', '--state-file', self.commfile.filepath]
        logger.info(' '.join([str(_) for _ in cmd]))
        subprocess.check_call(cmd)

    def update_gradient(self, gradient, J_tot):
        self.commfile.update(gradient, J_tot)

    def save(self, path: str) -> None:
        copy_file(self.commfile.filepath, os.path.join(path, 'congrad.nc'))
        copy_file(os.path.join(os.path.dirname(self.commfile.filepath), 'congrad_debug.out'), path)

    def iter_states(self):
        traject = self.commfile.read_traject()
        for istate in range(traject.shape[1]):
            yield traject[:, istate]


class CommFile(object):
    def __init__(self, filename: str, dconf: DictConfig):
        self.filepath = filename
        self.dconf = dconf
        self.initialized = False
        self.len_x = 0
        self.len_g = 0
        self.debug = False

    def resume(self, nstate, trim=3):
        """ This resumes the state of the CommFile object as it would be just before the call
        to "checkUpdate". Optionally, it can remove some iterations, if the "trim" argument is 
        set to a value > 0. If both the state vector and state vector gradient (g_c and x_c) have
        the same number of iterations in the netCDF file itself, then the last gradient is erased
        (this would happen if the inversion crashed during the call to the minimizer itself)"""
        self.trim(trim)
        with Dataset(self.filepath, 'r') as ds:
            self.len_x = len(ds.dimensions['dim_x']) - 1
            self.len_g = len(ds.dimensions['dim_g'])
        self.initialized = True

    def trim(self, trim):
        with Dataset(self.filepath, 'r') as ds:
            ng = len(ds.dimensions['dim_g'])
            nstate = len(ds.dimensions['n_state'])
            ngnew = ng - trim
            nxnew = ng - trim + 1
            with Dataset(f'{self.filepath}_2', 'w') as dsw:
                # copy the attributes
                dsw.congrad_finished = int(0)
                dsw.iter_max = ds.iter_max
                dsw.iter_convergence = ds.iter_convergence
                dsw.preduc = ds.preduc
                dsw.J_tot = ds.J_tot

                # copy the dimensions
                dsw.createDimension('n_state', nstate)
                dsw.createDimension('dim_x', 0)
                dsw.createDimension('dim_g', 0)

                # copy the variables
                dsw.createVariable('x_c', 'd', ('n_state', 'dim_x'))
                dsw['x_c'][:] = ds['x_c'][:, :nxnew]
                dsw.createVariable('g_c', 'd', ('n_state', 'dim_g'))
                dsw['g_c'][:] = ds['g_c'][:, :ngnew]
        shutil.move(f'{self.filepath}_2', self.filepath)

    def create_file(self, nstate):
        logger.info(self.filepath)
        if not os.path.exists(os.path.dirname(self.filepath)):
            os.makedirs(os.path.dirname(self.filepath))
        fid = Dataset(self.filepath, 'w')
        fid.createDimension('n_state', nstate)
        fid.createDimension('dim_x', 0)
        fid.createDimension('dim_g', 0)
        fid.iter_max = self.dconf.max_number_of_iterations
        fid.iter_convergence = self.dconf.number_of_iterations
        fid.preduc = 1. / self.dconf.gradient_norm_reduction
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
            logger.info("Cost function updated to %.2e" % J_tot)
            ds.J_tot = float(J_tot)
        self.len_g += 1

    def readState(self):
        with Dataset(self.filepath, 'r') as ds:
            return (ds['x_c'][:, self.len_x - 1]).data

    def checkUpdate(self):
        with Dataset(self.filepath, 'r') as ds:
            len_x = len(ds.dimensions['dim_x'])
            len_g = len(ds.dimensions['dim_g'])
            if len_x != self.len_x + 1 and not ds.congrad_finished:
                raise RuntimeError(
                    'The number of state vectors should have increased from %i to %i, instead it is %i' % (
                        self.len_x, self.len_x + 1, len_x
                    )
                )
            if len_g != self.len_g and not ds.congrad_finished:
                raise RuntimeError(
                    f'The number of gradients should have remained unchanged at {self.len_g}, instead it is {len_g}')
            else:
                self.len_x = len_x
            status = ds.congrad_finished
        return status

    def read_eigsys(self):
        with Dataset(self.filepath, 'r') as ds:
            eigvals = ds['eigenvalues'][:].data
            eigvecs = ds['eigenvectors'][:].data
        return eigvals, eigvecs

    def read_traject(self):
        with Dataset(self.filepath, 'r') as ds:
            if 'xc_traject' in ds.variables:
                traject = ds['xc_traject'][:]
            else:
                raise RuntimeError(
                    "No state trajectory found. Either the inversion did not converge yet, or the computing of intermediate trajectories was not requested (see rcfile)"
                )
        return traject
