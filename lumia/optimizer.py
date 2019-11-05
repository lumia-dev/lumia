#!/usr/bin/env python
from .Tools import costFunction
from numpy import *
import os

class Optimizer(object):
    def __init__(self, rcf, control, obsop, minimizer, interface):
        self.rcf = rcf                   # Settings
        self.obsop = obsop               # Model interface instance (initialized!)
        self.control = control           # modelData structure, containing the optimization data
        self.minimizer = minimizer       # minimizer object instance (initiated!)
        self.interface = obsop.interface # interface between model and optimization space
        
    def Var4D(self, label='apos'):
        self.minimizer.reset()     # Just to make sure ...
        
        state_preco = zeros(self.control.size)
        
        step = 'apri'
        status = 0
        while status == 0 :
            state_preco, status = self._Var4D_step(state_preco, step)
            step = 'var4d'
            
        # Finishup ...
        dy, err = self._computeDepartures(state_preco, label)
        self.J = self._computeCostFunction(state_preco, dy, err)
        gradient_preco = self.ComputeGradient(state_preco, dy, err)
        self.minimizer.update(gradient_preco, self.J.tot)
        self._calcPosteriorUncertainties()
        self.save(label)
        
    def _Var4D_step(self, state_preco, step='apri'):
        dy, err = self._computeDepartures(state_preco, step)
        self.J = self._computeCostFunction(state_preco, dy, err)
        gradient_preco = self.ComputeGradient(state_preco, dy, err)
        status = self.minimizer.calc_update(state_preco, gradient_preco, self.J.tot)
        state_preco = self.minimizer.readState()
        return state_preco, status
    
    def _computeDepartures(self, state_preco, step):
        state = self.control.xc_to_x(state_preco)
        departures = self.model.runForward(step, controlvec=state)
        dy = departures.loc[:, 'mismatch']
        dye = departures.loc[:, 'err']
        return dy, dye
    
    def _computeCostFunction(self, state_preco, dy, dye):
        dstate = state_preco-self.control.get('state_prior_preco')   # TODO: check if state_prior_preco is ever non-zero
        J_bg = 0.5*dot(dstate, dstate)
        J_obs = 0.5*dot(dy/err, dy/err)
        J = costFunction(bg=J_bg, obs=J_obs)
        return J
    
    def _ComputeGradient(self, state_preco, dy, dye):
        adjoint_state = self.obsop.runAdjoint(dy/err**2)
        gradient_obs_preco = self.control.g_to_gc(adjoint_state)
        state_departures = state_preco-self.control.get('state_prior_preco')
        gradient_preco = gradient_obs_preco + state_departures
        return gradient_preco.values  # TODO: ideally, the ".values" should not be needed ...
    
    def _calcPosteriorUncertainties(self, store_eigenvec=False):
        converged_eigvals, converged_eigvecs = self.minimizer.read_eigsys()
        LE = zeros_like(converged_eigvecs)
        for ii in range(len(converged_eigvals)):
            # If use_eigen_file is true, for each eigenvector, check first whether the file eigenvector_modelspace_%03i.nn
            # exists or not. If it does, read the eigenvector in model space from that file. Else, calculate the
            # eigenvector in model space and store it in such a file.
            LE[:,ii] = self.control.xc_to_x(converged_eigvecs[:,ii], add_prior=False)
            if store_eigenvec:
                self.control.set(LE[:, ii], 'eigenvec_%i'%ii)
            
            Mat2 = 1./converged_eigvals - 1.
            dapri = self.control.get('prior_uncertainty')
            dapos = nan_to_num(sqrt(dapri**2 + inner(LE**2, Mat2)))
            self.control.set(dapos, 'posterior_uncertainty')
            
    def save(self, step=None):
        step = '' if step is None else step+'.'
        path = self.rcf.get('path.output')
        if not os.path.exists(path): os.makedirs(path)
        self.rcf.write(os.path.join(path, 'lumia.%src'%step))
        self.obsop.save(path, step)
        self.control.write(os.path.join(path, 'control.%shdf'%step))
        self.minimizer.save(os.path.join(path, 'comm_file.%snc4'%step))