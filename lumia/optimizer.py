#!/usr/bin/env python
import pdb
import os
import logging
from loguru import logger
from numpy import zeros, zeros_like, sqrt, inner, nan_to_num, dot, random, ones
from lumia.minimizers.congrad import Minimizer as congrad
from .Tools import costFunction
from archive import Archive

# logger = logging.getLogger(__name__)


class Optimizer(object):
    def __init__(self, rcf, model, interface, atmdel=None, minimizer=congrad):
        self.rcf = rcf                        # Settings
        self.model = model                    # Model interfaces instance (initialized!)
        self.control = interface.data         # modelData structure, containing the optimization data
        self.minimizer = minimizer(self.rcf)  # minimizer object instance (initiated!)
        self.interface = interface
        self.iteration = 0
        self.atmdel = atmdel

    def GradientTest(self):

        self.minimizer.reset()

        # 1) Compute the gradient and cost function for a (random) control vector:
        state_preco1 = random.randn(self.control.size)
        dy1, err1 = self._computeDepartures(state_preco1, 'state1')
        self.J = self._computeCostFunction(state_preco1, dy1, err1)
        J0 = self.J.tot
        gradient_preco = self._ComputeGradient(state_preco1, dy1, err1)

        # 2) Compute the cost function for the same state + a small random perturbation
        dx = random.randn(self.control.size)
        alpha = 0.01

        with open(os.path.join(self.rcf.get('path.output'), 'gradient_test.log'), 'w') as fid :
            fid.write(f' alpha ;                DJ1 ;                DJ2 ;      DJ1/DJ2 ;    1-DJ1/DJ2\n')
            while alpha > 1.e-15 :
                alpha /= 10
                state_preco2 = state_preco1 - alpha * dx
                dy2, err2 = self._computeDepartures(state_preco2, 'state2')
                J1 = self._computeCostFunction(state_preco2, dy2, err2)

                # 3) Compute the gradient test itself: (J(x+dt) - J(x)) / dot(J', alpha * dx)
                DJ1 = abs(J1.tot - J0)
                DJ2 = abs(dot(gradient_preco, alpha * dx))
                self.iteration += 1
                logger.info(f'Gradient test: {alpha =}; {DJ1/DJ2 = :.10f}; {1-DJ1/DJ2 = :.10f}; ')
                fid.write(f'{alpha:6.0e} ; {DJ1:16.8e} ; {DJ2:16.8e} ; {DJ1/DJ2:.10f} ; {1-DJ1/DJ2:.10f}\n')

    def AdjointTest(self):

        # 1) Do a first (prior) forward run:
        state_preco1 = zeros(self.control.size)
        fwd1, _ = self._computeDepartures(state_preco1, step='adjtest1')

        # 2) Do a second (altered) forward run:
        state_preco2 = random.randn(self.control.size)
        fwd2, _ = self._computeDepartures(state_preco2, step='adjtest2')

        dx1 = state_preco2
        dy1 = fwd2 - fwd1  # substract the prior/background fluxes

        # 3) Do an adjoint run, with random departures
        dy2 = random.randn(fwd2.shape[0])
        dx2 = self._compute_adjoint(dy2)

        logger.info(f"Adjoint test result (value should be < machine precision) : { 1 - dot(dy1, dy2) / dot(dx1, dx2) }")

    def Var4D(self, label='apos'):
        self.minimizer.reset()     # Just to make sure ...

        state_preco = zeros(self.control.size)

        # self.control.vectors.loc[:, 'state_prior'] = 0 #TODO: 

        step = 'apri'
        status = 0
        while status == 0 :
            state_preco, status = self._Var4D_step(state_preco, step)
            step = 'var4d'

        # Finishup ...
        dy, err = self._computeDepartures(state_preco, label)
        self.J = self._computeCostFunction(state_preco, dy, err)
        gradient_preco = self._ComputeGradient(state_preco, dy, err)
        self.minimizer.update(gradient_preco, self.J.tot)
        self._calcPosteriorUncertainties()
        self.save(label)
        
    def Var4D_resume(self, label='apos', trim=0):
        state_preco = self.minimizer.resume(trim=trim)
        self.iteration = self.minimizer.iter
        status = self.minimizer.commfile.checkUpdate()
        step = 'var4d'
        while status == 0 :
            state_preco, status = self._Var4D_step(state_preco, step)
        # Finishup ...
        dy, err = self._computeDepartures(state_preco, label)
        self.J = self._computeCostFunction(state_preco, dy, err)
        gradient_preco = self._ComputeGradient(state_preco, dy, err)
        self.minimizer.update(gradient_preco, self.J.tot)
        self._calcPosteriorUncertainties()
        self.save(label)

    def _Var4D_step(self, state_preco, step='apri'):
        dy, err = self._computeDepartures(state_preco, step)
        self.J = self._computeCostFunction(state_preco, dy, err)
        gradient_preco = self._ComputeGradient(state_preco, dy, err)
        status = self.minimizer.calc_update(state_preco, gradient_preco, self.J.tot)
        state_preco = self.minimizer.readState()
        self.iteration += 1
        return state_preco, status

    def Apri(self, label='apos'):
        self.minimizer.reset()     # Just to make sure ...

        state_preco = zeros(self.control.size)

        step = 'apri'
        dy, err = self._computeDepartures(state_preco, step)

        dy = ones(dy.shape)

        adjoint_struct = self.model.runAdjoint(dy, self.atmdel)

        try:
            adjoint_struct.WriteStruct(os.path.join(self.rcf.get('path.output'), 'adjoint_struct.nc'))
        except:
            return adjoint_struct

    def _computeDepartures(self, state_preco, step, add_prior=True):
        state = self.control.xc_to_x(state_preco, add_prior=add_prior)
        struct = self.interface.VecToStruct(state)
        departures = self.model.calcDepartures(struct, step=step, atmdel=self.atmdel) # TODO: add atmdel
        dy = departures.loc[:, 'mismatch']
        dye = departures.loc[:, 'err']
        return dy, dye

    def _computeCostFunction(self, state_preco, dy, dye):
        dstate = state_preco-self.control.get('state_prior_preco')   # TODO: check if state_prior_preco is ever non-zero
        J_bg = 0.5*dot(dstate, dstate)
        J_obs = 0.5*dot(dy/dye, dy/dye)
        J = costFunction(bg=J_bg, obs=J_obs)
        logger.info(f"Iteration {self.iteration}: J_bg={J_bg:.2f}; J_obs={J_obs:.2f}")
        return J

    def _ComputeGradient(self, state_preco, dy, dye):
        # adjoint_struct = self.model.runAdjoint(dy/dye**2, self.atmdel)
        # adjoint_state = self.interface.VecToStruct_adj(adjoint_struct)
        gradient_obs_preco = self._compute_adjoint(dy/dye**2) #self.control.g_to_gc(adjoint_state)
        state_departures = state_preco-self.control.get('state_prior_preco')
        gradient_preco = gradient_obs_preco + state_departures
        mode = 'w' if self.iteration == 0 else 'a'
        with open(os.path.join(self.rcf.get('path.output'), 'costFunction.txt'), mode=mode) as fid :
            fid.write(f"iter {self.iteration}: J_obs = {self.J.obs}; J_bg = {self.J.bg}; dJ_obs={sum(gradient_obs_preco)}; dJ_bg={sum(state_departures)} \n")
                    # x_adj={sum(adjoint_state), sum(adjoint_struct[tr][cat]['emis'] for tr in adjoint_struct.keys() for cat in adjoint_struct[tr].keys()).sum()} \n")
        return gradient_preco

    def _compute_adjoint(self, departures):
        adjoint_struct = self.model.runAdjoint(departures, self.atmdel)
        adjoint_state = self.interface.VecToStruct_adj(adjoint_struct)
        gradient_obs_preco = self.control.g_to_gc(adjoint_state)
        return gradient_obs_preco

    def _calcPosteriorUncertainties(self, store_eigenvec=False):
        converged_eigvals, converged_eigvecs = self.minimizer.read_eigsys()
        LE = zeros_like(converged_eigvecs)
        for ii in range(len(converged_eigvals)):
            # If use_eigen_file is true, for each eigenvector, check first whether the file eigenvector_modelspace_%03i.nn
            # exists or not. If it does, read the eigenvector in model space from that file. Else, calculate the
            # eigenvector in model space and store it in such a file.
            dummy = self.control.xc_to_x(converged_eigvecs[:,ii], add_prior=False)
            LE[:,ii] = dummy.values #self.control.xc_to_x(converged_eigvecs[:,ii], add_prior=False)
            if store_eigenvec:
                self.control.set(LE[:, ii], 'eigenvec_%i'%ii)

        Mat2 = 1./converged_eigvals - 1.
        dapri = self.control.get('prior_uncertainty')
        dapos = nan_to_num(sqrt(dapri**2 + inner(LE**2, Mat2)))
        self.control.set('posterior_uncertainty', dapos)

    def save(self, step=None):
        step = '' if step is None else step+'.'
        path = self.rcf.get('path.output')

        self.rcf.write(os.path.join(path, 'lumia.%src'%step))
        self.model.save(path, step)
        self.control.save(os.path.join(path, 'control.%shdf'%step))
        #self.minimizer.save(os.path.join(path, 'comm_file.%snc4'%step))

        # Copy to archive
        arc = Archive(self.rcf.get('path.archive'))
        for file in os.scandir(path):
            if file.is_file:
                arc.put(os.path.join(path, file.name))