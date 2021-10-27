#!/usr/bin/env python
import os
import logging
from numpy import nan, zeros, zeros_like, sqrt, inner, nan_to_num, dot
from lumia.minimizers.congrad import Minimizer as congrad
from .Tools import costFunction
from archive import Archive

logger = logging.getLogger(__name__)


class Optimizer(object):
    def __init__(self, rcf, model, interface, minimizer=congrad):
        self.rcf = rcf                        # Settings
        self.model = model                    # Model interfaces instance (initialized!)
        self.control = interface.data         # modelData structure, containing the optimization data
        self.minimizer = minimizer(self.rcf)  # minimizer object instance (initiated!)
        self.interface = interface
        self.iteration = 0

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

    def _computeDepartures(self, state_preco, step):
        state = self.control.xc_to_x(state_preco)
        struct = self.interface.VecToStruct(state)
        departures = self.model.runForward(struct, step=step)
        dy = departures.loc[:, 'mismatch']
        dye = departures.loc[:, 'err']
        return dy, dye

    def _computeCostFunction(self, state_preco, dy, dye):
        dstate = state_preco-self.control.get('state_prior_preco')   # TODO: check if state_prior_preco is ever non-zero
        J_bg = 0.5*dot(dstate, dstate)
        import pdb; pdb.set_trace()
        J_obs = 0.5*dot(dy/dye, dy/dye)
        J = costFunction(bg=J_bg, obs=J_obs)
        logger.info(f"Iteration {self.iteration}: J_bg={J_bg:.2f}; J_obs={J_obs:.2f}")
        return J

    def _ComputeGradient(self, state_preco, dy, dye):
        adjoint_struct = self.model.runAdjoint(dy/dye**2)
        adjoint_state = self.interface.VecToStruct_adj(adjoint_struct)
        gradient_obs_preco = self.control.g_to_gc(adjoint_state)
        state_departures = state_preco-self.control.get('state_prior_preco')
        gradient_preco = gradient_obs_preco + state_departures
        mode = 'w' if self.iteration == 0 else 'a'
        with open(os.path.join(self.rcf.get('path.output'), 'costFunction.txt'), mode=mode) as fid :
            fid.write(f"iter {self.iteration}: J_obs = {self.J.obs}; J_bg = {self.J.bg}; dJ_obs={sum(gradient_obs_preco)}; dJ_bg={sum(state_departures)}; \
                    x_adj={sum(adjoint_state), sum(adjoint_struct[tr][cat]['emis'] for tr in adjoint_struct.keys() for cat in adjoint_struct[tr].keys()).sum()} \n")
        return gradient_preco

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