#!/usr/bin/env python
import os
from numpy import zeros, zeros_like, sqrt, inner, nan_to_num, dot, random
from lumia.minimizers.congrad import Minimizer as Congrad
from .Tools import CostFunction
from archive import Rclone
from loguru import logger
from lumia.uncertainties import Uncertainties_mt as Uncertainties
from lumia.Tools.system_tools import checkDir
from types import SimpleNamespace


class Optimizer(object):
    def __init__(self, rcf, model, interface, minimizer=Congrad):
        self.rcf = rcf                        # Settings
        self.model = model                    # Model interfaces instance (initialized!)
        self.minimizer = minimizer(self.rcf)  # minimizer object instance (initiated!)
        self.interface = interface            # transitions between model data and control vector
        self.control = Uncertainties().setup(interface)  # TODO: create a proper "Precon" module to handle the uncertainties
        self.iteration = 0

        # Paths :
        self.paths = SimpleNamespace(
            output = self.rcf.rcfGet('run.paths.output'),
            archive = self.rcf.rcfGet('run.paths.archive', default=False)
        )

        checkDir(self.paths.output)

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

        with open(os.path.join(self.paths.output, 'gradient_test.log'), 'w') as fid :
            fid.write(f' alpha ;                DJ1 ;                DJ2 ;      DJ1/DJ2 ;    1-DJ1/DJ2\n')
            while alpha > 1.e-15 :
                alpha /= 10
                state_preco2 = state_preco1 - alpha * dx
                dy2, err2 = self._computeDepartures(state_preco2, 'state2')
                J1 = self._computeCostFunction(state_preco2, dy2, err2)

                # 3) Compute the gradient test itself: (J(x+dt) - J(x)) / dot(J', alpha * dx)
                DJ1 = abs(J1.tot - J0)
                DJ2 = abs(dot(gradient_preco, alpha * dx))
                logger.info(f'Gradient test: {alpha =}; {DJ1/DJ2 = :.10f}; {1-DJ1/DJ2 = :.10f}; {DJ1=}; {DJ2=}')
                fid.write(f'{alpha:6.0e} ; {DJ1:16.8e} ; {DJ2:16.8e} ; {DJ1/DJ2:.10f} ; {1-DJ1/DJ2:.10f}\n')

    def AdjointTest(self):

        # 1) Do a first (prior) forward run:
        state_preco1 = zeros(self.control.size)
        fwd1, _ = self._computeDepartures(state_preco1, step='adjtest1')

        # 2) Do a second (altered) forward run:
        state_preco2 = random.randn(self.control.size)
        fwd2, _ = self._computeDepartures(state_preco2, step='adjtest1')

        dx1 = state_preco2
        dy1 = fwd2 - fwd1  # substract the prior/background fluxes

        # 3) Do an adjoint run, with random departures
        dy2 = random.randn(fwd2.shape[0])
        dx2 = self._compute_adjoint(dy2)

        logger.info(f"Adjoint test result (value should be < machine precision) : { 1 - dot(dy1, dy2) / dot(dx1, dx2) }")

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
        logger.info(f"optimizer.var4D() Finishup dy={dy}, err={err} ")
        self.J = self._computeCostFunction(state_preco, dy, err)
        gradient_preco = self._ComputeGradient(state_preco, dy, err)
        self.minimizer.update(gradient_preco, self.J.tot)
        self.calc_chi2(len(dy))
        self._calcPosteriorUncertainties()

        # Merge prior and posterior in a single file/Data:
        self.interface.save_step(self.control.xc_to_x(state_preco), 'apos')

        self.save(label)

    def calc_chi2(self, nobs: int) -> None:
        """
        Perform a chi2 goodness of fit test for the inversion result.
        """
        ndof = nobs - self.iteration
        with open(os.path.join(self.paths.output, 'chi2.txt'), 'w') as fid:
            fid.write(f'Inversion performed with {nobs = } observations and niter = {self.iteration} iterations\n')
            fid.write(f'Prior cost function (chi2) value: {self.J_pri.tot}\n')
            fid.write(f'      Reduced chi2 (chi2 / ndof): {self.J_pri.tot / ndof :.2f}\n')
            fid.write(f'Final cost function (chi2) value: {self.J.tot :.2f}\n')
            fid.write(f'      Reduced chi2 (chi2 / ndof): {self.J.tot / ndof :.2f}\n')

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
        if step == 'apri':
            self.J_pri = self.J
        return state_preco, status

    def _computeDepartures(self, state_preco, step, add_prior=True):
        state = self.control.xc_to_x(state_preco, add_prior=add_prior)
        struct = self.interface.VecToStruct(state)
        departures = self.model.calcDepartures(struct, step=step)
        dy = departures.loc[:, 'mismatch']
        dye = departures.loc[:, 'err']
        return dy, dye

    def _computeCostFunction(self, state_preco, dy, dye):
        dstate = state_preco-self.control.get('state_prior_preco')
        logger.info(f"Iteration {self.iteration}: dstate={dstate}")
        J_bg = 0.5*dot(dstate, dstate)
        J_obs = 0.5*dot(dy/dye, dy/dye)
        J = CostFunction(bg=J_bg, obs=J_obs)
        logger.info(f"Iteration {self.iteration}: J_bg={J_bg:.2f}; J_obs={J_obs:.2f}")
        return J

    def _ComputeGradient(self, state_preco, dy, dye):
        gradient_obs_preco = self._compute_adjoint(dy/dye**2)
        state_departures = state_preco-self.control.get('state_prior_preco')
        gradient_preco = gradient_obs_preco + state_departures
        mode = 'w' if self.iteration == 0 else 'a'
        with open(os.path.join(self.paths.output, 'costFunction.txt'), mode=mode) as fid :
            fid.write(f"iter {self.iteration}: J_obs = {self.J.obs}; J_bg = {self.J.bg}; dJ_obs={sum(gradient_obs_preco)}; dJ_bg={sum(state_departures)} \n")
        return gradient_preco

    def _compute_adjoint(self, departures):
        adjoint_struct = self.model.runAdjoint(departures)
        adjoint_state = self.interface.VecToStruct_adj(adjoint_struct)
        gradient_obs_preco = self.control.g_to_gc(adjoint_state)
        return gradient_obs_preco

    def _calcPosteriorUncertainties(self, store_eigenvec: bool = False) -> None:
        """
        Reconstruct posterior uncertainties based on the eigenvector/values computed during the optimization.
        The function doesn't return any argument but stores the "posterior_uncertainty" as an additional column in the "Optimizer.control" structure (use interface.VecToStruct to convert it back to an emission field.
        Arguments:
            store_eigenvec (optional): determine if the eigenvectors, converted to model space, should also be stored in the Optimizer.control structure
        """

        converged_eigvals, converged_eigvecs = self.minimizer.read_eigsys()
        LE = zeros_like(converged_eigvecs)
        for ii in range(len(converged_eigvals)):
            LE[:, ii] = self.control.xc_to_x(converged_eigvecs[:, ii], add_prior=False)
            if store_eigenvec:
                self.control.set(LE[:, ii], f'eigenvec_{ii}')

        mat2 = 1./converged_eigvals - 1.
        dapri = self.control.get('prior_uncertainty')
        dapos = nan_to_num(sqrt(dapri**2 + inner(LE**2, mat2)))
        self.control.set('posterior_uncertainty', dapos)

    def save(self, step : str = None) -> None:
        """
        Save the various variables and files used during the optimization to the output path ("path.output") rc-key. Optionally (if the "path.archive" rc-key is defined), transfer data to an external archive (rclone-based).
        If a "step" optional argument is provided, the files are renamed to include that step name.

        Specifically, the save function does :
        - write the rc-file as lumia.{step.}rc
        - trigger "Optimizer.model.save", which saves the model rc-file and the observations
        - trigger "Optimizer.interface.save" which saves the prior emissions (emissions_apri.nc) as well as the control vectors (control.hdf).
        - trigger "Optimizer.minimizer.save", which saves the minimizer data file (congrad.nc) and output (congrad_debug.txt) to the output path
        """
        #step = '' if step is None else step+'.'
        path = self.paths.output

        self.rcf.write(os.path.join(path, f'lumia.rc'))
        self.model.save(path, step)
        self.interface.save(path, step)
        self.minimizer.save(path)
        self.control.save(os.path.join(path, 'control.hdf'))

        # Copy to archive
        if self.paths.archive:
            arc = Rclone(self.paths.archive)
            for file in os.scandir(path):
                if file.is_file:
                    arc.put(os.path.join(path, file.name))
