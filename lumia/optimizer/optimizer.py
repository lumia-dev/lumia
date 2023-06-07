#!/usr/bin/env python

from dataclasses import dataclass, field
from pandas import DataFrame
from numpy.typing import NDArray
from .protocols import Departures, Model, Mapping, Prior, Observations
from omegaconf import DictConfig
from functools import partial
from pathlib import Path
from numpy import zeros, inner
from loguru import logger
from ..minimizers.congrad import Minimizer as Congrad
from .preconditioning import xc_to_x, g_to_gc


@dataclass(kw_only=True)
class CostFunction:
    prior_departures : NDArray = field(repr=False)
    obs_departures : Departures = field(repr=False)

    @property
    def prior(self) -> float:
        return 0.5 * self.prior_departures @ self.prior_departures

    @property
    def obs(self) -> float:
        return 0.5 * (self.obs_departures.mismatch / self.obs_departures.sigma) @ (self.obs_departures.mismatch / self.obs_departures.sigma)

    @property
    def value(self) -> float:
        return self.prior + self.obs


@dataclass(kw_only=True)
class Var4D:
    prior : Prior                   # Contains the prior uncertainty (B) as sigmas + correlation matrices
    model : Model                   # Calculates y = H(X) and x* = H*(dy)
    mapping : Mapping               # Contains the mapping between state vector and model params
    forcings : Observations         # Contains the obs, obs uncertainties
    minimizer_settings : DictConfig # settings required to setup the minimizer
    settings : DictConfig           # settings requires by the optimizer class (i.e. this class)
    iteration : int = 0
    _vectors : DataFrame | None = None
    
    def __post_init__(self):

        # Setup minimizer
        self.minimizer = Congrad(self.minimizer_settings)

        # Setup preconditioning
        self.xc_to_x = partial(
            xc_to_x,
            temporal_correlations = self.prior.temporal_correlations,
            horizontal_correlations = self.prior.horizontal_correlations,
            sigmas = self.prior.sigmas,
            coordinates = self.prior.coordinates
        )
        self.g_to_gc = partial(
            g_to_gc,
            temporal_correlations = self.prior.temporal_correlations,
            horizontal_correlations = self.prior.horizontal_correlations,
            sigmas = self.prior.sigmas,
            coordinates = self.prior.coordinates
        )

        # Setup model
        self.model.setup_observations(self.forcings)

        # Ensure that the required paths exist:
        self.settings.paths.output = Path(self.settings.paths.output)
        self.settings.paths.output.mkdir(exist_ok=True, parents=True)

    @property
    def nobs(self) -> int:
        return len(self.forcings.observations)

    @property
    def vectors(self) -> DataFrame:
        if self._vectors is None :
            self._vectors = self.prior.vectors.copy()
        return self._vectors
    
    def solve(self):

        state_preco = zeros(self.prior.size)
        step = 'apri'
        while not self.minimizer.converged: # self.minimizer.status < 2 :
            # Forward run:

            # state = self.xc_to_x(state_preco)
            # model_data = self.mapping.vec_to_struct(state)
            # obs_departures = self.model.calc_departures(model_data, step=step)
            obs_departures = self.forward_step(state_preco, step, setup_uncertainties = step == 'apri')

            # Calculate cost function:
            cost_func = CostFunction(prior_departures=state_preco, obs_departures=obs_departures)

            # Adjoint run:
            # model_data_adj = self.model.calc_departures_adj(obs_departures.mismatch / obs_departures.sigma ** 2)
            # state_vec_adj = self.mapping.vec_to_struct_adj(model_data_adj)
            # gradient_preco = self.g_to_gc(state_vec_adj) + state_preco
            gradient_preco = self.adjoint_step(obs_departures) + state_preco

            # diagnostics
            self.diagnostics(step, cost_func)

            # Determine next step:
            step = 'var4d' if not self.minimizer.finished else 'apos'
            self.iteration += 1

            logger.info(self.minimizer.status)
            logger.info(step)

            if not self.minimizer.finished:
                # Calculate next step:
                state_preco = self.minimizer.calc_update(state_preco, gradient_preco, cost_func.value)
            else :
                self.minimizer.update_gradient(gradient_preco, cost_func.value)

        # finishup
        self.calc_posterior_uncertainties()

    def forward_step(self, state_preco: NDArray, step: str = None, setup_uncertainties : bool = False) -> Departures:
        state = self.xc_to_x(state_preco)
        model_data = self.mapping.vec_to_struct(state)
        return self.model.calc_departures(model_data, step=step, setup_uncertainties=setup_uncertainties)

    def adjoint_step(self, obs_departures : Departures) -> NDArray:
        model_data_adj = self.model.calc_departures_adj(obs_departures.mismatch / obs_departures.sigma ** 2)
        state_adj = self.mapping.vec_to_struct_adj(model_data_adj)
        return self.g_to_gc(state_adj)

    def diagnostics(self, step : str, J: CostFunction) -> None:
        """
        Perform a chi2 goodness of fit test for the inversion result.
        """

        if step not in ['apri', 'apos']:
            return

        ndof = self.nobs - self.iteration
        with open(self.settings.paths.output / f'chi2.{step}.txt', 'w') as fid:
            fid.write(f'Inversion performed with {self.nobs = } observations and niter = {self.iteration} iterations\n')
            fid.write(f'{step} cost function (chi2) value: {J.value}\n')
            fid.write(f'      Reduced chi2 (chi2 / ndof): {J.value / ndof :.2f}\n')

    def calc_posterior_uncertainties(self, store_eigenvec: bool = False) -> None:
        """
        Reconstruct posterior uncertainties based on the eigenvector/values computed during the optimization.
        The function doesn't return any argument but stores the "posterior_uncertainty" as an additional column in the "Optimizer.control" structure (use interface.VecToStruct to convert it back to an emission field.
        Arguments:
            store_eigenvec (optional): determine if the eigenvectors, converted to model space, should also be stored in the Optimizer.control structure
        """

        converged_eigvals, converged_eigvecs = self.minimizer.read_eigsys()
        LE = converged_eigvecs * 0.
        for ii in range(len(converged_eigvals)):
            LE[:, ii] = self.xc_to_x(converged_eigvecs[:, ii])
            if store_eigenvec:
                self.vectors.loc[:, f'eigenvec_{ii}'] = LE[:, ii]

        mat2 = 1. / converged_eigvals - 1.
        dapos = (self.vectors.prior_uncertainty ** 2 + inner(LE ** 2, mat2)) ** .5
        self.vectors.loc[:, 'posterior_uncertainty'] = dapos


def adjoint_test(opt: Var4D):
    from numpy import random
    from copy import deepcopy
    
    # Ensure that the obs dataframe has uncertainties.
    # Don't use 1 to prevent the case where square (roots) have no effect
    opt.model.observations.loc[:, 'err'] = 2.

    # Do a reference prior run:
    xref = zeros(opt.prior.size)
    yref = opt.forward_step(xref)

    # Do a perturbed prior run:
    x1 = random.randn(opt.prior.size)
    y1 = opt.forward_step(x1)

    # Do an adjoint run with random obs departures:
    y2 = deepcopy(y1)
    y2.mismatch = random.randn(len(y2.mismatch))
    x2 = opt.adjoint_step(y2)

    # Calculate the difference between the prior and reference concentrations. This gets rid of contributions from non-optimized terms
    y1 = (y1.mismatch - yref.mismatch)
    y2 = y2.mismatch / y2.sigma ** 2

    # Print adjoint test results:
    logger.info(f'Adjoint test result (value should be < machine precision): {1 - (y1 @ y2) / (x2 @ x1)}')


def gradient_test(opt: Var4D):
    from numpy import random
    
    # Ensure that the obs dataframe has uncertainties.
    # Don't use 1 to prevent the case where square (roots) have no effect
    opt.model.observations.loc[:, 'err'] = 2.
    
    # 1) Compute the gradient and cost function for a (random) control vector:
    state_preco1 = random.randn(opt.prior.size)
    y1 = opt.forward_step(state_preco1, step='step1')
    j0 = CostFunction(prior_departures=state_preco1, obs_departures=y1)
    gradient_preco = opt.adjoint_step(y1) + state_preco1
    
    # 2) Estimate the gradient using a taylor approximation
    dx = random.randn(opt.prior.size)
    alpha = 0.01
    while alpha > 1.e-15:
        alpha /= 10
        state_preco2 = state_preco1 - alpha * dx
        y2 = opt.forward_step(state_preco2, step='step2')
        j1 = CostFunction(prior_departures=state_preco2, obs_departures=y2)

        # Gradient test: (J(x + dx) - J(x)) / (dJ @ (alpha * dx))
        dj1 = abs(j1.value - j0.value)
        dj2 = abs(gradient_preco @ (alpha * dx))

        logger.info(f'Gradient test: {alpha = :.0e}; {dj1 / dj2 = :.10f}; {1 - dj1  / dj2 = :.10f}; {dj1 = }; {dj2 = }')
