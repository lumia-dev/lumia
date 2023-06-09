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
from lumia.utils.dconf import prefix
from typing import Dict


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


@dataclass
class Settings :
    output_path : Path
    communication_file : Path
    gradient_norm_reduction : float = 1.e8
    number_of_iterations : int = 100
    max_number_of_iterations : int = 1000
    executable : Path = prefix / 'bin/congrad.exe'

    def __post_init__(self):
        self.output_path = Path(self.output_path)
        self.communication_file = Path(self.communication_file)
        self.executable = Path(self.executable)


@dataclass(kw_only=True)
class Var4D:
    prior : Prior                           # Contains the prior uncertainty (B) as sigmas + correlation matrices
    model : Model                           # Calculates y = H(X) and x* = H*(dy)
    mapping : Mapping                       # Contains the mapping between state vector and model params
    observations : Observations             # Contains the obs, obs uncertainties
    settings : Settings | DictConfig | Dict # settings requires by the optimizer class (i.e. this class)
    iteration : int = 0
    _vectors : DataFrame | None = None
    
    def __post_init__(self):

        if isinstance(self.settings, (dict, DictConfig)):
            self.settings = Settings(**self.settings) # {k: v for (k, v) in self.settings.items() if k in Settings.__annotations__})

        # Setup minimizer
        self.minimizer = Congrad(self.settings)

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
        self.model.setup_observations(self.observations)

    @property
    def nobs(self) -> int:
        return len(self.observations.observations)

    @property
    def vectors(self) -> DataFrame:
        if self._vectors is None :
            self._vectors = self.prior.vectors.copy()
        return self._vectors

    @property
    def step(self) -> str:
        if self.iteration == 0 :
            return 'apri'
        elif self.minimizer.converged:
            return 'apos'
        return 'var4d'

    def solve(self) -> NDArray:

        state_preco = self.prior.state_preco

        finished = False
        while not finished: # self.minimizer.converged:
            logger.info(f'Start iteration {self.iteration}, {self.step} step')

            # Full forward-adjoint loop
            obs_departures = self.forward_step(state_preco)
            prior_departures = state_preco - self.prior.state_preco
            cost_func = CostFunction(prior_departures=prior_departures, obs_departures=obs_departures)
            gradient_preco = self.adjoint_step(obs_departures) + prior_departures

            # Diagnostics and saving in the prior and posterior steps:
            if self.step in ['apri', 'apos']:
                self.diagnostics(cost_func)
                self.model.save(self.step)

            # Determine next step or just store the final gradient
            if not self.minimizer.converged:
                state_preco = self.minimizer.calc_update(state_preco, gradient_preco, cost_func.value)
                self.iteration += 1
            else :
                self.minimizer.update_gradient(gradient_preco, cost_func.value)
                finished = True

        self.calc_posterior_uncertainties()
        return state_preco

    def forward_step(self, state_preco: NDArray) -> Departures:
        state = self.xc_to_x(state_preco)
        model_data = self.mapping.vec_to_struct(state)
        return self.model.calc_departures(model_data, step=self.step)

    def adjoint_step(self, obs_departures : Departures) -> NDArray:
        model_data_adj = self.model.calc_departures_adj(obs_departures.mismatch / obs_departures.sigma ** 2)
        state_adj = self.mapping.vec_to_struct_adj(model_data_adj)
        return self.g_to_gc(state_adj)

    def diagnostics(self, J: CostFunction) -> None:
        """
        Perform a chi2 goodness of fit test for the inversion result.
        """
        ndof = self.nobs - self.iteration
        with open(self.settings.output_path / f'chi2.{self.step}.txt', 'w') as fid:
            fid.write(f'Inversion performed with {self.nobs = } observations and niter = {self.iteration} iterations\n')
            fid.write(f'{self.step} cost function (chi2) value: {J.value}\n')
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

    def reset(self):
        """
        Reset optimization:
        - set iteration number to 0
        - reset minimizer
        """
        self.iteration = 0
        self.minimizer = Congrad(self.settings)

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
    y1 = opt.forward_step(state_preco1)
    j0 = CostFunction(prior_departures=state_preco1, obs_departures=y1)
    gradient_preco = opt.adjoint_step(y1) + state_preco1
    
    # 2) Estimate the gradient using a taylor approximation
    dx = random.randn(opt.prior.size)
    alpha = 0.01
    while alpha > 1.e-15:
        alpha /= 10
        state_preco2 = state_preco1 - alpha * dx
        y2 = opt.forward_step(state_preco2)
        j1 = CostFunction(prior_departures=state_preco2, obs_departures=y2)

        # Gradient test: (J(x + dx) - J(x)) / (dJ @ (alpha * dx))
        dj1 = abs(j1.value - j0.value)
        dj2 = abs(gradient_preco @ (alpha * dx))

        logger.info(f'Gradient test: {alpha = :.0e}; {dj1 / dj2 = :.10f}; {1 - dj1  / dj2 = :.10f}; {dj1 = }; {dj2 = }')
