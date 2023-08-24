from scipy.optimize import fmin_cg
from dataclasses import dataclass
from lumia.optimizer import protocols
from numpy.typing import NDArray
from functools import partial
from .preconditioning import xc_to_x, g_to_gc
from numpy import array_equal
from loguru import logger
from omegaconf import DictConfig
from pathlib import Path
from lumia.utils import debug


@dataclass
class Settings :
    output_path : Path
    gradient_norm_reduction : float = 1.e8
    number_of_iterations : int = 100
    max_number_of_iterations : int = 1000

    def __post_init__(self):
        self.output_path = Path(self.output_path)


@dataclass(kw_only=True)
class Optimizer:
    prior : protocols.Prior
    model : protocols.Model
    mapping : protocols.Mapping
    observations : protocols.Observations
    settings : DictConfig | Settings | dict
    
    def __post_init__(self):
        if isinstance(self.settings, (dict, DictConfig)):
            self.settings = Settings(**self.settings)
    
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

        # Other variables :
        self.iteration = 0
        self.cached_results = {}
        
    @property
    def step(self) -> str:
        if self.iteration == 0 :
            return 'apri'
        return 'var4d'
    
    @property
    def nobs(self) -> int:
        return len(self.observations.observations)
            
    def compute_cost(self, state_preco: NDArray) -> float:
        obs_departures = self.forward_step(state_preco)
        dy = obs_departures.mismatch / obs_departures.sigma
        dx = state_preco - self.prior.state_preco
        cost = 0.5 * dx @ dx + 0.5 * dy @ dy
        self.cached_results['J'] = cost
        logger.info(f'Iteration {self.iteration}: cost function value {cost}')
        return cost

    @debug.trace_call
    def diagnostics(self, state_preco: NDArray):
        """
        Stuff done at the end of each iteration
        """
        logger.info(f"Iteration {self.iteration} completed")
        logger.info(f'Cost function value: {self.cached_results["J"]}')
        self.iteration += 1
        
        # Perform a chi2 goodness of fit test for the inversion result.
        ndof = self.nobs - self.iteration
        J = self.cached_results['J']
        with open(self.settings.output_path / f'chi2.{self.step}.txt', 'w') as fid:
            fid.write(f'Inversion performed with {self.nobs = } observations and niter = {self.iteration} iterations\n')
            fid.write(f'{self.step} cost function (chi2) value: {J}\n')
            fid.write(f'      Reduced chi2 (chi2 / ndof): {J / ndof :.2f}\n')
            
    def compute_gradient(self, state_preco: NDArray) -> NDArray:
        obs_departures = self.forward_step(state_preco)
        prior_departures = state_preco - self.prior.state_preco
        gradient_preco = self.adjoint_step(obs_departures) + prior_departures

        self.diagnostics(state_preco)
        return gradient_preco
        
    def solve(self) -> NDArray:
        return fmin_cg(
            self.compute_cost, 
            self.prior.state_preco, 
            fprime=self.compute_gradient,
            # gtol=self.settings.gradient_norm_reduction,
            # maxiter=self.settings.max_number_of_iterations,
            disp=True,
            full_output=True
        )
        
    def forward_step(self, state_preco: NDArray) -> protocols.Departures:
        if array_equal(self.cached_results.get('state_preco', None), state_preco):
            return self.cached_results['departures']
        
        state = self.xc_to_x(state_preco)
        model_data = self.mapping.vec_to_struct(state)

        # cache the results to avoid them having to be re-computed if the state doesn't change
        self.cached_results['state_preco'] = state_preco
        self.cached_results['departures'] = self.model.calc_departures(model_data, step=self.step)
        return self.cached_results['departures']

    def adjoint_step(self, obs_departures : protocols.Departures) -> NDArray:
        model_data_adj = self.model.calc_departures_adj(obs_departures.mismatch / obs_departures.sigma ** 2)
        state_adj = self.mapping.vec_to_struct_adj(model_data_adj)
        return self.g_to_gc(state_adj)