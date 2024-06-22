from scipy.optimize import fmin_cg, minimize, OptimizeResult
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
from pandas import DataFrame
import pickle


@dataclass
class Settings :
    output_path : Path
    gradient_norm_reduction : float = None
    number_of_iterations : int = 100
    max_number_of_iterations : int = 1000
    gtol : float = None

    def __post_init__(self):
        self.output_path = Path(self.output_path)


@dataclass(kw_only=True)
class Optimizer:
    prior : protocols.Prior                 # Contains the prior uncertainty (B) as sigmas + correlation matrices
    model : protocols.Model                 # Calculates y - H(x) and x* = H*(dy)
    mapping : protocols.Mapping             # contains the mapping between state vector and model params
    observations : protocols.Observations   # Contains the obs and obs uncertainties
    settings : DictConfig | Settings | dict # Settings required by the optimizer (i.e. this class)
    _vectors : DataFrame | None = None  # see vectors property below
    step : str = 'apri'
    n_gradient_evaluations : int = 0
    
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
        self.iteration : int = 0                    # just for diagnostics
        self.cached_results : dict = {}             # store transport model results, to avoid calling it twice with the same settings
        
    @property
    def nobs(self) -> int:
        return len(self.observations.observations)
    
    @property
    def vectors(self) -> DataFrame:
        """
        "vectors" is a DataFrame containing the prior, posterior and intermediate control vectors, 
        as well as their metadata (coordinates, category, uncertainty, etc.).
        It is based on a copy the "vectors" attribute of the "prior" object, which may not exist at
        the time of creation of the optimizer object. We perform the copy here, if necessary.
        """
        if self._vectors is None :
            self._vectors = self.prior.vectors.copy()
        return self._vectors
            
    @debug.timer
    def compute_cost(self, state_preco: NDArray) -> float:
        obs_departures = self.forward_step(state_preco)
        dy = obs_departures.mismatch / obs_departures.sigma
        dx = state_preco - self.prior.state_preco
        cost = 0.5 * dx @ dx + 0.5 * dy @ dy
        self.cached_results['J'] = cost
        logger.info(f'Iteration {self.iteration}: cost function value {cost}')
        logger.info(f'  J_bg = {0.5 * dx @ dx:.3f}; J_obs = {0.5 * dy @ dy:.3f}')
        return cost

    # @debug.trace_call
    # def diagnostics(self, state_preco: NDArray, gradient_preco: NDArray):
        # """
        # Stuff done at the end of each iteration
        # """
        # logger.info(f"Iteration {self.iteration} completed")
        # logger.info(f'Cost function value: {self.cached_results["J"]}')
        # logger.info(f'Gradient norm reduction: {self.prior_gradient_norm / (gradient_preco @ gradient_preco)**.5}')
        
        # if self.step == 'var4d':
        #     self.iteration += 1
        
        # Perform a chi2 goodness of fit test for the inversion result.
        # ndof = self.nobs - self.iteration
        # J = self.cached_results['J']
        # with open(self.settings.output_path / f'chi2.{self.step}.txt', 'w') as fid:
        #     fid.write(f'Inversion performed with {self.nobs = } observations and niter = {self.iteration} iterations\n')
        #     fid.write(f'{self.step} cost function (chi2) value: {J}\n')
        #     fid.write(f'      Reduced chi2 (chi2 / ndof): {J / ndof :.2f}\n')

    def callback(self, intermediate_result: OptimizeResult):
        """
        Stuff done at the end of each iteration
        """
        logger.info(f"Iteration {self.iteration} completed")
        logger.info(f'Cost function value: {self.cached_results["J"]}')
        logger.info(f'Number of gradient evaluations: {self.n_gradient_evaluations}')
        if self.n_gradient_evaluations >= self.settings.max_number_of_iterations:
            return True
            raise StopIteration
        self.iteration += 1
            
    @debug.timer
    def compute_gradient(self, state_preco: NDArray) -> NDArray:
        obs_departures = self.forward_step(state_preco)
        prior_departures = state_preco - self.prior.state_preco
        gradient_preco = self.adjoint_step(obs_departures) + prior_departures
        
        self.n_gradient_evaluations += 1
        return gradient_preco
    
    def solve(self) -> NDArray:
        
        # Prior run 
        self.step = 'apri'
        prior_obs_departures = self.forward_step(self.prior.state_preco)
        
        if self.settings.gradient_norm_reduction is not None :
            gradient_preco = self.adjoint_step(prior_obs_departures)
            prior_gradient_norm = (gradient_preco @ gradient_preco) ** .5
            
            # the scipy minimizer only accepts an absolute value for the target gradient norm. So we deduce that value from the initial gradient and the requested gradient norm reduction
            gtol = prior_gradient_norm / self.settings.gradient_norm_reduction
            self.settings.gtol = gtol
            logger.info(f'Setting the target gradient norm to {gtol =}')
        
        # Perform the actual minimization
        self.step = 'var4d'
        
        self.n_gradient_evaluations = 0

        result = minimize(
            self.compute_cost,
            self.prior.state_preco,
            jac=self.compute_gradient,
            callback=self.callback,
            method='CG',
            options={
                'maxiter': self.settings.number_of_iterations, 
                'disp': True,
                'return_all': True,
                'gtol': self.settings.gtol
            }
        )

        # Do a final (posterior run):
        self.step = 'apos'
        gradient_preco_apos = self.compute_gradient(result.x)
        posterior_gradient_norm = (gradient_preco_apos @ gradient_preco_apos) ** .5
        
        logger.info(f'Inversion finished. Achieved gradient norm reduction of {prior_gradient_norm / posterior_gradient_norm}')
        logger.info(f'    Prior gradient norm: {prior_gradient_norm : .2f}')
        logger.info(f'Posterior gradient norm: {posterior_gradient_norm : .2f}')

        # Store "result":
        with open(self.settings.output_path / 'optim_debug.pickle', 'wb') as fid:
            pickle.dump(result, fid)

        return result.x
    
    @debug.timer
    def forward_step(self, state_preco: NDArray) -> protocols.Departures:
        if array_equal(self.cached_results.get('state_preco', None), state_preco):
            return self.cached_results['departures']
        
        state = self.xc_to_x(state_preco)
        model_data = self.mapping.vec_to_struct(state)

        # cache the results to avoid them having to be re-computed if the state doesn't change
        self.cached_results['state_preco'] = state_preco
        self.cached_results['departures'] = self.model.calc_departures(model_data, step=self.step)
        return self.cached_results['departures']

    @debug.timer
    def adjoint_step(self, obs_departures : protocols.Departures) -> NDArray:
        model_data_adj = self.model.calc_departures_adj(obs_departures.mismatch / obs_departures.sigma ** 2)
        state_adj = self.mapping.vec_to_struct_adj(model_data_adj)
        return self.g_to_gc(state_adj)
