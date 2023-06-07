#!/usr/bin/env python
from dataclasses import dataclass, field
from numpy.typing import NDArray
from .protocols import Emissions
from pathlib import Path
from typing import List, Tuple
from omegaconf import DictConfig, OmegaConf
from ...observations.protocols import Observations
from pandas import DataFrame, read_hdf
from .io.xr import Data
from ...utils.system import runcmd
import shutil
from loguru import logger
from numpy import ones, array


@dataclass
class Departures:
    mismatch : NDArray
    sigma : NDArray
    index : NDArray
    
    
@dataclass
class Settings:
    tempdir : Path
    executable : List[str]
    footprint_path : Path
    output_path : Path
    split_categories : bool = True
    output_steps : List[str] = field(default_factory=list)
    extra_arguments : List[str] = field(default_factory=list)
    extra_fields : List[str] = field(default_factory=list)
    serial : bool = False

    def __post_init__(self):
        if isinstance(self.executable, str):
            self.executable = [self.executable]
            
            
class Transport:
    def __init__(self, dconf: DictConfig, observations : Observations = None):
        self.dconf = dconf
        self.settings = Settings(
            tempdir = Path(dconf.paths.temp),
            executable = dconf.transport.exec,
            footprint_path = Path(dconf.paths.footprints),
            output_path = Path(dconf.paths.output),
            split_categories = dconf.get('split_categories', True),
            output_steps = dconf.output_steps,
            extra_arguments = dconf.transport.get('extra_arguments', []),
            extra_fields = dconf.get('store_extra_fields', []),
            serial = dconf.transport.get('serial', False)
        )
        self._observations = observations

    def setup_observations(self, observations: Observations) -> None:
        self._observations = observations

    @property
    def observations(self) -> DataFrame | None:
        if self._observations is None :
            return None
        return self._observations.observations

    @property
    def sites(self) -> DataFrame | None:
        if self._observations is None:
            return None
        return self._observations.sites

    def calc_departures(self, emissions: Emissions, step: str = None, setup_uncertainties : bool = False) -> Departures:
        _, obsfile = self.run_forward(emissions, step)

        # db = self._observations.from_hdf(obsfile)
        db : DataFrame = read_hdf(obsfile)

        if self.settings.split_categories:
            for cat in emissions.transported_categories:
                self.observations.loc[:, f'mix_{cat.name}'] = db.loc[:, f'mix_{cat.name}'].values
        self.observations.loc[:, f'mix_{step}'] = db.mix.values
        self.observations.loc[:, 'mix_background'] = db.mix_background.values
        self.observations.loc[:, 'mix_foreground'] = db.mix.values - db.mix_background.values
        self.observations.loc[:, 'mismatch'] = db.mix.values - self.observations.loc[:, 'obs']

        # Optional: store extra columns that the transport model may have written, if requested:
        for key in self.settings.extra_fields :
            self.observations.loc[:, key] = db.loc[:, key].values

        # Output if requested:
        # if step in self.settings.output_steps:
        #     self.save(tag = step, emis_file = emfile)

        if setup_uncertainties:
            self.calc_uncertainties(step=step)

        dept = self.observations.dropna(subset=['mismatch', 'err']).loc[:, ['mismatch', 'err']]
        dept.loc[:, 'sigma'] = dept.err

        return dept.loc[:, ['mismatch', 'sigma']]
        # return Departures(dept.mismatch.values, dept.err.values, dept.index.values)

    def calc_departures_adj(self, forcings : NDArray) -> Data:

        # Write departures file
        self.observations.loc[:, 'dy'] = forcings
        departures_file = self.settings.tempdir / 'departures.hdf'
        self.observations.to_hdf(departures_file, 'departures')

        # Point to the existing emissions file (just used as a template)
        adjemis_file = self.settings.tempdir / 'emissions.nc'

        # Create command
        cmd = self.settings.executable + ['--adjoint', '--obs', departures_file, '--emis', adjemis_file, '--footprints', self.settings.footprint_path, '--tmp', self.settings.tempdir]
        # cmd = [sys.executable, '-u', self.settings.executable, '--adjoint', '--obs', departures_file, '--emis', adjemis_file, '--footprints', self.settings.footprint_path, '--tmp', self.settings.tempdir]
        if self.settings.serial :
            cmd.append('--serial')
        cmd.extend(self.settings.extra_arguments)

        # Run
        runcmd(cmd)

        # Read result and return:
        return Data.from_file(adjemis_file)

    def run_forward(self, emissions: Emissions, step: str = None, serial: bool = False) -> Tuple[Path, Path]:

        # Write the emissions. Don't compress when inside a 4dvar loop, for faster speed
        compression = step in self.settings.output_steps
        emf = emissions.to_netcdf(self.settings.tempdir / 'emissions.nc', zlib=compression, only_transported=True)

        # Write the observations:
        dbf = self.settings.tempdir / 'observations.hdf'
        self.observations.to_hdf(dbf, 'observations')

        # Run the model:
        cmd = self.settings.executable + ['--forward', '--obs', dbf, '--emis', emf, '--footprints', self.settings.footprint_path, '--tmp', self.settings.tempdir]
        # cmd = [sys.executable, '-u', self.settings.executable, '--forward', '--obs', dbf, '--emis', emf, '--footprints', self.settings.footprint_path, '--tmp', self.settings.tempdir]

        if self.settings.serial or serial:
            cmd.append('--serial')

        cmd.extend(self.settings.extra_arguments)
        runcmd(cmd)

        return emf, dbf

    def save(self, path: Path = None, tag : str = None, emis_file : Path = None) -> None:
        """
        Copies the last model I/O to "path", with an optional tag to identify it
        Arguments:
            - path: folder where files should be written. Defaults to the "output_path" attribute
            - tag: tag appended to the file names (e.g. observations.{tag}.tar.gz)
            - emis_file: location of the emission file to be copied to "path" (optional).
        """

        tag = '' if tag is None else tag.strip('.')+'.'
        if not path :
            path = self.settings.output_path
        path.mkdir(exist_ok=True, parents=True)

        OmegaConf.save(OmegaConf.structured(self), path / f'transport.{tag}yaml')
        self._observations.save_tar(path / f'observations.{tag}tar.gz')
        if emis_file is not None :
            shutil.copy(emis_file, path)

    def calc_uncertainties(self, err_obs : str = 'err_obs', err_min : float = 0, step: str = None, freq : str = '7D') -> None:
        # Ensure that all observations have a measurement error:
        sel = self.observations.loc[:, err_obs] <= 0
        self.observations.loc[sel, err_obs] = self.observations.loc[sel, 'obs'] * err_min / 100.

        for code in self.observations.code.drop_duplicates():
            # 1) select the data
            mix = self.observations.loc[self.observations.code == code].loc[:, ['time', 'obs', f'mix_{step}', err_obs]].set_index('time').sort_index()

            # 2) Calculate weekly moving average and residuals from it
            #trend = mix.rolling(freq).mean()
            #resid = mix - trend

            # Use a weighted rolling average, to avoid giving too much weight to the uncertain obs:
            weights = 1. / mix.loc[:, err_obs] ** 2
            total_weight = weights.rolling(freq).sum()   # sum of weights in a week (for normalization)
            obs_weighted = mix.obs * weights
            mod_weighted = mix.loc[:, f'mix_{step}'] * weights
            obs_averaged = obs_weighted.rolling(freq).sum() / total_weight
            mod_averaged = mod_weighted.rolling(freq).sum() / total_weight
            resid_obs = mix.obs - obs_averaged
            resid_mod = mix.loc[:, f'mix_{step}'] - mod_averaged

            # 3) Calculate the standard deviation of the residuals model-data mismatches. Store it in sites dataframe for info.
            sigma = (resid_obs - resid_mod).values.std()
            self.sites.loc[self.sites.code == code, 'err'] = sigma
            logger.info(f'Model uncertainty for site {code} set to {sigma:.2f}')

            # 4) Get the measurement uncertainties and calculate the error inflation
#            s_obs = self.observations.loc[:, err_obs].values
#            nobs = len(s_obs)
#            s_mod = sqrt((nobs * sigma**2 - (s_obs**2).sum()) / nobs)

            # 5) Store the inflated errors:
            self.observations.loc[self.observations.code == code, 'err'] = (
                self.observations.loc[self.observations.code == code, err_obs] ** 2 + sigma ** 2).values ** .5
            self.observations.loc[self.observations.code == code, 'resid_obs'] = resid_obs.values
            self.observations.loc[self.observations.code == code, 'resid_mod'] = resid_mod.values
            self.observations.loc[self.observations.code == code, 'obs_detrended'] = obs_averaged.values
            self.observations.loc[self.observations.code == code, 'mod_detrended'] = mod_averaged.values

    def calc_sensi_map(self, emissions: Emissions):
        departures = ones(self.observations.shape[0])
        emissions.to_netcdf(self.settings.tempdir / 'emissions.nc', zlib=False, only_transported=True)
        adjfield = self.calc_departures_adj(departures)
        sensi = {}
        for tracer in adjfield.tracers:
            sensi[tracer] = array([adjfield[tracer][cat].data.sum(0) for cat in adjfield[tracer].categories]).sum(0)
        return sensi


def adjoint_test(model: Transport, emis: Emissions):
    from numpy import random, append
    
    # 1) Do a reference run
    _, departures1 = model.calc_departures(emis, step='adjtest1')
    
    # 2) Do a second forward run, with perturbed emissions:
    dx1 = array([])
    for cat in emis.transported_categories:
        dx = random.randn(emis[cat.tracer][cat.name].size)
        emis[cat.tracer][cat.name].data += dx.reshape(emis[cat.tracer].shape)
        dx1 = append(dx1, dx)
        
    _, departures2 = model.calc_departures(emis, step='adjtest2')
    
    dy1 = (departures2.mismatch - departures1.mismatch).values
    
    # 3) Do an adjoint run:
    dy2 = random.randn(dy1.mismatch.size)
    adj = model.calc_departures_adj(dy2)
    
    # 4) Convert to vectors:
    dx2 = array([])
    for cat in emis.transported_categories:
        dx = adj[cat.tracer][cat.name].data.reshape(-1)
        dx2 = append(dx2, dx)
        
    logger.info(f'Adjoint test value: {1 - (dy1 @ dy2) / (dx1 @ dx2) = }')
    
