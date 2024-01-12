#!/usr/bin/env python
import sys
from dataclasses import dataclass, field
from numpy.typing import NDArray
from pathlib import Path
from typing import Tuple, List
from omegaconf import DictConfig, OmegaConf
from pandas import DataFrame, read_hdf
import shutil
from loguru import logger
from numpy import ones, array

from lumia.models.footprints.protocols import Emissions
from lumia.observations.protocols import Observations
from lumia.utils.system import runcmd
from lumia.data.xr import Data
from lumia.utils import debug


@dataclass
class Departures:
    mismatch : NDArray
    sigma : NDArray
    index : NDArray

    
@dataclass
class Transport:
    path_temp : Path
    path_output : Path
    path_footprints : Path
    executable : List[str]
    split_categories : bool
    output_steps : List[str]
    extra_arguments : DictConfig
    extra_fields : List[str]
    serial : bool
    setup_uncertainties : List[str] = field(default_factory=list)
    emissions_file : Path | None = None

    def __post_init__(self):
        self._observations = None
        self.path_temp = Path(self.path_temp)
        self.path_output = Path(self.path_output)
        self.path_footprints = Path(self.path_footprints)
        if self.emissions_file is None :
            self.emissions_file = self.path_temp / 'emissions.nc'
        self.path_output.mkdir(parents=True, exist_ok=True)
        self.path_temp.mkdir(parents=True, exist_ok=True)
        if isinstance(self.executable, (str, Path)):
            # Assume it's a python file, and run it with the current interpreter (i.e. in the same virtual environment)
            self.executable = [sys.executable, str(self.executable)]

        # Ensure that the "extra_arguments" (if any) are str:
        #for k, v in self.extra_arguments.items():
        #    self.extra_arguments[k] = [str(arg) for arg in self.extra_arguments[k]]

    def setup_observations(self, obs : Observations):
        self._observations = obs

    @property
    def observations(self) -> DataFrame | None:
        return self._observations.observations

    @property
    def sites(self) -> DataFrame | None:
        if self._observations is None:
            return None
        return self._observations.sites

    # Main methods:
    @debug.timer
    def calc_departures(self, emissions: Emissions, step: str = None) -> Departures:
        _, obsfile = self.run_forward(emissions, step)

        # db = self._observations.from_hdf(obsfile)
        db : DataFrame = read_hdf(obsfile)

        if self.split_categories:
            for cat in emissions.transported_categories:
                self.observations.loc[:, f'mix_{cat.name}'] = db.loc[:, f'mix_{cat.name}'].values
        self.observations.loc[:, f'mix_{step}'] = db.mix.values
        self.observations.loc[:, 'mix_background'] = db.mix_background.values
        self.observations.loc[:, 'mix_foreground'] = db.mix.values - db.mix_background.values
        self.observations.loc[:, 'mismatch'] = db.mix.values - self.observations.loc[:, 'obs']

        # Optional: store extra columns that the transport model may have written, if requested:
        for key in self.extra_fields :
            self.observations.loc[:, key] = db.loc[:, key].values

        if step in self.setup_uncertainties:
            self._observations.calc_uncertainties(step=step)

        dept = self.observations.dropna(subset=['mismatch', 'err']).loc[:, ['mismatch', 'err']]
        dept.loc[:, 'sigma'] = dept.err

        # Save output if requested:
        if step is None or step in self.output_steps :
            self.save(path=self.path_output, tag=step)

        return dept.loc[:, ['mismatch', 'sigma']]

    @debug.timer
    def calc_departures_adj(self, forcings : DataFrame, step='adjoint') -> Data:

        # Write departures file
        self.observations.loc[forcings.index, 'dy'] = forcings
        departures_file = self.path_temp / 'departures.hdf'
        self.observations.dropna(subset=['dy']).to_hdf(departures_file, 'departures')

        # Point to the existing emissions file (just used as a template)
        adjemis_file = self.emissions_file

        # Create command
        cmd = self.executable + ['--adjoint', '--obs', departures_file, '--emis', adjemis_file, '--footprints', self.path_footprints, '--tmp', self.path_temp]
        if self.serial :
            cmd.append('--serial')
        if step in self.extra_arguments:
            cmd.append(self.extra_arguments[step])
        if "*" in self.extra_arguments:
            cmd.append(self.extra_arguments['*'])
        # Run
        runcmd(cmd, shell=True)

        # Read result and return:
        return Data.from_file(adjemis_file)

    @debug.timer
    def run_forward(self, emissions: Emissions, step: str = None, serial: bool = False) -> Tuple[Path, Path]:

        # Write the emissions. Don't compress when inside a 4dvar loop, for faster speed
        compression = step in self.output_steps
        emissions.print_summary()
        emf = emissions.to_netcdf(self.emissions_file, zlib=compression, only_transported=True)

        # Write the observations:
        dbf = self.path_temp / 'observations.hdf'
        self.observations.to_hdf(dbf, 'observations')

        # Run the model:
        cmd = self.executable + ['--forward', '--obs', dbf, '--emis', emf, '--footprints', self.path_footprints, '--tmp', self.path_temp]

        if self.serial or serial:
            cmd.append('--serial')

        if step in self.extra_arguments:
            cmd.append(self.extra_arguments[step])
        if '*' in self.extra_arguments:
            cmd.append(self.extra_arguments['*'])
        runcmd(cmd, shell=True)

        return emf, dbf

    @debug.timer
    def save(self, tag : str | None = None, path: Path | None = None):
        """
        Copies the last model I/O to "path", with an optional tag to identify it
        Arguments:
            - path: folder where files should be written. Defaults to the "output_path" attribute
            - tag: tag appended to the file names (e.g. observations.{tag}.tar.gz)
        """

        tag = '' if tag is None else tag.strip('.')+'.'
        if not path :
            path = self.path_output
        path.mkdir(exist_ok=True, parents=True)

        # OmegaConf.save(OmegaConf.structured(self), path / f'transport.{tag}yaml')
        self._observations.save_tar(path / f'observations.{tag}tar.gz')
        shutil.copy(self.emissions_file, path / f'emissions.{tag}nc')

    @debug.timer
    def calc_sensi_map(self, emissions: Emissions):
        departures = ones(self.observations.shape[0])
        emissions.to_netcdf(self.path_temp / 'emissions.nc', zlib=False, only_transported=True)
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
    
