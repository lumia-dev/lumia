#!/usr/bin/env python
import sys
from dataclasses import dataclass, field
from numpy.typing import NDArray
from pathlib import Path
from typing import Tuple, List, Mapping
from omegaconf import DictConfig, ListConfig, OmegaConf
from pandas import DataFrame, read_hdf
import shutil
from loguru import logger
from numpy import ones, array
from shlex import split as shlex_split

from lumia.models.footprints.protocols import Emissions
from lumia.observations.protocols import Observations
from lumia.utils.system import runcmd
from lumia.data.xr import Data
from lumia.utils import debug

from .transport import Departures, adjoint_test
from .transport import Transport as BaseTransport
    
@dataclass(kw_only=True)
class Transport(BaseTransport):

    path_temp : Path
    path_output : Path
    path_footprints : Mapping[str, str | Path]
    path_footprints_scratch : Mapping[str, str | Path]
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
        for k, v in self.path_footprints.items():
            self.path_footprints[k] = Path(v)
        for k, v in self.path_footprints_scratch.items():
            self.path_footprints_scratch[k] = Path(v)
        if self.emissions_file is None :
            self.emissions_file = self.path_temp / 'emissions.nc'
        self.path_output.mkdir(parents=True, exist_ok=True)
        self.path_temp.mkdir(parents=True, exist_ok=True)
        if isinstance(self.executable, (str, Path)):
            # Assume it's a python file, and run it with the current interpreter (i.e. in the same virtual environment)
            self.executable = [sys.executable, str(self.executable)]

    # Main methods:
    @debug.timer
    def calc_departures(self, emissions: Emissions, step: str = None) -> Departures:
        _, obsfile = self.run_forward(emissions, step)

        # db = self._observations.from_hdf(obsfile)
        db : DataFrame = read_hdf(obsfile)

        if self.split_categories:
            for cat in emissions.transported_categories:
                self.observations.loc[:, f'mix_{cat.tracer}_{cat.name}'] = db.loc[:, f'mix_{cat.tracer}_{cat.name}'].values
                if cat.is_natural or cat.is_fossil:
                    self.observations.loc[:, f'mix_c14_{cat.name}'] = db.loc[:, f'mix_c14_{cat.name}'].values

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
    def calc_departures_adj(self, forcings: DataFrame, step='adjoint') -> Data:

        # Write departures file
        self.observations.loc[forcings.index, 'dy'] = forcings
        departures_file = self.path_temp / 'departures.hdf'
        self.observations.dropna(subset=['dy']).to_hdf(departures_file, key='departures')

        # Point to the existing emissions file (just used as a template)
        adjemis_file = self.emissions_file

        # Create command
        cmd = list(self.executable) + [
            '--adjoint',
            '--obs', str(departures_file),
            '--emis', str(adjemis_file),
            '--tmp', str(self.path_temp),
        ]
        cmd.extend(_expand_path_option('--footprints', self.path_footprints))

        if self.serial:
            cmd.append('--serial')

        # Run
        runcmd(cmd, shell=False)

        # Read result and return:
        return Data.from_file(adjemis_file)

    @debug.timer
    def run_forward(self, emissions: Emissions, step: str = None, serial: bool = False) -> Tuple[Path, Path]:

        # Write the emissions. Don't compress when inside a 4dvar loop, for faster speed
        compression = step in self.output_steps

        emf = emissions.to_netcdf(self.emissions_file, zlib=compression, only_transported=True)

        # Write the observations:
        dbf = self.path_temp / 'observations.hdf'
        self.observations.to_hdf(dbf, key='observations')

        # Run the model:
        cmd = list(self.executable) + [
            '--forward',
            '--obs', str(dbf),
            '--emis', str(emf),
            '--tmp', str(self.path_temp),
        ]
        cmd.extend(_expand_path_option('--footprints', self.path_footprints))

        if step == 'apri':
            cmd.append('--check-footprints')
            cmd.extend(_expand_path_option('--copy-footprints', self.path_footprints_scratch))

        if self.serial or serial:
            cmd.append('--serial')

        runcmd(cmd, shell=False)

        return emf, dbf


def _expand_path_option(flag: str, value) -> list[str]:
    """
    Convert either:
      - a single path/string
      - a dict-like mapping such as {"flsk": "/a", "intg": "/b"}
      - an OmegaConf DictConfig
    into CLI arguments.
    """
    if value is None:
        return []

    # Resolve OmegaConf containers first
    if isinstance(value, (DictConfig, ListConfig)):
        value = OmegaConf.to_container(value, resolve=True)

    if isinstance(value, dict):
        items = [f"{k}={Path(v)}" for k, v in value.items()]
        return [flag, *items]

    return [flag, str(Path(value))]


    
