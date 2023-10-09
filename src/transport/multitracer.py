#!/usr/bin/env python
import os
from loguru import logger
from transport.core import Model
from transport.emis import Emissions
from transport.observations import Observations
from transport.files.lumia import LumiaFootprintFile
from transport.files.stilt import StiltFootprintFile
from transport.concentrations import read_conc_file
from pandas import Timedelta
from pathlib import Path
from omegaconf import OmegaConf, DictConfig
from dataclasses import dataclass
from argparse import ArgumentParser


@dataclass(kw_only=True)
class Settings:
    obs : Path = None    # Path to the observations file
    emis : Path = None   # Path to the emissions file
    setup : bool = False # Setup the transport model (copy footprints to local directory, check the footprint files, etc.)
    forward : bool = False  # Do a forward run
    adjoint : bool = False  # Do an adjoint run
    adjtest : bool = False  # Do an adjoint test
    serial : bool = False   # Set to True to run on a single CPU
    footprints : str | DictConfig = None    # Path where the footprints are stored
    check_footprints : bool = False # Whether to check the footprint files for actual footprints
    copy_footprints : str = None    # Path where the footprints should be copied during the run
    tmp : Path = None               # Path to a temporary directory where (big) files can be written
    ncpus : int = os.cpu_count()    # Number of cores to use for parallelization
    max_footprint_length : str = '14D' # max duration of the footprints
    verbosity : str = 'INFO'        # Level of the logger
    background : str = None         # Path or glob pattern pointing to concentrations files to use as background (files should be in the CAMS format). If a 'mix_background' field is present in the observations, the backgrounds won't be re-interpolated

    def read(self, filename : Path):
        for k, v in OmegaConf.load(filename).items():
            setattr(self, k, v)

    def update(self, args : dict, defaults : ArgumentParser = None):
        for k, v in args.items():
            if v != defaults.get_default(k):
                setattr(self, k, v)


class MultiTracer(Model):
    _footprint_class = LumiaFootprintFile

    @property
    def footprint_class(self):
        return self._footprint_class


class Stilt(Model):
    _footprint_class = StiltFootprintFile

    @property
    def footprint_class(self):
        return self._footprint_class


if __name__ == '__main__':
    import sys

    p = ArgumentParser()
    p.add_argument('--setup', action='store_true', help="Setup the transport model (copy footprints to local directory, check the footprint files, ...)")
    p.add_argument('--forward', '-f', action='store_true', help="Do a forward run")
    p.add_argument('--adjoint', '-a', action='store_true', help="Do an adjoint run")
    p.add_argument('--footprints', '-p', help="Path where the footprints are stored")
    p.add_argument('--check-footprints', action='store_true', help='Determine which footprint file correspond to each observation')
    p.add_argument('--copy-footprints', default=None, help="Path where the footprints should be copied during the run (default is to read them directly from the path given by the '--footprints' argument")
    p.add_argument('--adjtest', '-t', action='store_true', default=False, help="Perform and adjoint test")
    p.add_argument('--serial', '-s', action='store_true', default=False, help="Run on a single CPU")
    p.add_argument('--tmp', default=Path('/tmp'), help='Path to a temporary directory where (big) files can be written', type=Path)
    p.add_argument('--ncpus', '-n', default=os.cpu_count(), type=int)
    p.add_argument('--max-footprint-length', type=str, default='14D')
    p.add_argument('--verbosity', '-v', default='INFO')
    p.add_argument('--background', '-b', type=str, nargs='*', default=None, help="Path or glob pattern pointing to concentrations files to use as background (files should be in the CAMS format). If a 'mix_background' field is present in the observations, the backgrounds won't be re-interpolated")
    p.add_argument('--obs', default=None)
    p.add_argument('--emis', default=None)
    p.add_argument('config', type=Path, help="Path to the config file (yaml format)", default=None, nargs='?')
    args = p.parse_args(sys.argv[1:])


    # Parse settings
    settings = Settings()               # Initialize with default values
    if args.config is not None :        # If a config file (yaml) is provided, read it
        settings.read(args.config)
    settings.update(vars(args), defaults=p) # command line arguments superseed both the default values and the config file
    
    # Set the verbosity in the logger (loguru quirks ...)
    logger.remove()
    logger.add(sys.stderr, level=settings.verbosity)


    obs = Observations.read(settings.obs)


    # Set the max time limit for footprints:
    LumiaFootprintFile.maxlength = Timedelta(settings.max_footprint_length)


    # Optional: detect footprints
    if settings.check_footprints or 'footprint' not in obs.columns:
        logger.info(f'Searching footprints in {settings.footprints}')
        obs.check_footprints(settings.footprints, LumiaFootprintFile, local=settings.copy_footprints)


    # Optional: interpolate a background field:
    if settings.background:# and 'mix_background' not in obs.columns:
        logger.info(f'Interpolating backgrounds from {settings.background}')
        logger.debug(settings.background)
        bg = read_conc_file(settings.background)
        obs.interp_background(bg, LumiaFootprintFile)
        if not settings.forward or settings.adjoint or settings.adjtest:
            obs.write(settings.obs)


    model = MultiTracer(parallel=not settings.serial, ncpus=settings.ncpus, tempdir=settings.tmp)


    emis = Emissions.read(settings.emis)


    if settings.forward:
        obs = model.run_forward(obs, emis)
        obs.write(settings.obs)


    elif settings.adjoint :
        adj = model.run_adjoint(obs, emis)
        adj.write(settings.emis)


    elif settings.adjtest :
        model.adjoint_test(obs, emis)
