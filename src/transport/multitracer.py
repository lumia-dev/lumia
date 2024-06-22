#!/usr/bin/env python
import os
import sys
from loguru import logger
from transport.core import Model
from transport.emis import Emissions
from transport.observations import Observations
from transport.files.lumia import LumiaFootprintFile
from transport.files.stilt import StiltFootprintFile
from transport.concentrations import read_conc_file
from pandas import Timedelta
from pathlib import Path

from lumia.utils.housekeeping import setupLogging


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
    from argparse import ArgumentParser, REMAINDER

    p = ArgumentParser()
    p.add_argument('--setup', action='store_true', default=False, help="Setup the transport model (copy footprints to local directory, check the footprint files, ...)")
    p.add_argument('--forward', '-f', action='store_true', default=False, help="Do a forward run")
    p.add_argument('--adjoint', '-a', action='store_true', default=False, help="Do an adjoint run")
    p.add_argument('--footprints', '-p', help="Path where the footprints are stored")
    p.add_argument('--check-footprints', action='store_true', help='Determine which footprint file correspond to each observation')
    p.add_argument('--copy-footprints', default=None, help="Path where the footprints should be copied during the run (default is to read them directly from the path given by the '--footprints' argument")
    p.add_argument('--adjtest', '-t', action='store_true', default=False, help="Perform and adjoint test")
    p.add_argument('--serial', '-s', action='store_true', default=False, help="Run on a single CPU")
    p.add_argument('--tmp', default='/tmp', help='Path to a temporary directory where (big) files can be written')
    p.add_argument('--ncpus', '-n', default=os.cpu_count(), type=int)
    p.add_argument('--max-footprint-length', type=Timedelta, default='14D')
    p.add_argument('--verbosity', '-v', default='INFO')
    p.add_argument('--background', '-b', type=str, nargs='*', default=None, help="Path or glob pattern pointing to concentrations files to use as background (files should be in the CAMS format). If a 'mix_background' field is present in the observations, the backgrounds won't be re-interpolated")
    p.add_argument('--obs', required=True)
    p.add_argument('--emis')#, required=True)
    p.add_argument('--outpPathPrfx', '-o', default='', help="Value of the run.thisRun.uniqueOutputPrefix key from the Lumia config yml file.", required=False)
    p.add_argument('args', nargs=REMAINDER)
    args = p.parse_args(sys.argv[1:])

    # Set the verbosity in the logger (loguru quirks ...)
    #logger.remove()
    #logger.add(sys.stderr, level=args.verbosity)
    #logger.debug('test')
    log_level=args.verbosity # 'DEBUG'
    sOutputPrfx=args.outpPathPrfx
    logPath=f'{sOutputPrfx}multitracer.log'
    i=1
    scriptName='multitracer'
    while(os.path.isfile(logPath)):
        logPath=f'{sOutputPrfx}multitracer-{i}.log'
        scriptName=f'multitracer-{i}'
        i+=1
    setupLogging(log_level,  scriptName, sOutputPrfx,  logPath,  cleanSlate=False)
    logger.info(f'{args}')  # document how multitracer.py was called including commandline options

    obs = Observations.read(args.obs)

    # Set the max time limit for footprints:
    LumiaFootprintFile.maxlength = args.max_footprint_length

    # Optional: detect footprints
    if args.check_footprints or 'footprint' not in obs.columns:
        logger.info(f'Searching footprints in {args.footprints}')
        obs.check_footprints(args.footprints, LumiaFootprintFile, local=args.copy_footprints)

    # Optional: interpolate a background field:
    if args.background:# and 'mix_background' not in obs.columns:
        logger.info(f'Interpolating backgrounds from {args.background}')
        logger.debug(args.background)
        bg = read_conc_file(args.background)
        obs.interp_background(bg, LumiaFootprintFile)
        if not args.forward or args.adjoint or args.adjtest:
            obs.write(args.obs)

    model = MultiTracer(parallel=not args.serial, ncpus=args.ncpus, tempdir=args.tmp)

    emis = Emissions.read(args.emis)

    if args.forward:
        obs = model.run_forward(obs, emis)
        logger.info('forward model completed successfully. Writing results...')
        obs.write(args.obs)

    elif args.adjoint :
        adj = model.run_adjoint(obs, emis)
        logger.info('adjoint model completed successfully. Writing results...')
        adj.write(args.emis)

    elif args.adjtest :
        model.adjoint_test(obs, emis)
        logger.info('adjoint test model completed successfully.')
    logger.info('multitracer.py subprocess completed successfully.')
