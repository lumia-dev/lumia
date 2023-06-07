#!/usr/bin/env python
import os
from loguru import logger
from transport.core import Model
from transport.emis import Emissions
from transport.observations import Observations
from transport.files.lumia import LumiaFootprintFile
from transport.files.stilt import StiltFootprintFile
from pandas import Timedelta


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
    p.add_argument('--ncpus', '-n', default=os.cpu_count())
    p.add_argument('--max-footprint-length', type=Timedelta, default='14D')
    p.add_argument('--verbosity', '-v', default='INFO')
    p.add_argument('--obs', required=True)
    p.add_argument('--emis')#, required=True)
    p.add_argument('args', nargs=REMAINDER)
    args = p.parse_args(sys.argv[1:])

    # Set the verbosity in the logger (loguru quirks ...)
    logger.remove()
    logger.add(sys.stderr, level=args.verbosity)

    obs = Observations.read(args.obs)

    # Set the max time limit for footprints:
    LumiaFootprintFile.maxlength = args.max_footprint_length

    if args.check_footprints or 'footprint' not in obs.columns:
        obs.check_footprints(args.footprints, LumiaFootprintFile, local=args.copy_footprints)

    model = MultiTracer(parallel=not args.serial, ncpus=args.ncpus, tempdir=args.tmp)
    emis = Emissions.read(args.emis)
    if args.forward:
        obs = model.run_forward(obs, emis)
        obs.write(args.obs)

    elif args.adjoint :
        adj = model.run_adjoint(obs, emis)
        adj.write(args.emis)

    elif args.adjtest :
        model.adjoint_test(obs, emis)
