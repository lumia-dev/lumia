#!/usr/bin/env python3

import sys
from loguru import logger
import lumia
from lumia import ui


if __name__ == '__main__':

    args = ui.apptainer_wrapper(sys.argv[1:])

    args = ui.parse_args(args)

    # Most basic case: just run lumia
    logger.remove()
    logger.add(sys.stderr, level=args.verbosity, backtrace=False, diagnose=True)

    # Read and append configuration file
    rcf = ui.parse_config(args)

    # Adjust the paths in memory
    #    lumia.paths.setup(rcf)

    match args.action:
        case 'forward':
            results = ui.forward(rcf)

        case 'optim':
            results = ui.optimize(rcf)

        case 'adjtest':
            results = ui.adjtest(rcf)

        case 'adjtestmod':
            results = ui.adjtestmod(rcf)

        case 'gradtest':
            results = ui.gradtest(rcf)

        case 'emis':
            emis = ui.prepare_emis(rcf)

        case 'validate':
            results = ui.validate(rcf)