#!/usr/bin/env python3

import sys
import os
import shutil
import subprocess
from argparse import ArgumentParser
import configparser
import logging

logger = logging.getLogger(__name__)

p = ArgumentParser()
authorized_commands = ['python3', 'ipython3', 'bash']
p.add_argument('action', choices=['run', 'extract', 'install']+authorized_commands)
p.add_argument('--bin', default=os.path.join(os.environ['HOME'], '.local/bin'))
p.add_argument('--scratch', default=None)
p.add_argument('--footprints', default=None)
p.add_argument('--output', default=None)
p.add_argument('--dest', default=os.environ['SINGULARITY_CONTAINER'])
p.add_argument('--extra-mount-path', action='append', dest='extras')
args, remainder = p.parse_known_args(sys.argv[1:])

if args.action == 'extract':
    shutil.copytree('/runflex', remainder[0], dirs_exist_ok=True)

elif args.action in authorized_commands :
    subprocess.run([args.action] + remainder)

elif args.action == 'run':
    subprocess.run(['python3', '/lumia/singularity/run.py'] + remainder)

elif args.action == 'install':
    # Copy the lumia script to the host ~/.local/bin
    shutil.copy('/lumia/singularity/lumia', args.bin)

    # Move the container to a new destination if needed
    if args.dest != os.environ['SINGULARITY_CONTAINER']:
        shutil.move(os.environ['SINGULARITY_CONTAINER'], args.dest)

    # Generate the lumia.ini script
    inifile = os.path.join(os.environ['HOME'], '.config/lumia.ini')
    config = {'DefaultContainer': args.dest}
    for dest, source in {'footprints':args.footprints, 'scratch':args.scratch, 'output':args.output}.items():
        if source is not None:
            config[f'/{dest}'] = source
        else :
            logger.warning(f"No default bind path for /{dest} specified. Edit your {inifile} manually or specify it at run time (e.g., singularity run --bind /path/to/{dest}:/{dest} lumia.sif")

    if 'extras' in args :
        config['extras'] = {}
        for extra in args.extras :
            external, internal = extra.split(':')
            config['extras'][external] = internal

    c = configparser.ConfigParser()
    c['runflex'] = config
    with open(os.path.join(inifile), 'w') as cf :
        c.write(cf)