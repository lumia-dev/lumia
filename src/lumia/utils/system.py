#!/usr/bin/env python

import subprocess
from loguru import logger


def colorize(msg, color=None):
    if color is not None :
        msg = f'<{color}>{msg}</{color}>'
    # grey :
    msg = msg.replace('<k>', '\x1b[0;30m')
    msg = msg.replace('</k>', '\x1b[0m')
    # red :
    msg = msg.replace('<r>', '\x1b[0;31m')
    msg = msg.replace('</r>', '\x1b[0m')
    # Green
    msg = msg.replace('<g>', '\x1b[0;32m')
    msg = msg.replace('</g>', '\x1b[0m')
    # Yellow
    msg = msg.replace('<y>', '\x1b[0;33m')
    msg = msg.replace('</y>', '\x1b[0m')
    #msg = msg.replace('<ybg>', '\x1b[0;43m')
    # Blue
    msg = msg.replace('<b>', '\x1b[0;34m')
    msg = msg.replace('</b>', '\x1b[0m')
    # Magenta
    msg = msg.replace('<m>', '\x1b[0;35m')
    msg = msg.replace('</m>', '\x1b[0m')
    # Cyan
    msg = msg.replace('<c>', '\x1b[0;36m')
    msg = msg.replace('</c>', '\x1b[0m')
    # White
    msg = msg.replace('<w>', '\x1b[0;37m')
    msg = msg.replace('</w>', '\x1b[0m')

    # Bold
    msg = msg.replace('<s>', '\x1b[1m')
    msg = msg.replace('</s>', '\x1b[22m')
    # Italic
    msg = msg.replace('<i>', '\x1b[3m')
    msg = msg.replace('</i>', '\x1b[23m')
    # Underlined
    msg = msg.replace('<u>', '\x1b[4m')
    msg = msg.replace('</u>', '\x1b[24m')
    return msg


def runcmd(cmd, shell : bool = False):
    cmdstr = ' '.join([str(x) for x in cmd])
    logger.info(colorize(cmdstr, 'm'))
    if shell :
        cmd = cmdstr
    try :
        # p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        p = subprocess.run(cmd, shell=shell)
    except subprocess.CalledProcessError :
        logger.error("external command failed, exiting ...")
        raise subprocess.CalledProcessError
    logger.debug(f'return value from subprocess.run({cmd} is p={p}.')
    # for line in p.stdout:
    #     sys.stdout.buffer.write(line)
    #     sys.stdout.buffer.flush()
