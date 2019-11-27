#!/usr/bin/env python
import logging

import colorlog
import shutil
columns = shutil.get_terminal_size().columns

log = logging.getLogger()
handler = colorlog.StreamHandler()
formatter = colorlog.ColoredFormatter(
    "%(message_log_color)s %(name)40s | %(reset)s %(log_color)s %(levelname)-8s (line %(lineno)d) | %(reset)s %(message)s",
    datefmt=None,
    reset=True,
    log_colors={
        'DEBUG': 'blue',
        'INFO': 'cyan',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'purple'},
    secondary_log_colors={'message': {
        'DEBUG': 'bold_blue',
        'INFO': 'bold_cyan',
        'WARNING': 'bold_yellow',
        'ERROR': 'bold_red',
        'CRITICAL': 'white,bg_purple'}},

)
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.INFO)