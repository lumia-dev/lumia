#!/usr/bin/env python
import logging
from tqdm.autonotebook import tqdm
import colorlog
import shutil
columns = shutil.get_terminal_size().columns

def colorize(msg, color=None):
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
    msg = msg.replace('<ybg>', '\x1b[0;43m')
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
    return msg

class TqdmHandler(colorlog.StreamHandler):
    def __init__(self):
        colorlog.StreamHandler.__init__(self)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)  # , file=sys.stderr)
       #     self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

log = logging.getLogger()
handler = colorlog.StreamHandler()
formatter = colorlog.ColoredFormatter(
    "%(message_log_color)s %(name)30s | %(reset)s %(log_color)s %(levelname)-8s (line %(lineno)d) | %(reset)s %(message)s",
    datefmt=None,
    reset=True,
    log_colors={
        'DEBUG': 'purple',
        'INFO': 'cyan',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'white,bg_red'},
    secondary_log_colors={'message': {
        'DEBUG': 'bold_blue',
        'INFO': 'bold_cyan',
        'WARNING': 'bold_yellow',
        'ERROR': 'red',
        'CRITICAL': 'white,bg_red'}},

)
handler.setFormatter(formatter)

handler2 = TqdmHandler()
handler2.setFormatter(formatter)

#log.addHandler(handler)
log.addHandler(handler2)
log.setLevel(logging.INFO)
