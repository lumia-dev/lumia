#!/usr/bin/env python
import logging
from tqdm.autonotebook import tqdm
import colorlog
import shutil
columns = shutil.get_terminal_size().columns

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

handler2 = TqdmHandler()
handler2.setFormatter(formatter)

#log.addHandler(handler)
log.addHandler(handler2)
log.setLevel(logging.INFO)
