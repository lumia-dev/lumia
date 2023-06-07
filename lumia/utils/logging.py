#!/usr/bin/env python
import logging
from tqdm import tqdm
import shutil
from .system import colorize


columns = shutil.get_terminal_size().columns


try :
    import colorlog
    base_handler = colorlog.StreamHandler
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
except ModuleNotFoundError :
    base_handler = logging.StreamHandler
    formatter = logging.Formatter(
        "%(name)30s | %(levelname)-8s (line %(lineno)d) | %(message)s",
        datefmt=None
    )


class TqdmHandler(base_handler):
    def __init__(self):
        super().__init__()

    def emit(self, record):
        try:
            msg = colorize(self.format(record))
            tqdm.write(msg)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            self.handleError(record)

#handler = hl()
#handler.setFormatter(formatter)


handler = TqdmHandler()
handler.setFormatter(formatter)

#log.addHandler(handler)
logger = logging.getLogger()
logger.addHandler(handler)

