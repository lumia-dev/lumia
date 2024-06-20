#!/usr/bin/env python
import sys
from loguru import logger
import re
import time
import sys



def   setupLogging(log_level,  parentScript, sOutputPrfx,  logName:str='-run.log',  cleanSlate=True):
    if(cleanSlate):
        logger.remove()
    logger.add(
        sys.stdout,
        format='<green>{time:YYYY-MM-DD HH:mm:ss.SSS zz}</green> | <g>{elapsed}</> | <level>{level: <8}</level> | <c>{file.path}</>:<c>{line})</> | {message}',  #<blue><c>{file.path}</>:<c>{line}</blue>)</> | {message}',
        level= log_level, colorize=True, backtrace=True, diagnose=True
    )
    logFile=sOutputPrfx+parentScript+logName
    logger.info(f'A log file is written to {logFile}.')
    logger.add(
        logFile,
        format='{time:YYYY-MM-DD HH:mm:ss.SSS zz} | elapsed time: {elapsed} | {level: <8} | {file.path}:L.{line}) | {message}',
        level= log_level, colorize=True, backtrace=True, diagnose=True, rotation="5 days"
    )


import logging

def set_colour(level):
    """
    Sets colour of text for the level name in
    logging statements using a dispatcher.
    """
    escaped = "[\033[1;%sm%s\033[1;0m]"
    return {
        'DEBUG': lambda: logging.addLevelName(logging.INFO, escaped % ('95', level)),
        'INFO': lambda: logging.addLevelName(logging.INFO, escaped % ('94', level)),
        'WARNING': lambda: logging.addLevelName(logging.ERROR, escaped % ('93', level)),
        'ERROR': lambda: logging.addLevelName(logging.WARNING, escaped % ('91', level))
    }.get(level, lambda: None)()


class NoColorFormatter(logging.Formatter):
    """
    Log formatter that strips terminal colour
    escape codes from the log message.
    """

    # Regex for ANSI colour codes
    ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

    def format(self, record):
        """Return logger message with terminal escapes removed."""
        return "%s %s %s" % (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            re.sub(self.ANSI_RE, "", record.levelname),
            record.msg,
        )

# Create logger
logger = logging.getLogger(__package__)

# Create formatters
logformatter = NoColorFormatter()
colorformatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

# Set logging colours
for level in 'DEBUG','INFO', 'ERROR', 'WARNING':
    set_colour(level)

# Set logging level
logger.setLevel(logging.INFO)

# Set log handlers
loghandler = logging.FileHandler("log.txt", mode="w", encoding="utf8")
streamhandler = logging.StreamHandler(sys.stdout)

# Set log formatters
loghandler.setFormatter(logformatter)
streamhandler.setFormatter(colorformatter)

# Attach log handlers to logger
logger.addHandler(loghandler)
logger.addHandler(streamhandler)

# Example logging statements
logging.info("This is just an information for you")
logging.warning("This is just an information for you")
logging.error("This is just an information for you")
