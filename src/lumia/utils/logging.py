#!/usr/bin/env python
import sys
from loguru import logger


def setup_logging(verbosity: str='INFO'):
    logger.remove()
    logger.add(
        sys.stdout,
        format='<g>{elapsed}</> | <level>{level: <8}</level> | <c>{file.path}</>:<c>{line})</> | {message}',
        level=verbosity
    )
