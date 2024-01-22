#!/usr/bin/env python

from datetime import datetime
from argparse import ArgumentParser
from lumia.Tools.rctools import rc
import lumia
from lumia.obsdb.footprintdb import obsdb
from lumia.formatters import lagrange
from lumia.Tools.logging_tools import logger

def Forward(rcf, verbosity='INFO'):

    # Set verbosity level
    logger.setLevel(verbosity)

    # Read config
    rcf = rc(rcf)
    #start = datetime(*rcf.rcfGet('run.time.start'))
    #end = datetime(*rcf.rcfGet('run.time.end'))
    # essentially this meant in older versions of the yml config file:  start=(datetime(2012, 5, 1))) with start being a datetime object
    # now with run.time.start being a Timestamp string this needs to be changed to:
    timestamp_string = rcf.rcfGet('run.time.start') #"2018-01-01 00:00:00"
    format_string = "%Y-%m-%d %H:%M:%S"
    start = datetime.strptime(timestamp_string, format_string)
    timestamp_string = rcf.rcfGet('run.time.end') #"2018-12-31 23:59:59"
    end = datetime.strptime(timestamp_string, format_string)
    logger.debug(f'start={start},  end={end} from run.time.end={timestamp_string}')

    # Read observations
    tracer='co2'
    try:
        if (isinstance(rcf['run']['tracers'], str)):
            tracer=rcf['run']['tracers']
        else:
            trac=rcf['run']['tracers']
            tracer=trac[0]
    except:
        tracer='co2'
    #obsfile = rcf.rcfGet('observations.filename')
    obsfile = rcf.rcfGet(f'observations.{tracer}.file.path')
    db = obsdb(filename=obsfile)
    db.setupFootprints(path=rcf.rcfGet('footprints.path'), cache=rcf.rcfGet('footprints.cache'))

    # Read fluxes
    # TODO: This ought to be dependent on the tracer and Forward(rcf, verbosity='INFO') should have another parameter stating TRACER
    tracers = rcf.rcfGet('run.tracers',  default=['CO2'])
    categories = dict.fromkeys(rcf.rcfGet(f'emissions.{tracers[0]}.categories'))
    for cat in categories :
        categories[cat] = rcf.rcfGet(f'emissions.{tracers[0]}.{cat}.origin')
    emis = lagrange.ReadArchive(rcf.rcfGet(f'emissions.{tracers[0]}.prefix'), start, end, categories=categories)

    # transport model run
    model = lumia.transport(rcf, obs=db, formatter=lagrange)
    model.runForward(struct=emis, step=rcf.rcfGet('tag'))

    return model

if __name__ == '__main__' :

    # Read arguments
    p = ArgumentParser()
    p.add_argument('rc', help="Main configuration file (i.e. rc-file)")
    p.add_argument('--verbosity', help="verbosity of the run (i.e. level of logging). Choose between DEBUG, INFO (default) and WARNING", default='INFO')
    args = p.parse_args()

    fwd = Forward(args.rc, args.verbosity)
