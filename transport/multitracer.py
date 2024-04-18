#!/usr/bin/env python
import os
from loguru import logger
from pandas import Timedelta, Timestamp # , to_numeric
from pandas.api.types import is_float_dtype
from transport.core import Model
from transport.emis import Emissions
#from transport.observations import Observations
from observations import Observations
import h5py
from typing import List
from types import SimpleNamespace
from gridtools import Grid
from numpy import inf
from dataclasses import asdict


class LumiaFootprintFile(h5py.File):
    maxlength : Timedelta = inf
    __slots__ = ['shift_t', 'origin', 'timestep', 'grid']

    def __init__(self, *args, maxlength:Timedelta=inf, **kwargs):
        super().__init__(*args, mode='r', **kwargs)
        self.shift_t = 0

        try :
            # self.origin = Timestamp(self.attrs['origin'])  TypeError: Cannot convert input [['2018-01-01 00:00:00']] of type <class 'numpy.ndarray'> to Timestamp
            # self.origin = Timestamp(self.attrs['origin'])
            # print('multitracer.py, L27, self.attrs[origin]=',  flush=True)
            # print(self.attrs['origin'],  flush=True)
            # strng=str(self.attrs['origin'])
            strng=str(self.attrs['origin']).strip('[]')
            # strng2=strng.strip('[]')
            # print('strng=%s, strng2=%s'%(strng, strng2),  flush=True)
            self.origin = Timestamp(strng)
            self.timestep = Timedelta(seconds=abs(self.attrs['run_loutstep']))
            if self.maxlength != inf :
                self.maxlength /= self.timestep
            assert self['latitudes'].dtype in ['f4', 'f8']
            self.grid = Grid(latc=self['latitudes'][:], lonc=self['longitudes'][:])

        except AssertionError :
            self.grid = Grid(
                lon0=self.attrs['run_outlon0'], dlon=self.attrs['run_dxout'], nlon=len(self['longitudes'][:]),
                lat0=self.attrs['run_outlat0'], dlat=self.attrs['run_dyout'], nlat=len(self['latitudes'][:])
            )

        except KeyError:
            logger.warning(f"Coordinate variables (latitudes and longitudes) not found in file {self.filename}. Is the file empty?")

    @property
    def footprints(self) -> List[str]:
        return [k for k in self.keys() if isinstance(self[k], h5py.Group)]

    def align(self, grid: Grid, timestep: Timedelta, origin: Timestamp):
        try:
            # logger.info(f"grid={grid}")
            logger.info(f"self.grid={self.grid}")
            assert Grid(latc=grid.latc, lonc=grid.lonc) == self.grid, f"Can't align the footprint file grid ({self.grid}) to the requested grid ({Grid(**asdict(grid))})"
        except:
            logger.error("ABORT: error in multitracer.py, assert(grid==self.grid) failed.")
            raise Exception("ABORT: error in multitracer.py, assert(grid==self.grid) failed.")
        try:
            assert timestep == self.timestep, "Temporal grid mismatch"
            shift_t = (self.origin - origin)/timestep
            assert int(shift_t) - shift_t == 0
            self.shift_t = int(shift_t)
        except:
            logger.error("ABORT: error in multitracer.py, assert(timestep==self.timestep) failed.")
            raise Exception("ABORT: error in multitracer.py, assert(timestep==self.timestep) failed.")

    def get(self, obsid) -> SimpleNamespace :
        itims = self[obsid]['itims'][:] 
        ilons = self[obsid]['ilons'][:]
        ilats = self[obsid]['ilats'][:]
        sensi = self[obsid]['sensi'][:]

        if self[obsid]['sensi'].attrs.get('units') == 's m3 kg-1':
            sensi *= 0.0002897

        #if Timestamp(self[obsid]['sensi'].attrs.get('runflex_version', '2000.1.1')) < Timestamp(2022, 9, 1):
        #    sensi *= 0.0002897

        # If the footprint is empty, return here:
        if len(itims) == 0:
            return SimpleNamespace(shift_t=0, itims=itims, ilats=ilats, ilons=ilons, sensi=sensi)

        # sometimes, the footprint will have non-zero sentivity for the time-step directly after the observation time,
        # because FLEXPART calculates the concentration after releasing the particles, and before going to the next step.
        # This causes issues at the end of the simulations, as the corresponding emissions aren't available. The work
        # around below deletes these sensitivity components and re-attribute their values to the time step just before the obs

        # Check if the time of the last time step is same as release time (it should be lower by 1 timestep normally)
        # if it's the case, decrement that time index by 1
        if self.origin + itims[-1] * self.timestep == Timestamp(self[obsid].attrs['release_end']):
            ii = ilons[itims == itims[-1]]
            jj = ilats[itims == itims[-1]]
            s = sensi[itims == itims[-1]]
            sensi[(itims == itims[-1] - 1) & (ilats == jj) & (ilons == ii)] += s
            sensi = sensi[:-len(s)]
            ilons = ilons[:-len(s)]
            ilats = ilats[:-len(s)]
            itims = itims[:-len(s)]

        # Trim the footprint if needed
        sel = itims.max() - itims <= self.maxlength

        # Apply the time shift
        itims += self.shift_t

        # Exclude negative time steps
        if itims.min() < 0 :
            sel *= False

        return SimpleNamespace(
            name=obsid,
            shift_t=self.shift_t,
            itims=itims[sel], 
            ilons=ilons[sel], 
            ilats=ilats[sel], 
            sensi=sensi[sel])


class MultiTracer(Model):
    _footprint_class = LumiaFootprintFile

    @property
    def footprint_class(self):
        return self._footprint_class


if __name__ == '__main__':
    import sys

    from argparse import ArgumentParser, REMAINDER
    logger.debug(f'My execution has been requested: {sys.argv}')
    
    p = ArgumentParser()
    p.add_argument('--setup', action='store_true', default=False, help="Setup the transport model (copy footprints to local directory, check the footprint files, ...)")
    p.add_argument('--forward', '-f', action='store_true', default=False, help="Do a forward run")
    p.add_argument('--adjoint', '-a', action='store_true', default=False, help="Do an adjoint run")
    p.add_argument('--footprints', '-p', help="Path where the footprints are stored")
    p.add_argument('--check-footprints', action='store_true', help='Determine which footprint file correspond to each observation')
    p.add_argument('--copy-footprints', default=None, help="Path where the footprints should be copied during the run (default is to read them directly from the path given by the '--footprints' argument")
    p.add_argument('--adjtest', '-t', action='store_true', default=False, help="Perform an adjoint test")
    p.add_argument('--serial', '-s', action='store_true', default=False, help="Run on a single CPU")
    p.add_argument('--tmp', default='/tmp', help='Path to a temporary directory where (big) files can be written')
    p.add_argument('--ncpus', '-n', default=os.cpu_count())
    p.add_argument('--max-footprint-length', type=Timedelta, default='14D')
    p.add_argument('--verbosity', '-v', default='INFO')
    p.add_argument('--obs', required=True)
    p.add_argument('--emis')#, required=True)
    p.add_argument('--outpPathPrfx', '-o', help="Value of the run.thisRun.uniqueTmpPrefix key from the Lumia config yml file.", required=False)
    p.add_argument('args', nargs=REMAINDER)
    bTryagain=True

   
    # I have seen this goofing up without producing a proper error - sometimes the first argument seems to contain unwanted information - try chucking these out...
    iSkip=0
    while((bTryagain) and (iSkip<5)):
        try:
            args = p.parse_args(sys.argv[iSkip:])
            bTryagain=False
        except:
            iSkip=iSkip+1
    if((bTryagain) or (iSkip>4)):
        logger.error('CRITICAL ERROR: subprocess failed: lumia.transport.multitracer() invalid arguments passed. The Forward/Adjoint/Adjtest transport model was NOT run.')
        raise RuntimeError('CRITICAL ERROR: subprocess failed: lumia.transport.multitracer() invalid arguments passed. The Forward/Adjoint/Adjterst model was NOT run.')
    else:
        logger.info('If you got the error >>multitracer.py: error: the following arguments are required: --obs<< but no CRITICAL ERROR, then you can ignore it safely. If you read this, then the contingency plan worked.')
    # Set the verbosity in the logger (loguru quirks ...)
    logger.remove()
    logger.add(sys.stderr, level=args.verbosity)

    mmode='notSpecified'
    if args.forward:
        mmode='ForwardRun'
    elif args.adjoint :
        mmode='AdjointRun'
    elif args.adjtest :
        mmode='AdjTestRun'
    if('notSpecified' in mmode):
        logger.error('CRITICAL ERROR: subprocess failed: lumia.transport.multitracer() Operation mode (Forward/Adjoint/Adjtest) not specified. The transport model was NOT run.')
        raise RuntimeError('CRITICAL ERROR: subprocess failed: lumia.transport.multitracer() Operation mode (Forward/Adjoint/Adjtest) not specified. The transport model was NOT run.')

    if((args.obs is None) or (len(args.obs)<6)):
        logger.error(f'CRITICAL ERROR: subprocess failed: lumia.transport.multitracer() Option --obs not provided. The {mmode} transport model was NOT run.')
        raise RuntimeError('CRITICAL ERROR: subprocess failed: lumia.transport.multitracer() Option --obs not provided. The {mmode} transport model was NOT run.')

    outpPathPrfx=None
    if not(args.outpPathPrfx is None):
        outpPathPrfx=args.outpPathPrfx
    if((args.outpPathPrfx is None) or (len(args.outpPathPrfx)<6)):
        outpPathPrfx=''
        # try to derive it from the obs argument --obs ./tmp/LumiaDA-2024-01-19T01_40/LumiaDA-2024-01-19T01_40-departures.hdf
        if (len(args.obs) > 16):
            sDir=os.path.dirname(args.obs)
            c=os.path.sep
            if len(sDir.split(os.path.sep)) > 1:             
                sp=sDir.split(os.path.sep)
                uniqid=sp[-1]  # should now hold the name of the lowest subdirectory, e.g. LumiaDA-2024-01-19T01_40
                outpPathPrfx=sDir+os.path.sep+uniqid+'-'
    if((outpPathPrfx is None) or (len(outpPathPrfx)<6)):
        logger.error(f'CRITICAL ERROR: subprocess failed: lumia.transport.multitracer() --outpPathPrfx not provided nor could it be derived from the --obs argument. The {mmode} transport model was NOT run.')
        raise RuntimeError('CRITICAL ERROR: subprocess failed: lumia.transport.multitracer() invalid arguments passed. The Forward/Adjoint/Adjterst transport model was NOT run.')
        
    obs = Observations.read(args.obs)
    #  We need to ensure that all columns containing float values are perceived as such and not as object or string dtypes -- or havoc rages down the line
    knownColumns=['stddev', 'obs','err_obs', 'err', 'lat', 'lon', 'alt', 'height', 'background', 'mix_fossil', 'mix_biosphere', 'mix_ocean', 'mix_background', 'mix']
    for col in knownColumns:
        if col in obs.columns:
            if(is_float_dtype(obs[col])==False):
                obs[col]=obs[col].astype(float)
    
    # Set the max time limit for footprints:
    LumiaFootprintFile.maxlength = args.max_footprint_length

    sOutpPrfx=args.outpPathPrfx
    if (sOutpPrfx is None):
        sOutpPrfx=""
    if args.check_footprints or 'footprint' not in obs.columns:
        obs.check_footprints(args.footprints, LumiaFootprintFile, local=args.copy_footprints,  sOutpPrfx=sOutpPrfx)
    
        #if(os.path.isfile(sOutpPrfx+"missing-footprint-files.csv")):
            # Call runflex to create the missing footprint files
            
            # Discover the newly created footprint files
            #obs.check_footprints(args.footprints, LumiaFootprintFile, local=args.copy_footprints,  sOutpPrfx=sOutpPrfx)

    model = MultiTracer(parallel=not args.serial, ncpus=args.ncpus, tempdir=args.tmp)
    emis = Emissions.read(args.emis)  # goes to lumia.transport.emis.init_.read() and reads  self.rcf[ 'run']['thisRun']['uniqueTmpPrefix']+emissions.nc
    if args.forward:
        logger.debug(f'lumia.transport.multitracer.main model.run_forward(obs, emis) with obs={obs} ')
        logger.debug(f'lumia.transport.multitracer.main model.run_forward(obs, emis) with emis={emis} ')
        obs = model.run_forward(obs, emis)  # goes to lumiatransport.core.model.run_forward() -> run() -> run_tracer() 
        logger.debug(f'lumia.transport.multitracer.main writing obs = model.run_forward(obs, emis) via obs.write({args.obs}) ')
        obs.write(args.obs)
        logger.info('transport.multitracer (subprocess): Forward run completed successfully.')

    elif args.adjoint :
        adj = model.run_adjoint(obs, emis)
        adj.write(args.emis)
        logger.info('transport.multitracer (subprocess): Adjoint run completed successfully.')

    elif args.adjtest :
        model.adjoint_test(obs, emis)
        logger.info('transport.multitracer (subprocess): Adjtest run completed successfully!')
