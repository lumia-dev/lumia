#!/usr/bin/env python
from abc import ABC, abstractmethod
from functools import partial
from numpy import array, argsort, dot, finfo, ndarray, zeros, arange, nonzero, ndarray
from typing import List, Protocol, Type
from tqdm import tqdm
from loguru import logger
from multiprocessing import Pool, cpu_count, shared_memory
from dataclasses import dataclass
from h5py import File
import tempfile
import os
from pandas import Timestamp, Timedelta, DataFrame
from pandas import DataFrame as Observations
from transport.emis import EmissionFields, Emissions, Grid
from lumia.utils import debug


class Footprint(Protocol):
    itims : ndarray
    ilats : ndarray
    ilons : ndarray
    sensi : ndarray


class FootprintFile(Protocol):
    footprints : List[Footprint]

    def __getitem__(self, obsid: str) -> Footprint: ...

    def align(self, grid: Grid, timestep: Timedelta, origin: Timestamp) -> int: ...

    def get_endpoints(self, obsids: List[str]) -> DataFrame :
        """
        This method should return these endpoints as a DataFrame, with
        the following columns: 
            - lon : float
            - lat : float
            - height : float
            - time : Timestamp
            - obsid : str
        If background info (trajectories endpoints) is not present in the file, an error should be raised.
        """
        ...
     
        
@dataclass
class SharedMemory:
    footprint_class: Type[FootprintFile] = None
    emis: EmissionFields = None
    obs: Observations = None
    grid: Grid = None

    def clear(self, *args):
        if len(args) == 0:
            args = self.__dataclass_fields__
        for k in args:
            setattr(self, k, None)


shared_mem = SharedMemory()


@dataclass
class BaseTransport:
    footprint_class: Type[FootprintFile]
    parallel: bool = False
    ncpus: int = cpu_count()
    tempdir: str='/tmp'
    _silent: bool = None

    def __post_init__(self):
        shared_mem.footprint_class = self.footprint_class

    @property
    def silent(self):
        silent = self._silent if self._silent is not None else self.parallel
        return silent

    def run_files(self, *args, **kwargs) :
        if self.parallel :
            return self.run_files_mp(*args, **kwargs)
        else :
            return self.run_files_serial(*args, **kwargs)

    @abstractmethod
    def run_files_mp(self, *args, **kwargs):
        pass

    @abstractmethod
    def run_files_serial(self, *args, **kwargs):
        pass


@dataclass
class Forward(BaseTransport):

    def run(self, emis: Emissions, obs: Observations) -> Observations :
        # Loop over the tracers:
        # The rational is that 2 tracers will likely have different set of footprints, while two categories for one tracer will share the same footprints
        for tracer in emis.tracers :

            obs = obs.loc[obs.tracer == tracer.tracer].copy()
            fwd = self.run_tracer(tracer, obs)

            # Combine :
            for col in [col for col in fwd.columns if col.startswith('mix')]:
                obs.loc[obs.index, col] = fwd.loc[:, col]

        return obs

    def run_tracer(self, emis: EmissionFields, obs: Observations) -> Observations:

        # Retrieve the observations for that tracer, and their footprint file name:
        filenames = obs.footprint.dropna().drop_duplicates()

        # To optimize CPU usage in parallell simulations, process the largest files first
        nobs = array([obs.loc[obs.footprint == f].shape[0] for f in filenames])
        filenames = [filenames.values[i] for i in argsort(nobs)[::-1]]

        shared_mem.emis = emis
        shared_mem.obs = obs

        for obslist in self.run_files(filenames):
            for field in emis.categories:
                obs.loc[obslist.index, f'mix_{field}'] = obslist.loc[:, f'mix_{field}']#.astype(float)

        shared_mem.clear('emis', 'obs')

        # Combine the flux components :
        try:
            obs.loc[:, 'mix'] = obs.mix_background.copy()
        except AttributeError:
            logger.warning(f'Missing background concentrations for tracer {emis.tracer}. Setting mix_background to 0')
            obs.loc[:, 'mix_background'] = 0
            obs.loc[:, 'mix'] = 0.
        for cat in emis.categories:
            obs.loc[:, 'mix'] += obs.loc[:, f'mix_{cat}'].values

        return obs

    def run_files_serial(self, filenames: List[str]) -> List[Observations]:
        res = []
        for filename in tqdm(filenames):
            res.append(self.run_file(filename, silent=self.silent))
        return res

    def run_files_mp(self, filenames: List[str]) -> List[Observations]:
        with Pool(processes=self.ncpus) as pool:
            res = list(tqdm(pool.imap(self.run_file, filenames, chunksize=1), total=len(filenames), leave=False))
        return res

    @staticmethod
    def run_file(filename: str, silent: bool = True) -> Observations:
        """
        Do a forward run on the selected footprint file. Set silent to False to enable progress bar
        """
        obslist = shared_mem.obs
        obslist = obslist.loc[obslist.footprint == filename, ['obsid',]]
        emis = shared_mem.emis
        with shared_mem.footprint_class(filename) as fpf :

            # Align the coordinates
            fpf.align(emis.grid, emis.times.timestep, emis.times.min)

            for iobs, obs in tqdm(obslist.itertuples(), desc=fpf.filename, total=obslist.shape[0], disable=silent):
                fp = fpf.get(obs)
                for cat in emis.categories :
                    obslist.loc[iobs, f'mix_{cat}'] = (emis[cat].data[fp.itims, fp.ilats, fp.ilons] * fp.sensi).sum()
        return obslist


class Adjoint(BaseTransport):
    @debug.timer
    def run(self, adj_emis: Emissions, obs: Observations) -> Emissions :
        # Create an empty adjoint structure :
        for adj in adj_emis.tracers:

            # Prepare observations:
            obs = obs.loc[obs.tracer == adj.tracer].copy()

            # Run adjoint just for one category
            adj_emis[adj.tracer] = self.run_tracer(adj, obs)

        return adj_emis

    def run_tracer(self, adjemis: EmissionFields, obs: Observations) -> EmissionFields :

        # Retrieve the observations for that tracer, and their footprint file name:
        # Sort the files by the number of obs they contain (largest first)
        filenames = obs.footprint.dropna().drop_duplicates()
        nobs = array([obs.loc[obs.footprint == f].shape[0] for f in filenames])
        filenames = [filenames.values[i] for i in argsort(nobs)[::-1]]

        # Run the separate chunks
        shared_mem.obs = obs

        # Set the current data to 0:
        adjemis.setzero()

        # Get the shape of the adjoint field, store it in memory and create a new container for the data
        shared_mem.grid = adjemis.grid
        shared_mem.time = adjemis.times

        for adjfile in tqdm(self.run_files(filenames), desc='Concatenate adjoint files', leave=self.silent):
            with File(adjfile, 'r') as ds :
                coords = ds['coords'][:]
                values = ds['values'][:]
                for cat in adjemis.categories :
                    adjemis[cat].data.reshape(-1)[coords] += values
            os.remove(adjfile)

        # # Attempt of a new implementation using the multiprocessing.shared_memory module:
        # adjfield = adjemis[adjemis.categories[0]].data                                          # Just get the first category for reference (shape, size and dtype)
        # shm = shared_memory.SharedMemory(name='adjfield', create=True, size=adjfield.nbytes)    # Create the shared memory object
        # adjfield = ndarray(adjfield.shape, dtype=adjfield.dtype, buffer=shm.buf)                # Populate it with a numpy array
        # adjfield[:] = 0.                                                                        # Ensure the array is initialized with zeros
         
        # _ = tqdm(self.run_files(filenames), desc='Calculate adjoint chunks', leave=self.silent) # The subprocesses will then add data to it
        # for cat in adjemis.categories :
        #     #if adjemis[cat].optimized:
        #     adjemis[cat].data[:] = adjfield[:]
        #     #else :
        #     #    del adjemis[cat]
        # shm.close()
        shared_mem.clear('grid', 'time', 'obs')
        return adjemis

    def run_files_serial(self, filenames: List[str]) -> List[str]:
        return [self.run_subset(filenames, silent=self.silent)]

    def run_files_mp(self, filenames: List[str]) -> List[str]:

        # Distribute the files equally amongst the processes. Start with the larger files, to balance the load:
        icpu = arange(len(filenames))
        while icpu.max() >= self.ncpus :
            icpu[icpu >= self.ncpus] -= self.ncpus
        buckets = []
        filenames = array(filenames)
        for cpu in range(self.ncpus):
            bucket = filenames[icpu == cpu]
            if len(bucket) > 0 :
                buckets.append(bucket)

        func = partial(self.run_subset, silent=self.silent, tempdir=self.tempdir)

        with Pool(processes=self.ncpus) as pool :
            return list(tqdm(pool.imap(func, buckets, chunksize=1), total=self.ncpus, desc='Compute adjoint chunks', leave=False))

    @staticmethod
    def run_subset(filenames: List[str], silent: bool = True, tempdir: str = '/tmp') -> str :
        #observations = shared_memory.obs
        times = shared_mem.time
        grid = shared_mem.grid
        adj_emis = zeros((times.nt, grid.nlat, grid.nlon))

        for file in tqdm(filenames, disable=silent) :
            observations = shared_mem.obs.loc[shared_mem.obs.footprint == file]

            with shared_mem.footprint_class(file) as fpf :
                fpf.align(grid, times.timestep, times.min)

                for obs in tqdm(observations.itertuples(), desc=fpf.filename, total=observations.shape[0], disable=silent):
                    fp = fpf.get(obs.obsid)
                    adj_emis[fp.itims, fp.ilats, fp.ilons] += obs.dy * fp.sensi

        with tempfile.NamedTemporaryFile(dir=tempdir, prefix='adjoint_', suffix='.h5') as fid :
            fname = fid.name
        with File(fname, 'w') as fid :
            adj_emis = adj_emis.reshape(-1)
            nz = nonzero(adj_emis)[0]
            fid['coords'] = nz
            fid['values'] = adj_emis[nz]
        return fname

        # # Save to shared memory
        # shm = shared_memory.SharedMemory(name='adjfield')
        # result = ndarray(adj_emis.shape, adj_emis.dtype, buffer=shm.buf)
        # result[:] += adj_emis
        # shm.close()



# @dataclass
# class Background:
#     footprint_class : Type[FootprintFile]
#     parallel : bool = False
#     ncpus : int = cpu_count()
#     tempdir : Path = Path('/tmp')
#     _silent : bool | None = None
    
#     def run(self, conc_field : xr.Dataset, obs: DataFrame) -> DataFrame:
        
#         for tracer in conc_field.tracers :
#             obs_tracer = self.interp_tracer(
#                 tracer, 
#                 conc_field[[tracer, 'height_above_ground']], obs.loc[obs.tracer == tracer]
#                 )
#             obs.loc[obs_tracer.index, 'mix_background'] = obs_tracer.mix_background
#         return obs
            
#     def interp_tracer(self, tracer_name : str, conc : xr.Dataset, obs : DataFrame):
#         bg = []
#         for fpfile in tqdm(obs.footprint.dropna().drop_duplicates()):
#             # No parallelization, for now:
#             bg.append(self.interp_footprint_file(tracer_name, conc, obs, fpfile))
#         return concat(bg)

#     @staticmethod
#     def interp_footprint_file(self, tracer_name : str, conc : xr.Dataset, obs: DataFrame, footprint_file : str):
#             with self.footprint_class(footprint_file) as fpf:
#                 endpoints = fpf.endpoints

#             # Interpolate columns at the particles positions:
#             columns = conc.interp(
#                 longitude = ('particles', endpoints.lon),
#                 latitude = ('particles', endpoints.lat),
#                 time = ('particles', endpoints.time)
#                 )
            
#             # Interpolate the columns at the particles height:
#             layer = columns.height_above_ground.values
#             column_conc = columns[tracer_name].values
#             level = (layer[:, :1] + layer[:, :-1]) / 2.
#             conc_interp = [interp(endpoints.height[ipos], level[ipos], column_conc[ipos]) for ipos in range(len(endpoints))]
            
#             # Calculate the average for each release:
#             df = DataFrame.from_dict({'mix_background': conc_interp, 'obsid': endpoints.obsid})
#             df = df.groupby('obsid').mean()
#             return df.reset_index()
        

@dataclass
class Model(ABC):
    parallel : bool = False
    ncpus : int = cpu_count()
    tempdir : str = '/tmp'

    def run_forward(self, obs: Observations, emis: Emissions) -> Observations :
        return Forward(self.footprint_class, self.parallel, self.ncpus, tempdir=self.tempdir).run(emis, obs)

    def run_adjoint(self, obs: Observations, adj_emis: Emissions) -> Emissions:
        return Adjoint(self.footprint_class, self.parallel, self.ncpus, tempdir=self.tempdir).run(adj_emis, obs)

    # def interpolate_backgrounds(self, obs: Observations, conc_field : xr.Dataset) -> Observations:
    #     return Background(self.footprint_class, self.parallel, self.ncpus, tempdir=self.tempdir).run(conc_field, obs)
        

    @property
    @abstractmethod
    def footprint_class(self) -> FootprintFile:
        """
        This should return the class used to read the footprint files (i.e. a derived instance of transport.base.files.FootprintsFile
        """
        pass

    def adjoint_test(self, obs: Observations, emis: Emissions) -> None :
        obs.loc[:, 'mix_background'] = 0.
        obs = self.run_forward(obs, emis)
        x1 = emis.asvec()
        obs.dropna(subset=['mix'], inplace=True)
        y1 = obs.mix.values
        y2 = y1 + 0.
        obs.loc[:, 'dy'] = y2
        adj = self.run_adjoint(obs, emis)
        x2 = adj.asvec()
        adjtest = 1 - dot(x1, x2)/dot(y1, y2)
        logger.info(f"Adjoint test: {dot(x1, x2)-dot(y1, y2) = }")
        logger.info(f"Adjoint test: {1 - dot(x1, x2)/dot(y1, y2) = }")
        if abs(adjtest) < finfo('float32').eps :
            logger.info("Success")
        else :
            logger.warning("Adjoint test failed")
        logger.info(f"Assumed machine precision of: {finfo('float32').eps = }")

    def write_footprints(self, destpath, desclass=None, silent=False):
        raise NotImplementedError
