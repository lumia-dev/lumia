#!/usr/bin/env python

import os
import shutil
from pandas import DataFrame, read_hdf, isnull
from numpy import array, nan
from tqdm import tqdm
from loguru import logger
from transport.core.model import FootprintFile
from typing import Type, Dict
import xarray as xr
from transport.concentrations import interp_file
from pathlib import Path
from lumia.utils import debug
from omegaconf import DictConfig


def check_migrate(source, dest):
    if os.path.exists(dest):
        return True
    elif os.path.exists(source):
        shutil.copy(source, dest)
        return True
    return False


class Observations(DataFrame):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    @classmethod
    def read(cls, filename: str) -> "Observations":
        return cls(read_hdf(filename))

    @property
    def _constructor(self):
        """
        Ensures that the parent's (DataFrame) methods return an instance of Observations and not DataFrame
        """
        return Observations

    @property
    def tracers(self):
        return self.tracer.drop_duplicates()

    def write(self, filename: str) -> None:
        self.to_hdf(filename, key='observations')

    def gen_filenames(self, multitracer : bool = False) -> None:
        fnames = self.code.str.lower() + self.height.map('.{:.0f}m.'.format) + self.time.dt.strftime('%Y-%m.hdf')
        self.loc[:, 'footprint'] = fnames

    @debug.trace_args("archive", "local")
    def find_footprint_files(self, archive: str | Dict, local: str | None = None) -> None:
        """
        Locate the footprint files, and, if needed, copy them to a temporary path (e.g. on a fast SSD).
        Arguments:
            - archive : Path where the footprint files are stored (permanent storage). Optionally, can be provided as a dictionary containing tracer-specific paths (e.g. {'co2':'/path/to/the/co2/footprints', 'ch4':'/path/to/the/ch4/footprints'}.
            - local : Path where the footprint files should be copied:
                * If left to "None" (default), the footprints are left in place.
                * If "archive" is provided as a dictionary and "local" is not None, then footprints will be copied to a tracer-specific subdirectory of "archive".
        """
        
        tracer_specific_path = isinstance(archive, dict | DictConfig)

        for tracer in self.tracers :

            remote_path = archive
            if tracer_specific_path:
                remote_path = archive[tracer]

            if local is None :
                local_path = remote_path
            else :
                local_path = local
                if tracer_specific_path :
                    local_path += '/' + tracer

            # Append file names, both for local and archive:
            footprints = self.loc[self.tracer == tracer, 'footprint']
            fnames_archive = remote_path + '/' + footprints
            fnames_local = local_path + '/' + footprints

            # Retrieve the files from archive if needed:
            if local is not None :
                Path(local_path).mkdir(exist_ok=True, parents=True)
            exists = array([check_migrate(arc, loc) for (arc, loc) in tqdm(zip(fnames_archive, fnames_local), desc=f'Migrate footprint files for tracer {tracer}', total=len(fnames_local), leave=False)])
            fnames_local[~exists] = nan
            self.loc[self.tracer == tracer, 'footprint'] = fnames_local

        if len(self.footprint.dropna()) == 0 :
            logger.error("No valid footprints found. Exiting ...")
            raise RuntimeError("No valid footprints found")
            
    def gen_obsid(self) -> None:
        # Construct the obs ids:
        obsids = self.code + self.height.map('.{:.0f}m.'.format) + self.time.dt.strftime('%Y%m%d-%H%M%S')
        self.loc[~isnull(self.footprint), 'obsid'] = obsids

    def check_footprint_files(self, cls: Type[FootprintFile]) -> None:
        # Check in the files which footprints are actually present:
        fnames = self.footprint.drop_duplicates().dropna()
        footprints = []
        for fname in tqdm(fnames, desc="check_footprint_files"):
            with cls(fname) as fpf:
                footprints.extend(fpf.footprints)
        self.loc[~self.obsid.isin(footprints), 'footprint'] = nan

    @debug.trace_args()
    def check_footprints(self, archive: str, cls: Type[FootprintFile], local: str=None) -> None:
        """
        Search for the footprint corresponding to the observations
        """
        # 1) Create the footprint file names
        self.gen_filenames()
        self.find_footprint_files(archive, local)
        self.gen_obsid()
        self.check_footprint_files(cls)

    def interp_background(self, conc_field : xr.Dataset, footprint_class: Type[FootprintFile]):
        # single process implementation:
        files = self.loc[:, ['footprint', 'tracer']].drop_duplicates().dropna()
        self.loc[:, 'mix_background'] = nan
        pbar = tqdm(files.itertuples(), total=len(files))
        for fpfile in pbar:
            obs = self.loc[(self.footprint == fpfile.footprint) & (self.tracer == fpfile.tracer), ['footprint', 'obsid', 'tracer']]
            with footprint_class(fpfile.footprint) as fpf:
                endpoints = fpf.get_endpoints(obs.obsid)
            bg = interp_file(conc_field, endpoints, fpfile.tracer.upper(), field='mix_interpolated')
            tqdm.write(f'Mean {fpfile.tracer} background for file {fpfile.footprint}: {bg.mix_interpolated.mean()}')
            bg = self.merge(bg, on='obsid', how='left').set_index(self.index).mix_interpolated.dropna()
            self.loc[bg.index, 'mix_background'] = bg