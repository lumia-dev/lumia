#!/usr/bin/env python

from omegaconf import OmegaConf, DictConfig
from pandas import Timestamp
from types import SimpleNamespace
from gridtools import Grid
from importlib import resources
from pathlib import Path
from loguru import logger
import sys


# The following (commented lines) has been disabled for now, keeping it for reference as it might be useful ...

# Automatic type conversions:
# resolvers[type] = SimpleNamespace(forward=(prefix, basetype_to_type), reverse=type_to_basetype), with:
#   - type : class of the object that needs to be converted to a valid omegaconf basetype
#   - prefix : prefix to be used in the yaml files (${prefix:value}
#   - basetype_to_type : lambda function used to convert the key value to requested type (e.g. convert ${ts:20180101} to Timestamp(2018,1,1)
#   - type_to_basetype : reverse operation (e.g. convert Timestamp(2018,1,1) to "${ts:20180101}".

# resolvers = {
#     # Timestamp
#     Timestamp: SimpleNamespace(
#         forward=('ts', lambda s: Timestamp(s)),
#         reverse=lambda v: f'${{ts: {v}}}'),
#     Grid: SimpleNamespace(
#         forward=('Grid', lambda d: Grid(**d)),
#         reverse=lambda v: f'${{'
#                           f'Grid: {{lon0:{v.lon0}, lon1:{v.lon1}, lat0:{v.lat0}, lat1:{v.lat1}, dlon:{v.dlon}, dlat={v.dlat}}}'
#                           f'}}')
# }

# for resolver in resolvers.values():
#     OmegaConf.register_new_resolver(*resolver.forward)


# Register the "lumia" resolver (points to the path where lumia is installed):
prefix = resources.files("lumia")
if Path(sys.prefix) in prefix.parents :
    prefix = Path(sys.prefix)
else :
    prefix = prefix.parent
OmegaConf.register_new_resolver('lumia', lambda p: prefix / p)

# Register the `ts` and `Grid` resolvers:
OmegaConf.register_new_resolver('ts', Timestamp)
OmegaConf.register_new_resolver('Grid', lambda d: Grid(**d))


def read_config(file : str | Path, machine: str = None, **extra_keys) -> DictConfig:
    """
    Load a yaml configuration file.
    The "machine" optional argument can be a string, pointing to the name of a section to be rename as "machine". The content of that section should be machine-specific settings (i.e. paths, libraries, number of CPUs, etc.). This allows having a single configuration file valid on different computers.
    
    For example, consider the following sections of a yaml file:
    ```
        laptop :
            temp : /dev/shm/lumia
            footprints : /data/LUMIA/footprints
            correlations : /data/LUMIA/correlations
            ncores : 8
            output : output
        
        hpc :
            temp : /tmp
            footprints : /proj/LUMIA/footprints
            correlations : /scratch/LUMIA/correlations
            ncores : ${oc.env:SLURM_TASKS_PER_NODE}
            output : /scratch/LUMIA/output
            
        run :
            start : 1 jan 2018
            end : 1 jan 2019
            tag : my-project
            ncpus : ${machine.ncpus}
            paths : 
                temp : ${machine.tmp}
                footprints : ${machine.footprints}
                correlations : ${machine.correlations}
                output : ${machine.output}/${.tag}
            
    ```
    
    If loaded with "read_config(filename, machine='hpc')", the keys under the "run" section will read their value from the "hpc" section, e.g. "run.paths.footprints" will evaluate to "/proj/LUMIA/footprints".
    
    If no value is provided for "machine", an error will be raised when attempting to read keys pointing to the "machine" section. To prevent that, one solution can be to add a default "machine" section.
    
    ```
        machine :
            temp : /tmp
            footprints : ???                # Setting a key to "???" will raise a "MissingMandatoryValue" error
            correlations : correlations
            output : output
            ncores : 1
    ```
    
    The `extra_keys` section allows completing or overwriting keys from the YAML file. For instance:
    - conf = read_config(filename, extra_keys={'run':{'project': 'my_project'}}) will set the `run.project` key to `my_project`, regardless of the value that may exist in the yaml file.
    - conf = read_config(filename, extra_keys={'run':{'project': None}}) will not impact how the YAML file is read, as `None` values are ignored.
    """
    
    logger.info(f'Read config file {file}')
    
    dconf = OmegaConf.load(file)
    if machine :
        logger.info(f'Setting config section "machine" to "{machine}"')
        dconf['machine'] = dconf[machine]
        
    # Add keys for other sections as well
    if extra_keys is not None :
        for section_name, section in extra_keys.items():
            # Get rid of the None values
            curated_kwargs = {k: v for (k, v) in section.items() if v is not None}
            
            # Merge with the relevant section
            dconf[section_name] = OmegaConf.merge(dconf[section_name], curated_kwargs) 
            
            for k, v in curated_kwargs:
                logger.info(f'Setting config key {k} to value {v}')
    return dconf

    
def write_config(dconf: DictConfig, path : Path) -> None:
    """
    Write the LUMIA config file to a yanl file in the requested directory.
    Create the directory if needed.
    
    Arguments:
    dconf : LUMIA configuration object
    path : path to the output file. Must be the full file name, not just the path.
    """
    path.parent.mkdir(exist_ok=True, parents=True)
    OmegaConf.save(config=dconf, f=path)
    logger.debug(f"Run settings written to {path}")