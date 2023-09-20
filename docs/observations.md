# Observations

The `lumia.observations` module provides classes to store data in the observations space (observation vector, observation uncertainties, observation coordinates, model estimates for concentrations, etc.).

???+ Note "Modularity"
    
    The design of the observations class depends, to some extent, of the type of observations (e.g. in-situ observations vs. satellite observations) and of the type of transport model (what metadata needs to be stored?).

    This document describes the default class (`lumia.Observations`, a.k.a. `lumia.observations.observations.Observations`). It is designed for handling in-situ data, and for use with the default [`lumia.Transport`](transport-module.md) class.

## Structure

The `Observations` object contains two main data attributes:

- `sites` (*pandas.DataFrame*) contains information on observation series: site name and code, coordinates (if relevant), observation type, data provider, DOI, etc.
- `observations` (*pandas.DataFrame*) contains information on the observation themselves: observed concentrations, estimated uncertainties, coordinates, etc.)

The `Observations` class also implements a few methods:

- I/O methods, to read and write the observations to various file formats (HDF, tar.gz)
- selection methods, to refine the data selection and extract a subset of the observations.
- `calc_uncertainty`: a method to estimate the observation uncertainties (i.e. combine the measurement uncertainty with an estimate of the model uncertainty).

Finally, the `Observation` objects have two "properties" (attributes): `mismatch` and `sigma`, which contain respectively the model-data mismatches (model - obs) and the uncertainty (1$\sigma$) on them. These attributes are accessed by the optimizer.

## Construction

The simplest way to create a new `Observations` object is to construct first its `sites` and `observations` tables:

- The `Observations.sites` table contain information common to a whole observation series. For instance, the site name and coordinates, sitecode, tracer, units, data provider, DOI, source file, etc. There is no specific or mandatory list of columns, the table is here for the user convenience.
- The `Observations.observations` table contains the observations themselves, their uncertainties, relevant metadata (time, sampling height, etc.) and more generally any data in the observation space that may be required by the model (e.g. path to pre-computed observation footprints) or that it may be worth storing. It is also where modelled values will be stored. The `observations` table has a few mandatory or recommended columns:
    - __obs__: the observed concentrations
    - __err__: the total observation uncertainty (i.e. uncertainty on the model-data mismatch, including both the observation uncertainty and the model uncertainty)
    - __time__: the observation time
    - __height__: the sampling height (above ground) of the observations
    - __code__: the site code
    - __site__: the index of the relevant row in the `Observations.sites` table
    - __mix_background__: the background concentrations (i.e. boundary condition).

One way to see it is that the "observations" table stores what is usually the data part of an observation files (i.e. the columns in a CSV file, or the variables in a netCDF file), whereas the "sites" table stores the metadata (header in a CSV file, attributes in a netCDF file).

The code below shows a partial example for importing a bunch of files (in this example, ICOS CSV files) to a `lumia.Observations` object, and finally storing that object as a __tar.gz__ file:

```python
from pathlib import Path
from pandas import DataFrame, read_csv, concat
from lumia import Observations

def import_csv_icos(filename : Path) -> (dict, DataFrame):
    """
    A function that reads one ICOS observation file (in CSV format), and returns:
    - metadata: a dictionary containing the elements that should go in the `Observations.sites` table
    - data: a DataFrame containing the section of the `Observations.observations` table corresponding to this file
    """
    ...
    return metadata, data

# Create temporary lists to store the imported data
sites, observations = [], []

# Loop over the files (look for every file matching the "/path/to/observations/co2*.csv" pattern
for filepath in Path('/path/to/observations').glob('ICOS_ATC_*.CO2'):
    site, obs = import_csv_icos(filepath)
    sites.append(site)
    observations.append(obs)

# Concatenate the sites and observations lists in two DataFrames:
sites = concat(sites)
observations = concat(observations)

# Create the actual lumia.Observations object
obs = Observations(sites=sites, observations=observations)

# Store it to disk:
obs.save_tar('observations.tar.gz') 

```

## I/O and file formats

The `Observations` class can read and write to two different file formats:

- __tar.gz__ files are the default file format. The *sites* and *observations* tables are simply written out as CSV files, and clobbed together in a gzip-compressed tar files. The main advantage of that format is that it remains human-readable (one only needs to uncompress the tar.gz file). On the other hand, it is not very fast.
- __hdf__ files are also used internally, for communication with the [`transport` package](transport_pkg.md).

The **tar.gz** format is the recommended one for day-to-day interactions:

```python
import lumia

# Read an observation file in tar.gz format:
obs = lumia.Observations.from_tar('observations.tar.gz')

# Write an Observations object to a file:
obs.save_tar('observations.tar.gz')
```

## Other methods:

The list may not be complete: refer to the actual code for further information.

* `Observations.calc_uncertainties`: Compute an estimate of the uncertainties in the observation space, combining the measurement uncertainty and model uncertainty. The method is called at the end of the first forward iteration, as it (may) need the prior model estimates for the observations. See documentation in the code for further details. The method may need an additional `settings` attribute to be defined.
* `Observations.select_times`: when provided with a list of sites (indices of the `Observations.sites` table), returns a new `Observations` object, containing only data for these sites.
* `Observations.select_sites`: returns a new `Observations` object, limited to the provided range of dates.