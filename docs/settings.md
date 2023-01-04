# Configuration file

The settings are contained in a [configuration file](inversion.yaml) in _yaml_ format.

The file contains six major sections: 
- the `run` section, which contains general simulation settings (domain, resolution, etc.)
- the `emissions` section contains all settings needed to construct the file containing the surface fluxes
- the `observations` section contains all settings needed to handle the observations (obs file, units, uncertainties, etc.)
- the `optimize` section contains settings specific to the inversion (which category needs to be optimized, with which uncertainties, etc.)
- the `model` section contains settings of the transport model
- the `minimizer` section contains settings of the conjugate gradient minimizer

Additional sections may be defined for convenience. The values in this page are those used for the [tutorial](../tutorial)

## `run` section

General settings (simulation extent and resolution, paths, etc.)

```
run:
    start    : 2018-01-01
    end      : 2019-01-01
    timestep : 1h
    domain   : eurocom025x025
    grid     : ${Grid:{lon0:-15, lat0:33, lon1:35, lat1:73, dlon:.5, dlat:.5}}
    tracers  : co2
    paths    :
        data        : data
        output      : output
        footprints  : footprints
        temp        : temp
```

- `start` and `end` (time boundaries of the simulation) can be in any format supported by `pandas.Timestamp`. 
- `timestep` can be anything supported by `pandas.tseries.frequencies.to_offset`.
- The `grid` key defines an instance of [`gridtools.Grid`](gridtools.py). The syntax is `${Grid:value}`, with `value` a python dictionary containing keywords passed to `gridtools.Grid`.

## `emissions` section:

The section contains keys needed to construct the emissions file. There needs to be one subsection for each tracer (only one "co2" tracer in this example), and an `emissions.tracers` key is also necessary.

The emission file for the simulation is constructed from annual, category-specific, pre-processed emission files. The keys define the naming pattern for these files:
- the files are in [data/fluxes/nc/eurocom025x025/1h](data/fluxes/nc/eurocom025x025/1h/)
- the files for category `biosphere` are named following the pattern `flux_co2.EDGARv4.3_BP2019.%Y.nc`

```
emissions :
    tracers : ${run.tracers}
    co2 :
        region   : ${run.grid}
        interval : ${run.timestep}
        categories :
            fossil    : EDGARv4.3_BP2019
            biosphere : LPJ-GUESS_verify
        prefix : flux_co2.
        path   : ${run.paths.data}/fluxes/nc/${run.domain}/{run.timestep}/
```

## `observations` section:

The section contains keys needed to read and process the observation database. In this tutorial, it just points to the right observation file, but further settings are possible, to restrict the time period, setup the uncertainties, etc. These settings are read in two places:
- directly in the relevant obsdb module ([lumia.obsdb.InversionDb.obsdb](lumia/obsdb/InversionDb.py) class)
- in a pre-processing step, e.g. in the [lumia.ui.setup_observations](lumia/ui/main_functions.py) method.

See the in-line documentation of these two modules for further info

```
observations :
    file :
        path  : doc/observations/obs_example.tar.gz
```

## `optimize` section

This section contains the keys that define the inversion problem. It contains two large subsection:
- one "emissions" subsection, which contains settings related to the state vector and its uncertainty matrix (structure of the uncertainty, number of optimized categories, resolution of the optimization, etc.)
- one "observations" subsection, which defines the way observation uncertainties are treated:

```
optimize :
    emissions :
        co2 :
            biosphere :
                annual_uncertainty    : 0.45 PgC
                spatial_correlation   : 500-g
                temporal_correlation  : 30D
                npoints               : 2500
                optimization_interval : 7d

    observations :
        co2 :
            uncertainty :
                type : dyn
                freq : 7d
```

Here, the emissions will be optimized for the `biosphere` category of the `co2` tracer. The total uncertainty is set to 0.45 PgC (`annual_uncertainty`), with correlations decaying spatially (`spatial_correlation`) following a 500 km Gaussian function, and temporally (`temporal_correlation`) following a 30 days exponential function (see sect 3.5.1 in [https://gmd.copernicus.org/articles/14/3383/2021/](https://gmd.copernicus.org/articles/14/3383/2021/)).
The inversion solves for 2500 cluster of pixels (`npoints`) each seven days (`optimization_interval`).

With `observations.co2.uncertainty.type` set to `dyn`, the observation uncertainties are determined based on the quality of the fit of the short term (< 7 days) variability of the modelled concentration to the observed concentrations (see [setup_uncertainties](lumia/ui/main_functions.py) method).

## `congrad` section:

This short section contains settings for the conjugate gradient minimizer. The main relevant user-setting is `max_number_of_iterations` (set to a lower value to speed things up, set to > 50 for scientific results):

```
congrad :
    max_number_of_iterations : 80
    communication_file       : ${run.paths.temp}/congrad.nc
    executable               : ${lumia:src/congrad/congrad.exe}
```

## `model` section

This section contains settings for the interface between lumia and the transport model (i.e. [lumia/obsoperator](lumia/obsoperator/__init__.py))

```
model :
    transport :
        exec   : ${lumia:transport/multitracer.py}
        serial : False
    output :
        steps : ['apri', 'apos']
    path : ${run.paths}
```
Note the syntax of the `model.transport.exec` key: `${lumia:path}` points to a path relative to the installation path of the `lumia` python module.
