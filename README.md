# LUMIA

LUMIA is a python library primarily developed to perform atmospheric inversions. The code repository has the following structure:
- the folder **lumia** contains the actual lumia python library
- the folder **scripts** contains example codes to use lumia as well as a transport model code (see description further down).
- the folder **src** contains the source code for the conjugate gradient minimizer used in the default setup (it needs to be compiled before an inversion can be ran).

## Setup and use

Clone this git repository on your system (or download one of the releases), and move to the created directory.

### 1. Optional step: compile the conjugate gradient optimizer executable

The 4DVAR optimization relies on an external executable to compute the successive minimization steps. A fortran implementation is provided along with the python code, which first needs to be compiled. From the **src/congrad** directory, edit the **Makefile** to adapt it to your system, and compile with `make congrad.exe`.

If you don't plan to run inversions, or plan to use a different optimizer, skip this step.

### 2. Installation

The installation relies on the standard python `pip` and `setuptools` libraries:

* Use `pip install -e .` to install lumia on your system. This will link the lumia folder in the standard python libraries folder of your system (i.e. something like **$PREFIX/lib/python3.X/site_packages/** on a unix system, where __$PREFIX__ depends on whether you are installing as a super-user, as a normal user, or within a virtual environment).
    * By default, the setup script will also create links in your **$PREFIX/bin** folder pointing to the **congrad.exe** executable that you compiled at the step before, and to the **scripts/var4d.py** and **scripts/lagrange_mp.py**. If you are not happy with this behaviour, comment the `scripts` and `data_files` lines in the **setup.py** file.
    * Also, comment the `data_files` line in **setup.py** if you have skipped Step 1.
    * Refer to the pip user guide (https://pip.pypa.io/en/stable/user_guide/) for more information on the pip command. Lumia is, at this stage, not available in pypi.

### 3. Tutorial

A typical lumia run requires 4 components:
- the **lumia** python library itself
- a main script using the library: the **scripts/var4d.py** is here provided as an example
- a transport model and its various input data: the **scripts/lagrange_mp.py** is provided as an example.
- One (or several) configuration rc-file(s), containing pairs of `key : value` settings. An example rc-file is provided in the **examples** folder, but you will need to build your own. 

In the following, we assume you are using the example **scripts/var4d.py** script, and that it has been installed in your system or user **$PATH** following the standard installation of Step 2. The script is designed to perform a variational regional CO2 inversion, using the **lagrange_mp.py** transport model and the conjugate gradient gradient descent algorithm (**congrad.exe**). 

First, check that the script is correctly installed with

    var4d.py --help

This should print a help message listing the run attributes of **var4d.py**. If the script doesn't run at all, make sure it's executable and in a directory that is within your **$PATH** environment variable (or point directly to the script with `python /path/to/var4d.py --help`). If the script runs but complains about missing python libraries, install them.

The scripts has only one mandatory argument, which is the path to the configuration rc-file (<mark>see specific documentation still missing below</mark>). I recommand setting the `--verbosity` attribute to `DEBUG` the first time.

    var4d.py --verbosity=DEBUG config.rc

The script will do the following:
1. Read the pre-generated observation database and further setup the uncertainties, background concentrations and footprint files locations (the later two are specific to the transport operator we use here). The path to the observation database is passed via the `observations.input_file` key in the rc-file, or via the `--obs` argument of the script. If the `footprints.setup`, `obs.setup_bg` or `obs.setup_uncertainties` keys are set to `T` (True) in the rc-file, the script will compute and write additional columns in the observation database. Please refer to the <mark>still missing dedicated section below</mark>.
2. Load flux files: the example script <mark> not yet</mark> can read in a pre-processed flux file, at the location pointed to by the `--emis` argument of the script. In practice, it is more convenient to construct that file on demand and the script attempts to do this if no `--emis` argument is provided. <mark> Here we refer to the specific section on fluxes below</mark>.
3. Finally, setup and run the inversion.

### 4. Transport model

The package includes a regional transport model (**scripts/lagrange_mp.py**), which relies on pre-computed observation footprints and background concentrations. Please refer to www.geosci-model-dev-discuss.net/gmd-2019-227 (Section 3.2) for the mathematical description of the model. The transport model is typically called and controlled by the main lumia process (var4d.py), but it can also be called independently, in forward or adjoint mode:

Forward mode: 

    lagrange_mp.py --forward --obs obsfile.tar.gz --emis emfile.nc --checkfile forward.ok rcfile

This will calculate the concentrations corresponding to the observations in the observations file `obsfile.tar.gz`, given the fluxes in the flux file `emfile.nc`. The optional `--checkfile` argument provides the path to a file (named `forward.ok` in this example) that is written out at the very end of the simulation, and that is used by the main lumia script to check that the simulation didn't crash. The mandatory `rcfile` points to a rc-file, containing mainly settings related to the parallelization.

Adjoint mode:

    lagrange_mp.py --adjoint --obs obsfile.tar.gz --emis adjfile.nc --checkfile forward.ok rcfile

The arguments are mostly the same as in forward mode, but the observation file contains the model-data mismatches computed in forward mode and the `--emis` argument points to an empty file (which will be written out by the model, and will contain the adjoint field corresponding to these mismatches. The `rcfile` is also more important in adjoint mode as it contains some keys necessary to create the adjoint field structure (see below).

The script accepts additional arguments:
- `--serial`: by default, the script will split the observation database in several chunks, and run them in parallel. Use this argument if running on a single-cpu system (or on the frontend node of a HPC ...).
- `--verbosity`: set to `DEBUG` for more detailed messaging

#### 4.1 rc-file

The rcfile must contain a `path.run` key, which determines where the model output should be written, and a `model.transport.split` key, which takes an integer value and sets on how many CPUs the transport should be parallelized (the parallelisation is simply done by splitting the observation database and running multiple instances of the transport model, so multi-node parallelization on HPC systems is not possible, although it should be technically very simple to implement).

In adjoint mode, the transport operator also requires additional rc-keys, listed below:

    emissions.categories    : cat1, cat2, cat3 ! names of the fluxes categories
    emissions.cat1.optimize : T ! compute the adjoint of category "cat1"
    emissions.*.optimize    : F ! don't compute the adjoint of other categories
    time.start              : year1, month1, day1, hour1
    time.end                : year1, month2, day2, hour2
    emissions.*.interval    : m ! monthly emissions for all categories

In general, the transport is not called as a standalone process, but as a subprocess of a lumia run. In this case, the aforementioned keys should be set directly in the main lumia rc-file.

#### 4.2. Footprints

The model relies on pre-computed observation footprints:

#### 4.3. Flux file

#### 4.4. Observations file

### 5. Observations

### 6. Surface fluxes

### 7. Variational inversion

### 8. Technicalities

#### 8.1. rc-files

#### 8.2. Dependencies

#### 8.3. Conjugate gradient minimizer

## Updates history