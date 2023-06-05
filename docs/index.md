The Lund University Modular Inversion Algorithm (LUMIA) is a python package for performing atmospheric transport inversions.

The release 2020.8 is described in [https://www.geosci-model-dev-discuss.net/gmd-2019-227/](https://www.geosci-model-dev-discuss.net/gmd-2019-227/) can be downloaded [here](lumia.202008.tar.gz), however, we recommend instead getting the latest commit from [github](https://github.com/lumia-dev/lumia.git)

## Installation

LUMIA is distributed as a python package, the simplest way to install it is using the standard `pip` command:

```
git clone https://github.com/lumia-dev/lumia.git lumia
pip install -e lumia
```

To check that the installation is successful, try `import lumia` from a python interpreter in a different directory. You can also verify that the command `which lumia` (from a bash interpreter) points to the `lumia` script in the `bin` directory of the `git` repository you just cloned.

Notes:

- The `pip` installer automatically installs dependencies. If you prefer using another python package manager (e.g. `apt` or `conda`), you need to install these dependencies manually before (see dependencies list in the [setup.py](../setup.py) file).
- It can be a good idea to at least pre-install the `cartopy` dependency with either `conda` or your system's package manager (`apt`, `dnf`, etc.): `pip` will install cartopy, but not the `geos` library which it depends upon.
- In general, in case of troubles with dependencies, `conda` (anaconda, miniconda) is the simplest solution.

The inversions also rely on a Fortran executable for calculating successive minimization steps, which needs to be compiled on your system:
```
cd src/congrad
make congrad.exe
```
(you will need the netCDF-Fortran library and a fotran compiler, e.g. gfortran).

## Usage

### As a program

The `lumia` script (under the `bin` directory of the `git` repository) can be used to run forward simulations and inversions:

```
lumia forward --rc forward.yaml   # run a forward simulation based on the forward.yaml configuration file
```
or
```
lumia optim --rc optim.yaml     # run an inversion using the optim.yaml configuration file
```

For information on how to build the yaml configuration files, see the [tutorial](tutorial.ipynb)

### As a python library

The `lumia` python package contains several python modules, which can be imported independently (e.g. `import rctools`, `from lumia import obsdb`, etc.):

- The `lumia` module is the main one, and implements the inversion themselves. It also contains submodules needed to read/write lumia-formatted observation and emission files
- The `transport` module contains the pseudo-transport model (i.e. based on pre-computed Lagrangian footprints). It can be used independently of `lumia`.
- The `gridtools`, `rctools`, `archive` and `icosPortalAccess` modules are small utilities used by `lumia` and/or `transport`.

See the [tutorial](tutorial.ipynb) for an example of a script using `lumia` to perform an inversion.

## Developer access

LUMIA is a scientific tool. The code is made available under the [EUPL v1.2](LICENSE) open source license, and it is not provided with any warranty. In particular:

- it is in constant evolution;
- it has bugs that we haven't found yet;
- some parts of the code are insufficiently documented.

You are free to download it and use it, but you are strongly encouraged to [contact us](mailto:lumia@mail.nateko.lu.se) before, to discuss the level of support that we can provide, and give you a developer access to our git repository. 