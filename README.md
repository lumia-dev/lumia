The Lund University Modular Inversion Algorithm (LUMIA) is a python package for performing atmospheric transport inversions.

The release 2020.8 is described in https://www.geosci-model-dev-discuss.net/gmd-2019-227/ and can be downloaded at [url](lumia.202008.tar.gz)

# 1. Installation
lumia is constructed as a standard python package, and can be installed using the standard `pip` command.
There are two main ways of installing it:
- Installation as a python package
- Installation as a python project (recommended) 

The difference is that the installation as a project also downloads the documentation/example files, and installs everything in a folder of your choice. The installation as a package is faster and simpler but it only downloads and installs the lumia python package itself (i.e. it makes it possible to use `import lumia` in your python scripts), in the `site-packages` of your python environment.

## 1.1 Installation as a python package

`pip install git+https://github.com/lumia-dev/lumia.git`
## 1.2 Installation as a python project

    git clone https://github.com/lumia-dev/lumia.git myproj
    pip install -e myproj```

(replace `myproj` by the name of your project)

## 1.3 Caveats: dependencies
Regardless the installation method, the installer handles some dependencies (some python packages will be installed automatically if they are missing on the system), but two dependencies cannot be installed via pip, and need to be installed manually:
- The `cartopy` python package is required (it is used to compute land/sea masks). See https://scitools.org.uk/cartopy/docs/latest/installing.html for installation instructions.
- The variational inversions described in the documentation rely on an external compiled Fortran program. The source code and installation instructions can be found in the src/congrad folder.


# 2. Usage

After following either of the installation procedure, the `lumia` package and its sub-modules should be available for other python scripts on the machine. By default, the installer also creates a copy of the `transport/lagrange_mp.py` script inside the user `$PATH`. The script is used as a transport model in the example inversions presented in the [tutorial](doc/var4d.html).

For more specific usage instruction, please refer to the [tutorial](doc/var4d.html), and to the example jupyter notebooks in the GMDD folder.

# 3. Developer access

LUMIA is a scientific tool. The code is made available under the [EUPL v1.2](LICENSE) open source license, and it is not provided with any warranty. In particular:
- it is in constant evolution;
- it has bugs that we haven't found yet;
- some parts of the code are insufficiently documented.

You are free to download it and use it, but you are strongly encouraged to [contact us](mailto:lumia@mail.nateko.lu.se) before, to discuss the level of support that we can provide, and give you a developer access to our git repository. 

# 4. Publications

- https://www.geosci-model-dev-discuss.net/gmd-2019-227/
- https://acp.copernicus.org/preprints/acp-2019-1008/
- https://doi.org/10.1098/rstb.2019.0512
