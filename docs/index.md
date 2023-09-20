


The Lund University Modular Inversion Algorithm (LUMIA) is a python package for performing atmospheric transport inversions.

The release 2020.8 is described in [https://www.geosci-model-dev-discuss.net/gmd-2019-227/](https://www.geosci-model-dev-discuss.net/gmd-2019-227/) can be downloaded [here](lumia.202008.tar.gz), however, we recommend instead getting the latest commit from [github](https://github.com/lumia-dev/lumia.git):

```bash
git clone --branch master https://github.com/lumia-dev/lumia.git
```

This documentation focuses on an updated release
???+ Danger "Pending update"
    this documentation is being revised at the very moment. The text in this page refers to the "master" branch, not to the "default" one).

# Folder structure

The folder structure of LUMIA is the following:

- the _lumia_ folder contains the `lumia` python module (i.e. what you get when doing `import lumia` in python)
- the _transport_ folder contains the `transport` python module (that you can import using `import transport` in python), which contains the pseudo-transport model used in our LUMIA simulations (see documentation)
- the _docs_ folder contains a more extensive documentation
- the _run_ folder contains example scripts and configuration files.
- the _gridtools.py_ file is a standalone module (accessed via `import gridtools`)

The other files and folders are either related to optional functionalities (_icosPortalAccess_; _src_), required for the functionality of the [LUMIA web page](https://lumia.nateko.lu.se) (_CNAME_, _mkdocs.yml_), or for the structure of the python package (_setup.py_, _LICENSE_). The _Makefile_ is more for information purpose than for being used ...

???+ note "lumia, LUMIA, transport and transport"

    The LUMIA git repository contains two python packages (`lumia` and `transport`). Furthermore, the `lumia` package contains several modules called `transport`. This can be very confusing. Therefore, throughout the documentation:

    - the [transport package](transport_pkg.md) refers to the top-level `tranport` package (i.e. what is imported via `import transport`)
    - the [transport module](transport_mod.md) refers to the `lumia.models.footprints.transport` module.
    - "the lumia package" refers to `lumia` (what is accessed via `import lumia`)
    - LUMIA refers to the entire project.

# Installation

LUMIA is written in python, and depends on many other scientific packages. We recommend using in a [miniconda](https://docs.conda.io/projects/miniconda/en/latest/) virtual environment, with at least the `cartopy` package installed:
```bash
# Create a conda environment for your LUMIA project (change `my_proj` by the name you want to give it)
conda create -n my_proj cartopy

# Activate the environment:
conda activate my_proj

# Clone lumia from its git repository (change `my_folder` by the name of the folder you want LUMIA to be installed in. The folder should not exist before)
git clone --branch master https://github.com/lumia-dev/lumia.git my_folder

# Install the LUMIA python library inside your virtual environment (replace `my_folder` by the name of the folder where you have cloned LUMIA in).
pip install -e my_folder
```

???+ Warning "Dos and Don'ts"
    * Do get familiar with how python packages should be installed [https://docs.python.org/dev/installing/index.html](https://docs.python.org/dev/installing/index.html)
    * Do get familiar with how conda environments work:
        - [conda documentation](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
        - [conda cheat sheet](https://docs.conda.io/projects/conda/en/4.6.0/_downloads/52a95608c49671267e40c689e0bc00ca/conda-cheatsheet.pdf)
    * Don't try to run the setup.py file directly! (i.e. don't try anything like `python setup.py install`).
    * Don't try to use the _Makefile_ directly (but you can look at it, it contains a shortened version of this documentation).

# Testing the installation

Once you have installed `lumia` in your python environment (i.e. once you have gone through the [installation instructions above](#1.-recommended-installation)), the `lumia` module will be accessible on your system. Try for example:
```bash
conda activate my_proj  # Make sure you are in the right python environment!
cd /tmp                 # Move to another folder, basically anywhere where your lumia files are not
python -m lumia         # Run python with the `lumia` module
```

This should produce an error such as:
```bash
/home/myself/miniconda3/envs/my_proj/bin/python: No module named lumia.__main__; 'lumia' is a package and cannot be directly executed
```

This is good! it means that python has found lumia (but doesn't know what to do with it, that's another issue).

If you get instead an error such as:
```bash
/usr/bin/python3: No module named lumia
```
This means that python cannot find the `lumia` module: either you are not within the right python environment (read the [conda documentation](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) if you are not familiar with it), or that another error happened during the installation of `lumia` (did you use the `pip install -e /path/to/lumia` as instructed above? did that return an error (maybe some dependency could not be installed?)).

# Recommended workflow

The _run_ folder contains example scripts and configuration files. You can use them as an example on how to start (in addition of reading the documentation that exists). You can also put your own scripts and data in that folder.

We can recommend a few alternative workflows, depending on what you plan to do with LUMIA:

- You can put your scripts and data directly under the _run_ directory (e.g. _/home/myself/lumia/run_). This is a good way if you are just starting with LUMIA, of if you plan to develop it further.
- You can install lumia in one folder (e.g. _/home/myself/libraries/lumia_), and put your scripts in a completely different folder (e.g. */home/myself/projects/my_fancy_project*): if you have installed it correctly, the `lumia` python package is available from anywhere on your system (provided that you have activated the python environment in which it is installed). This is a good way if you plan to work on LUMIA from several different projects.
- If you plan to actively develop LUMIA, you could also have separate installation (e.g. */home/myself/lumia_stable* and */home/myself/lumia_dev*), each installed in a different python environement (e.g. *my_old_env* and *my_new_env*). 


# Documentation summary

LUMIA implements the inversion as a combination of (semi) independent modules, implementing the various components of an inversion setup. The links below point to the documentation of these modules:


??? Note "Theoretical summary"

    LUMIA is (primarily) a library for performing atmospheric inversions: Observations of the atmospheric composition are used to improve a prior estimate of the emissions (or fluxes) of one (or several) atmospheric tracer (e.g CO$_2$). 

    The inversion relies on an atmospheric chemistry-transport model (CTM) to link fluxes of a tracer in and out of the atmosphere with observed atmospheric concentration. This can be formalized as

    $$y + \varepsilon_{y} = H(x) + \varepsilon_{H}$$

    where $y$ is an ensemble of observations, $H$ is a CTM, and $x$ is a vector containing parameters controlling the CTM (i.e. x is the _control vector_). The uncertainty terms $\varepsilon_{y}$ and $\varepsilon_{H}$ represent respectively the measurement error and the model error. 

    The aim of the inversion is to determine the control vector $x$ that leads to the optimal fit of the model $H$ to the observations $y$. This is formalized as finding the vector $x$ that minimises the cost function

    $$J(x) = \frac{1}{2}\left(x-x_b\right)^TB^{-1}\left(x-x_b\right) + \frac{1}{2}\left(H(x) - y\right)^TR^{-1}\left(H(x) - y\right)$$

    where $x_b$ a prior estimate of the control vector $x$ (that we are trying to determine). The error covariance matrices $B$ and $R$ contain respectively the estimated uncertainties on $x_b$ and $y - H(x)$.

    The optimal control vector $\hat{x}$ is determined using an iterative conjugate gradient approach.

    While the aim of the inversion is typically to estimate the emissions of a tracer, the control vector $x$ rarely consists (directly) of the emissions: it can for instance contain scaling factor or offsets, at a lower resolution than the emission themselves, it may contain only a subset of the emissions, it can also include additional terms such as an estimate of the boundary condition, bias correction terms, etc. 

    $$ E = M(x) $$

- [`lumia.observations`](observations.md) implements the observation vector ($y$), the corresponding uncertainties ($R$);
- [`lumia.models`](models.md) handles the interface between LUMIA and the actual CTM ($H$);
- [`lumia.prior`](prior.md) implements the prior control vector $x_b$ and its uncertainty matrix $B$;
- [`lumia.optimizer`](optimizers.md) implements the optimization algorithm;
- [`lumia.data`](data.md) implements the emissions ($E$) themselves;
- [`lumia.mapping`](mapping.md) implements the mapping operator ($M$), that is used to convert between model data and control vector.
- [`lumia.utils`](utils.md) contains various utilities, used by several of the other `lumia` modules. The [Utilities](utils.md) page also describes the `gridtools` module, that is distributed along `lumia`.
- [`transport`](transport.md) acts as a CTM ($H$) in our LUMIA inversions.

Most of these modules can be used independently. For instance, the `lumia.observations` defines an `Observation` class that can be used to read, write and analyze observation files used in LUMIA (including LUMIA results).

Example scripts showing a full inversion are provided under the _run_ directory, and presented in more details in the [Tutorial](tutorial.md).

Finally, some top-level objects (classes and functions) are described under the [LUMIA overview](overview.md) page. The page also contains a more elaborate information on the coding strategy of LUMIA.

??? Note "LUMIA Development strategy"
    LUMIA was designed as a modular system: it should remain as easy as possible to replace the default CTM and the individual components (prior, observations, optimizer, etc.) should remain as independent from each other as reasonable.

    In practice this means that the following technical choices were made:

    - The LUMIA modules contain python classes to handle the different components of the inversions (e.g. an `Observations` class to handle the observations, a `PriorConstraints` class to construct and store the prior uncertainty matrix, etc.).
    - All modules implement or support alternative versions of these classes. For instance, the "Observations" class is defined in the `lumia.observations.observations` class (i.e. _lumia/observations/observations.py_ file):
        - This approach makes space for alternative implementations (e.g. one could implement an `Observations` class part of a `lumia.observations.satellite_obs` module).
        - It can however make imports paths length (`from lumia.observations.observations import Observations`, `from lumia.models.footprints.data import Data`, etc.). To circumvent this, many "shortcuts" have been defined in the *\__init__.py* files of the module (enabling e.g. `from lumia.observations import Observations`), and, even in the top-level *\__init__.py* file (*lumia/\__init__.py*). These are documented in the relevant sections (e.g. in [Observations](observations.md)) and, for the top-level objects, in the section below.
        - In several of the modules, there is a _protocol.py_ file, which essentially contain templates for the various classes. In theory, as long as alternative classes follow the template defined in the _protocol.py_ files, they should not lead to major compatibility issues.
    - Although it was developed along LUMIA, the code that acts as a CTM ([`transport`](transport.md)) remains a separate python package and is run as a subprocess: This forces us to limit the degree of inter-dependency between the two codes, and thus should make it easier implementing an alternative CTM.