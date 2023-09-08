## Package structure and recommended installation

### Package structure

LUMIA is distributed as a python package. The (simplified) folder structure is the following:
- **docs** : this documentation
- **lumia** : the `lumia` python package (accessible via an `import lumia` in python), which contains all the optimization-related code
- **transport** : the `transport` python package (accessible via `import transport`), which is the pseudo-transport model used in LUMIA
- **gridtools.py** : source code for the `gridtools` package (accessible via `import gridtools`), which contains some utilities to handle spatial grids.
- **run**: the *run* folder contains some example scripts using LUMIA. It is also the recommended place to put your own scripts. By default, files in that folder are excluded from the git repository.

At the root of the git repository, you will also find the following files:
- **mkdocs.yml** : this file defines the structure of the online documentation
- **Makefile** : the Makefile contains a few recepies for installing and configuring lumia
- **setup.py** : this is the file that enables installing lumia as a python package. Do not use it directly (see installation notes below).
- **requirements.txt** : this file contains the list of python packages required by lumia.

### Installation

We recommend installing LUMIA in a dedicated python virtual environment. We suggest using `conda` for this (https://docs.conda.io/en/latest/) as it will facilitate installing packages that are not purely python-based (e.g. *cartopy*, *xesmf*, etc.). However, other tools (including the standard *venv*) should work as well.

#### Creation of a conda environment:
1. Dowload and install anaconda/miniconda if needed:
    ```bash
    mkdir -p ~/miniconda3
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
    bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
    rm -rf ~/miniconda3/miniconda.sh
    ```
2. Activate your conda installation: `conda init bash`
3. Create a virtual environment: `conda create -n my_env python=3.11` (we recommend using the latest python version available).
4. Activate the environment: `conda activate my_env`

#### Installing lumia
1. If not already done, clone the lumia git repository:
    ``` 
    git clone ... my_project
    cd my_project
    ```
2. Install the dependencies:
    - with conda: `conda install --file requirements.txt`
    - with pip: `pip install -r requirements.txt`
3. Install lumia itself: `pip install -e .`
