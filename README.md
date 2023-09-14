The Lund University Modular Inversion Algorithm (LUMIA) is a python package for performing atmospheric transport inversions.

The release 2020.8 is described in https://www.geosci-model-dev-discuss.net/gmd-2019-227/ and can be downloaded at [url](lumia.202008.tar.gz)

# 1. Recommended installation

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

## Dos:
- Get familiar with how python packages should be installed [https://docs.python.org/dev/installing/index.html](https://docs.python.org/dev/installing/index.html)
- Get familiar with how conda environments work:
    - [conda documentation](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
    - [conda cheat sheet](https://docs.conda.io/projects/conda/en/4.6.0/_downloads/52a95608c49671267e40c689e0bc00ca/conda-cheatsheet.pdf)

## Don'ts:
- Don't try to run the setup.py file directly! (i.e. don't try anything like `python setup.py install`).
- Don't try to use the _Makefile_ directly (but you can look at it, it contains a shortened version of this documentation).

# 2. Folder structure and recommended usage

## Folder structure

The folder structure of LUMIA is the following:
- the _lumia_ folder contains the `lumia` python module (i.e. what you get when doing `import lumia` in python)
- the _transport_ folder contains the `transport` python module (that you can import using `import transport` in python), which contains the pseudo-transport model used in our LUMIA simulations (see documentation)
- the _docs_ folder contains a more extensive documentation
- the _run_ folder contains example scripts and configuration files.
- the _gridtools.py_ file is a standalone module (accessed via `import gridtools`)

The other files and folders are either related to optional functionalities (_icosPortalAccess_; _src_), required for the functionality of the [LUMIA web page](https://lumia.nateko.lu.se) (_CNAME_, _mkdocs.yml_), or for the structure of the python package (_setup.py_, _LICENSE_). The _Makefile_ is more for information purpose than for being used ...

## Testing the installation

Once you have installed `lumia` in your python environment (once you have gone through the [installation instructions above](#1.-recommended-installation) above), the `lumia` module will be accessible on your system. Try for example:
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

## Recommended workflow

The _run_ folder contains example scripts and configuration files. You can use them as an example on how to start (in addition of reading the documentation that exists). You can also put your own scripts and data in that folder.

We can recommend a few alternative workflows, depending on what you plan to do with LUMIA:
- You can put your scripts and data directly under the _run_ directory (e.g. _/home/myself/lumia/run_). This is a good way if you are just starting with LUMIA, of if you plan to develop it further.
- You can install lumia in one folder (e.g. _/home/myself/libraries/lumia_), and put your scripts in a completely different folder (e.g. */home/myself/projects/my_fancy_project*): if you have installed it correctly, the `lumia` python package is available from anywhere on your system (provided that you have activated the python environment in which it is installed). This is a good way if you plan to work on LUMIA from several different projects.
- If you plan to actively develop LUMIA, you could also have separate installation (e.g. */home/myself/lumia_stable* and */home/myself/lumia_dev*), each installed in a different python environement (e.g. *my_old_env* and *my_new_env*). 

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
