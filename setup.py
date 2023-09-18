#!/usr/bin/env python

import setuptools
from pathlib import Path

with open("README.md", "r") as fh:
    long_description = fh.read()

if Path('etc/lumia.cfg').exists():
    data_files = [('etc/lumia', ['etc/lumia.cfg'])]
else :
    data_files = None

setuptools.setup(
    name="lumia",
    version="1.0",
    author="Guillaume Monteil",
    author_email="guillaume.monteil@nateko.lu.se",
    description=long_description,
    long_description_content_type="text/markdown",
    url="https://www.geosci-model-dev-discuss.net/gmd-2019-227/#discussion",
    packages=setuptools.find_packages(),#['lumia', 'transport', 'archive', 'rctools'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: European Union Public Licence 1.2 (EUPL 1.2)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
    install_requires=['loguru', 'pandas>=2.0.1', 'dask', 'tables', 'numpy', 'h5py', 'matplotlib', 'python-dateutil', 'netCDF4', 'xarray', 'pint', 'omegaconf', 'scipy', 'setuptools', 'bottleneck', 'mkdocs', 'h5netcdf', 'tqdm'],
    extras_require={
        'interactive': ['ipython', 'ipywidgets', 'geoviews', 'hvplot', 'jupyter_bokeh'],
        'icos': ['icoscp'],
    },
    data_files=data_files
)
