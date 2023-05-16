#!/usr/bin/env python

import setuptools
from pathlib import Path
import os

# TODO we need a relative path
with open("README.md", "r") as fh:
    long_description = fh.read()

if Path('etc/lumia.cfg').exists():
    data_files = [('etc/lumia', ['etc/lumia.cfg'])]
else :
    data_files = None

setuptools.setup(
    name="lumia",
    version="0.0.1",
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
    install_requires=['loguru', 'pandas>=1.4', 'tqdm', 'netcdf4', 'h5py', 'cartopy', 'xarray', 'pint', 'scipy', 'omegaconf', 'h5netcdf', 'mkdocs'],
    extras_require={
        'interactive': ['ipython'],
        'icos': ['icoscp']
    },
    scripts=['bin/lumia'],
    data_files=data_files
)
