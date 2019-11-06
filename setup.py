#!/usr/bin/env python

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
        name="lumia",
        version="2019.10",
        author="Guillaume Monteil",
        author_email="guillaume.monteil@nateko.lu.se",
        description=long_description,
        long_description_content_type="text/markdown",
        url="https://www.geosci-model-dev-discuss.net/gmd-2019-227/#discussion",
        packages=setuptools.find_packages(),
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: European Union Public Licence 1.2 (EUPL 1.2)",
            "Operating System :: OS Independent",
        ],
        python_requires='>=3.6',
        install_requires=['pandas'],
)