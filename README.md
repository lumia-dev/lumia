# LUMIA

The Lund University Modular Inversion Algorithm (LUMIA) is a python package for performing atmospheric transport inversions.

The release 2020.8 is described in https://www.geosci-model-dev-discuss.net/gmd-2019-227/ and can be downloaded at [url](lumia.202008.tar.gz)

## Installation instructions

```
wget THEPACKAGE.tar.gz
tar xvf THEPACKAGE.tar.gz
pip -e ...
```

## Usage

The installation above setup the `lumia` library on the system, which can then be imported as any regular python module.
It also adds the following executable to the user $PATH:
- lagrange_mp.py
- congrad.exe

For more specific usage instruction, a [tutorial](GMDD/var4d.html) is available, and several example jupyter notebooks are included in the release file.

## Developer access

The LUMIA python package is a scientific tool:
- it is on constant evolution
- it has bugs that we haven't found yet
- some parts of the code are insufficiently documented

The code is released under the XXX open source licence. However, if you intend to use it for your scientific research, we suggest you contact us before ([lumia@blablabla](email)). We may provide support and give you a developer access to our git repository.

## Publications

https://www.geosci-model-dev-discuss.net/gmd-2019-227/

https://acp.copernicus.org/preprints/acp-2019-1008/

Thompson et al., 2020
