
# Installation and setup

The Lund University Modular Inversion Algorithm (LUMIA) is a python package for performing atmospheric transport inversions.

The release 2020.8 is described in [https://www.geosci-model-dev-discuss.net/gmd-2019-227/](https://www.geosci-model-dev-discuss.net/gmd-2019-227/) can be downloaded [here](lumia.202008.tar.gz), however, we recommend instead getting the latest commit from [github](https://github.com/lumia-dev/lumia.git):

```bash
git clone --branch master https://github.com/lumia-dev/lumia.git
```

This documentation focuses on an updated release
???+ Danger "Pending update"
    this documentation is being revised at the very moment. The text in this page refers to the "master" branch, not to the "default" one).

The installation instructions are described in further details in the [README](https://github.com/lumia-dev/lumia/blob/master/README.md) file. In short:

1. Create a python virtual environment, using your favourite environment manager (e.g., conda)
2. Git clone the lumia source code in a clean folder
3. Install LUMIA as a python package in your python virtual environment, using pip
4. Start working!

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