# Build the "congrad.exe" program, which implements the Lanczos conjugate gradient optimizer (fortran code).
# Adapt the makefile to your needs ...
congrad:
	make -C src/congrad -f makefile.opensuse.gfortran congrad.exe
	mv src/congrad/congrad.exe bin/

# Create a conda environment for LUMIA, and install it in it.
# Adapt the name of the environment, and its path (typically, it should be something like ~/anaconda/envs/NAME_OF_THE_ENVIRONMENT/bin/python, 
# The suggested installation is for lumia, but it will also install a bunch of (relatively heavy) libraries for data analysis and visualisation
# If you want lumia and nothing else, follow the "install-lumia-minimal" section below instead.
install-lumia:
	conda create -n NAME_OF_THE_ENVIRONMENT cartopy ipython datashader
	conda activate NAME_OF_THE_ENVIRONMENT
	/path/to/your/conda/environment/bin/python -m pip install -e .[interactive]


# Minimal lumia installation (see notes above the "install-lumia" section above)
install-lumia-minimal:
	conda create -n NAME_OF_THE_ENVIRONMENT
	conda activate NAME_OF_THE_ENVIRONMENT
	/path/to/your/conda/environment/bin/python -m pip install -e .