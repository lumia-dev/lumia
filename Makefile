

install:
	python -m pip install -e .[interactive]

envcontainer:
	apptainer build --fakeroot lumiaenv.simg lumiaenv.def

container:
	apptainer build --fakeroot lumia.simg lumia.def

congrad:
	make -C src/congrad -f makefile.opensuse.gfortran congrad.exe
	mv src/congrad/congrad.exe bin/
