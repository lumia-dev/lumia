

install:
	python -m pip install -e .[interactive]

envcontainer:
	apptainer build --fakeroot lumiaenv.simg lumiaenv.def

container:
	apptainer build --fakeroot lumia.simg lumia.def
