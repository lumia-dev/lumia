BIN_DIR?=~/.local/bin
CONTAINER?=lumia.sif
ENVCONTAINER?=lumiaenv.sif

build: lumia.def
	[ ! -f ${CONTAINER} ] || rm ${CONTAINER}
	singularity build ${CONTAINER} lumia.def

buildenv: lumiaenv.def
	[ ! -f ${CONTAINER}] || rm -f ${CONTAINER}
	singularity build -f ${ENVCONTAINER} lumiaenv.def

config:
	python genconfig.py --exec ${BIN_DIR} --container ${CONTAINER} --dev ..

install: lumia
	cp lumia ${BIN_DIR}
	chmod +x ${BIN_DIR}/lumiao
