BIN_DIR?=~/.local/bin
CONTAINER?=lumia.sif

build: lumia.def
	[ ! -f ${CONTAINER} ] || rm ${CONTAINER}
	singularity build ${CONTAINER} lumia.def