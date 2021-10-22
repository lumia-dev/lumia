#
# Makefile for Mike Fisher's conjugate gradient optimizer for TM5 4DVAR
#

# --- help --------------------------------------

help:
	@echo " "
	@echo " usage:"
	@echo " "
	@echo "   make congrad.exe # compile executable"
	@echo " "
	@echo "   make clean       # remove objects"
	@echo "   make clean-all   # remove objects and executable"
	@echo " "

# --- compiler and flags ------------------------

FC = gfortran 

FFLAGS_DEF = -fomit-frame-pointer -fdefault-real-8
FFLAGS_OPT = -O3 
#FFLAGS_PAR = -openmp
FFLAGS_CHECK = -O2 -check all -fdefault-real-8 -fdefault-double-16 -g -traceback -fpe0 -ftrapuv

FFLAGS = $(FFLAGS_DEF) $(FFLAGS_OPT) -ffree-line-length-none
#FFLAGS = $(FFLAGS_DEF) $(FFLAGS_WP) $(FFLAGS_CHECK)

# --- libs etc ---------------------------------------

INCS = -I/usr/include
LIBS = -L/usr/lib -lnetcdff -lnetcdf

# --- implicit rules ---------------------------------

# how to form object files from Fortran source:
%.o: %.F90
	$(FC) $< -c -o $@ $(FFLAGS) $(INCS)
	@echo ' '

lapack.o         	: lapack.F90
m_option_parser.o	: m_option_parser.F90
conjuGrad.o      	: conjuGrad.F90 lapack.o m_option_parser.o
conjuGrad_main.o 	: conjuGrad_main.F90 conjuGrad.o

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# targets
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

congrad.exe: conjuGrad_main.o
	  $(FC) -o $@ $(FFLAGS) *.o $(LIBS)

clean:
	rm -f *.{mod,o}

clean-all:
	rm -f *.{mod,o} congrad.exe
