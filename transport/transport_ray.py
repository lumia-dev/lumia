#!/usr/bin/env python

from h5py import File
from numpy import arange, array_equal
from datetime import datetime, timedelta
from pandas import date_range
from pandas import read_excel
from lumia.formatters.lagrange import ReadStruct, Struct, WriteStruct

import logging
logger = logging.getLogger(__name__)

class Flux:
    """
    This class slighly adapts the "Struct" class from the lumia.formatters.lagrange module: it ensures that
    all the fields are on the exact same grid (including temporally), and stores some variables related to that
    grid, to make the conformity check with the footprints easier and allow adjusting the time indices, which
    don't have the same origin.
    TODO: this should be directly part of the "Struct" class itself
    """
    def __init__(self, fluxfile):
        self.data = ReadStruct(fluxfile)
        self.categories = self.data.keys()
        
        # Make sure that all categories have the same coordinates (this is a requirement):
        cat0 = self.categories[0]
        for cat in self.categories :
            assert array_equal(self.data[cat]['time_interval']['time_start'], self.data[cat0]['time_interval']['time_start'])
            assert array_equal(self.data[cat]['time_interval']['time_end'], self.data[cat0]['time_interval']['time_end'])
            assert array_equal(self.data[cat]['lats'], self.data[cat0]['lats'])
            assert array_equal(self.data[cat]['lons'], self.data[cat0]['lons'])
            
        # Retrieve the dimensions
        # TODO: for now, we assume that lat and lon conform with those of the footprints. Ideally, we'd need a check on this
        tstart = self.data[cat0]['time_interval']['time_start']
        self.start = tstart[0]
        self.tres = tstart[1]-tstart[0]
        self.nt, self.nlat, self.nlon = self.data[cat0]['emis'].shape
        
    def write(self, outfile):
        WriteStruct(self.data, outfile)
        
        
class Observations:
    """
    This is used mainly to store the observations and define the read/write methods
    """
    def __init__(self, obsfile):
        self.data = read_excel(obsfile)
        
    def write(self, obsfile):
        self.data.to_excel(obsfile)
        
        
@ray.remote
class FootprintFile:
    """
    This class is used to apply forward or adjoint transport to the footprints contained in a single
    footprint file (which can itself contain multiple footprints).
    The format of the footprints is a HDF5 file, with one group for each footprint (the group name identifies
    the observation). Within each group, there are four variables:
        - sensi: the non-zero elements of the footprint
        - ilats, ilons, itims: the lat/lon/time grid indices of the values in sensi
    The grid is dedined spatially by the "latitudes", "longitudes" top-level variables, and temporally by the 
    "tres" and "start" dataset attributes.
    
    The main entry point is the "run" method, which takes three arguments:
        - an instance of the "Observations" class (obs or departures list)
        - an instance of the "Flux" class (fluxes or adjoint field)
        - an action ("forward" or "adjoint")
    """
    def __init__(self, filename):
        self.filename = filename
        try :
            self.ds = File(filename, 'r')
            self.tres = timedelta(seconds=self.attrs['tres'])
            self.start = datetime(*self.attrs['start'])
            self.close = self.ds.close
        except OSError:
            logger.warning(f'Footprint file {self.filename} not found')
            
    def run(self, obs, emis, step):
        """
        Main entry point of the FootprintFile class. 
        * The "obs" argument is an instance of the "Observations" class, and contains as "data" attribute a pandas
          DataFrame with at least the following columns:
           - obsid: the unique id of each obs (which identifies the footprint it corresponds to in the file)
           - dy (in adjoint runs only): a list of model-data mismatches
        * The "emis" argument is an instance of the "Flux" class, and contains either the fluxes to be applied
          in the forward run, or the adjoint structure to where the results of the adjoint are stored.
        * The "step" argument needs to be either "forward" or "adjoint"
        """
        if step == 'forward':
            return self._Forward(obs, emis)
        elif step == 'adjoint':
            return self._Adjoint(obs, emis)
            
    def _set_position(self, emis):
        """
        The ilats/ilons/itims indices may need to be adjusted if the footprints and fluxes are not on the same grid.
        Typically this is needed for the footprints, as their temporal grid is specific to the footprint file.
        Spatial grid shifts may be added here in the future ...
        """
        # Check that the flux and footprints have the same time resolution:
        assert self.tres == emis.tres; f"The time resolution of the footprint file ({self.tres}) and of the fluxes ({emis.tres}) do not match"
        
        # Calculate the temporal shift
        shift_t = (self.start-emis.start).total_seconds()/self.tres
        assert shift_t-int(shift_t) == 0; f"There need to be an integer number of time steps between the start of the footprints ({self.start}) and the start of the emissions ({emis.start})"
        self.shift_t = int(shift_t)
        
        # For now, enforce no spatial shift:
        self.shift_lat = 0
        self.shift_lon = 0
        
        # Store the max values of the indices:
        self.max_t = emis.nt
        self.max_lat = emis.nlat
        self.max_lon = emis.nlon
        
        self.pos_set = True
            
    def _getFootprint(self, obsid):
        """
        This method reads the individual footprints from the footprint file, and apply the required and optional
        coordinate corrections/selection
        """
        # Get the non-zero footprint value as well as its lat/lon/time indices
        ilons = self.ds[obsid]['ilons'][:]+self.shift_lon
        ilats = self.ds[obsid]['ilats'][:]+self.shift_lat
        itims = self.ds[obsid]['itims'][:]+self.shift_t
        sensi = self.ds[obsid]['sensi'][:]
        
        select = (itims >= 0) * (itims < self.max_t) * (ilons >= 0) * (ilons < self.max_ilon) * (ilats >= 0) * (ilats < self.max_ilat)
        
        return {
            'ilons': ilons[select],
            'ilats': ilats[select],
            'itims': itims[select],
            'sensi': sensi[select]
        }
        
    def _Forward(self, observations, emis):
        """
        Compute the (foreground) concentration for a specific obs (given by its obsid) and a surface flux, provided as argument
        """
        self._set_position(emis)
        for obs in observations:
            fpt = self._getFootprint(obs.obsid)
            for cat in emis.categories:
                dy = (emis.data[cat][fpt['itims'], fpt['ilats'], fpt['ilons']]*fpt['sensi']).sum()
                setattr(obs, cat, dy)
        return observations
    
    def _Adjoint(self, obs, emis_adj):
        """
        Compute the adjoint field corresponding to a set of model-data mismatches
        """
        self._set_position(emis_adj)
        fpt = self._getFootprint(obs.obsid)
        for cat in emis_adj.categories :
            emis_adj.data[cat]['emis'][fpt['itimes'], fpt['ilats'], fpt['ilons']] += fpt['sensi'][:]*obs.dy
        return emis_adj
    

class Transport:
    """
    The Transport class controls the forward and adjoint runs, and in particular their optional parallelization.    
    The parallelization is done on the footprint files (each file can itself contain many footprints). It relies on 
    the "ray" module.
    The Transport class also handles the loading in memory of the input files (obs/flux or departures/adjoint flux),
    and the writing to disk of the results.
    The main user entry points are the "run" and "write" methods (which are joint pointers to the _run_mp/_run_serial 
    and _write_f/_write_a methods, defined in the __init__ depending on the value of the "step" and "serial" arguments.
    """
    def __init__(self, obs, flux, step, serial=True):
        """
        - obs: an observation file name (to be read by the "Observations" class)
        - flux: a flux file name (to be read by the "Flux" class)
        - step: "forward" or "adjoint"
        - serial (default True): defines whether a serial or parallell (with ray) run is going to be performed.
        """
        self.obs = Observations(obs)
        self.flux = Flux(flux)
        self.step = step
        self.write = {'forward':self._write_f, 'adjoint':self._write_a}[step]
        self.run = self._run_mp
        if serial :
            self.run = self._run_serial
        
    def _run_mp(self):
        ray.init()
        
        # Create the workers
        footprint_files = unique(self.obs.data.footprint.dropna())
        workers = [FootprintFile.remote(fpf) for fpf in footprint_files]
        
        # Launch them
        results = []
        for w, fpf in zip(workers, footprint_files) :
            obs = self.obs.data.loc[self.obs.data.footprint == fpf]
            results.append(worker.remote.run(obs, self.flux, self.step))
            
        # Gather the results
        for res in results :
            res = ray.get(res)
            if self.step == 'forward':
                self.obs.data[res.index] = res
            elif self.step == 'adjoint':
                self.flux += res
        ray.shutdown()
                
    def _run_serial(self):
        footprint_files = unique(self.obs.data.footprint.dropna())
        for fpf in footprint_files :
            fpf = FootprintFile(fpf)
            obs = self.obs.data.loc[self.obs.data.footprint == fpf.name]
            res = fpf.run(obs, self.flux, self.step)
            if self.step == 'forward':
                self.obs.data[res.index] = res
            elif self.step == 'adjoint':
                self.flux = res

    def _write_f(self, outfile):
        """
        Write output of the forward run (e.g. a DataFrame)
        """
        self.obs.write(outfile)
        
    def _write_a(self, outfile):
        """
        Write output of an adjoint run (e.g. an adjoint flux field)
        """
        self.flux.write(outfile)
        
        
if __name__ == '__main__':
    logger = logging.getLogger(os.path.basename(__file__))
    
    # Read arguments:
    p = ArgumentParser()
    p.add_argument('--forward', '-f', action='store_true', default=False, help="Do a forward run")
    p.add_argument('--adjoint', '-a', action='store_true', default=False, help="Do an adjoint run")
    p.add_argument('--serial', '-s', action='store_true', default=False, help="Run on a single CPU")
    p.add_argument('--obs', required=True)
    p.add_argument('--emis', required=True)
    p.add_argument('--verbosity', '-v', default='INFO')
    args = p.parse_args(sys.argv[1:])
    
    # Create the model :
    assert args.forward != args.adjoint ; "The model must be either forward or adjoint"
    step = 'forward' if args.forward
    step = 'adjoint' if args.adjoint
    model = Transport(args.obs, args.emis, step, serial=self.serial)
    
    # Run (serial or parallel)
    model.run()
        
    # Collect results:
    model.write({'forward':self.obs, "adjoint":self.emis}[step])