#!/usr/bin/env python
from .Tools import Region, Categories, colorize
from numpy import *
import logging
import os
from netCDF4 import Dataset
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from copy import deepcopy

    
class Interface:
    data_initialized = False
    def __init__(self, rcf, struct=None):
        self.rcf = rcf
        self.categories = Categories(rcf)
        if struct is not None :
            self.setup(struct, info=True)
        self.writeStruct = WriteStruct
        
    def setup(self, struct=None, info=False):
        self.data = struct
        if info :
            for cat in self.categories :
                logging.info('Total for category %s: %i umol/m2/s'%(cat, self.data[cat.name]['emis'].sum()))
        self.data_initialized = True
        
    def StructToVec(self, struct=None, method='MonthlyFlux'):
        if struct is None and self.data_initialized :
            struct = self.data
        else :
            logging.critical("No data in memory to form a control vector ...")
            raise RuntimeError
        
        self._stv = getattr(self, '_stv_%s'%method) # TODO: check if the "self" can be avoided here ...
        self.VecToStruct = getattr(self, '_vts_%s'%method)
        self.VecToStruct_adj = getattr(self, '_vts_%s_adj'%method)
        return self._stv(struct)
    
    ###################################
    # Specific classes
    
    def _stv_MonthlyFlux(self, struct):
        statevec = []
        for cat in [x for x in self.categories if x.optimize]:
            emcat = coarsenTime(struct[cat.name], cat.optimization_interval, compute_std=False)
            for i_time, tt in enumerate(sorted(emcat['time_interval']['time_start'])):
                for i_lat in range(emcat['emis'].shape[1]):
                    for i_lon in range(emcat['emis'].shape[2]):
                        statevec.append(emcat['emis'][i_time, i_lat, i_lon])
        return array(statevec)
    
    def _vts_MonthlyFlux(self, vector):
        # TODO: split this in two, with the refining in a second time?
        i_state = 0
        
        fine_data = deepcopy(self.prior_data)
        for cat in [x for x in catInfo if x.optimize]:
            coarse_data = coarsenTime(self.prior_data[cat.name], cat.optimization_interval, compute_std=False)
            for i_time, tt in enumerate(sorted(coarse_data['time_interval']['time_start'])):
                for i_lat in range(data['emis'].shape[1]):
                    for i_lon in range(data['emis'].shape[2]):
                        coarse_data['emis'][i_time, i_lat, i_lon] = statevec[i_state]
                        i_state += 1
            fine_data[cat.name]['emis'] = refineTime(coarse_data, fine_data[cat.name])
        return fine_data
    
    def _vts_MonthlyFlux_adj(self, adjstruct):
        dt = {'y':relativedelta(years=1), 'm':relativedelta(months=1), 'd':timedelta(1)}
        adjvec = array(())

        for cat in [x for x in self.categories if x.optimize]:
            #1) Adjoint of refineTime (#TODO: take this out of stateToStruct_adj)
            adjCat = refineTime_adj(
                deepcopy(adjstruct[cat.name]),
                self.prior_data[cat.name],
                dt[cat.optimization_interval]
            )

            # 2) Coarsen:
            adjCat = coarsenTime(adjCat, cat.optimization_interval)

            # 3) Fill in the state:
            stateCat = adjCat['emis'].reshape(-1)
            adjvec = append(adjvec, stateCat)

        return adjvec

            
    
def coarsenTime(emis, interval, compute_std=False):
    intervals_emis = emis['time_interval']['time_start']
    if interval == 'y' :
        intervals_optim = [datetime(x.year, 1, 1) for x in intervals_emis]
        dt = relativedelta(years=1)
    elif interval == 'm':
        intervals_optim = [datetime(x.year, x.month, 1) for x in intervals_emis]
        dt = relativedelta(months=1)
    elif interval == 'd' :
        intervals_optim = [datetime(x.year, x.month, x.day) for x in intervals_emis]
        dt = timedelta(1)
    else :
        raise NotImplementedError
    emisOut = []
    emisOut_std = []
    emisOut_min = []
    emisOut_max = []
    intervals_optim = array(intervals_optim)
    for tt in unique(intervals_optim):
        try :
            emisOut.append(emis['emis'][intervals_optim == tt, :, :].sum(0))
        except :
            import pdb; pdb.set_trace()
        nt = sum(intervals_optim == tt)
        if compute_std :
            emisOut_std.append(emis['emis'][intervals_optim == tt, :, :].std(0)*nt)
            emisOut_min.append(emis['emis'][intervals_optim == tt, :, :].min(0)*nt)
            emisOut_max.append(emis['emis'][intervals_optim == tt, :, :].max(0)*nt)
    em = {'time_interval':{}}
    em['emis'] = array(emisOut)
    em['time_interval']['time_start'] = unique(intervals_optim)
    em['time_interval']['time_end'] = unique(intervals_optim)+dt
    if compute_std :
        return em, {'std':array(emisOut_std), 'min':array(emisOut_min), 'max':array(emisOut_max)}
    return em


def refineTime(coarseEmis, fineEmis):
    """
    Restore the high resolution temporal structure of the fluxes, from that of a previous time step.
    With:
        - x the monthly total flux at one grid point, 
	- f(t) the flux at time step t,
	- nt the number of time steps in a month
	- f0(t) the previous (or prior, it does not matter) flux at time step t:
    f(t) = x/nt + f0 - x0/nt

    x is provided in coarseEmis,
    f0 and x0 are deduced from fineEmis
    nt is computed
    """
    f = deepcopy(fineEmis)
    nt_optim = len(coarseEmis['time_interval']['time_start'])
    tstart_emis = fineEmis['time_interval']['time_start']
    for i_time in range(nt_optim):
        tmin = coarseEmis['time_interval']['time_start'][i_time]
        tmax = coarseEmis['time_interval']['time_end'][i_time]
        select = (tstart_emis >= tmin)*(tstart_emis < tmax)
        nt = sum(select)+0.   # Make sure we have a real
        x1 = coarseEmis['emis'][i_time, :, :]
        f0 = fineEmis['emis'][select, :, :]
        x0 = f0.sum(0)
        f['emis'][select, :, :] = x1/nt + f0 - x0/nt

#	offset = fineEmis['emis'][select, :, :]-fineEmis['emis'][select, :, :].mean(0)
#	fineEmis['emis'][select, :, :] = coarseEmis['emis'][i_time, :, :]/nt + offset
    return f['emis']


def refineTime_adj(adjEmis, fineEmis, dt_optim):
    intervals_adjEmis = adjEmis['time_interval']['time_start']
    if dt_optim == relativedelta(years=1):
        intervals_optim = array([datetime(x.year, 1, 1) for x in intervals_adjEmis])
    elif dt_optim == relativedelta(months=1):
        intervals_optim = array([datetime(x.year, x.month, 1) for x in intervals_adjEmis])
    elif dt_optim == timedelta(1) :
        intervals_optim = array([datetime(x.year, x.month, x.day) for x in intervals_adjEmis])
    else :
        raise NotImplementedError
    for i_time, time in enumerate(unique(intervals_optim)) :
        select = intervals_optim == time
        nt = sum(select)+0.  # Make sure we have a real
        adjEmis['emis'][select, :, :] /= nt
    return adjEmis


def WriteStruct(data, path, prefix):
    """
    Write the model input (control parameters)
    """

    # Create the filename and directory (if needed)
    filename = os.path.join(path, '%s.nc'%prefix)
    if not os.path.exists(path):
        os.makedirs(path)

    # Write to a netCDF format
    with Dataset(filename, 'w') as ds :
        ds.createDimension('time_components', 6)
        for cat in data.keys():
            gr = ds.createGroup(cat)
            gr.createDimension('nt', data[cat]['emis'].shape[0])
            gr.createDimension('nlat', data[cat]['emis'].shape[1])
            gr.createDimension('nlon', data[cat]['emis'].shape[2])
            gr.createVariable('emis', 'd', ('nt', 'nlat', 'nlon'))
            gr['emis'][:] = data[cat]['emis']
            gr.createVariable('times_start', 'i', ('nt', 'time_components'))
            gr['times_start'][:] = array([x.timetuple()[:6] for x in data[cat]['time_interval']['time_start']])
            gr.createVariable('times_end', 'i', ('nt', 'time_components'))
            gr['times_end'][:] = array([x.timetuple()[:6] for x in data[cat]['time_interval']['time_end']])
    return filename


def ReadStruct(filename):
    with Dataset(filename) as ds :
        categories = ds.groups.keys()
        data = {'cat_list':categories}
        for cat in categories :
            data[cat] = {
                'emis':ds.groups[cat].variables['emis'][:],
                'time_interval':{
                    'time_start': array([datetime(*x) for x in ds.groups[cat].variables['times_start'][:]]),
                    'time_end': array([datetime(*x) for x in ds.groups[cat].variables['times_end'][:]]),
                }
            }
    return data