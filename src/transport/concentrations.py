#!/usr/bin/env python

from pandas import DataFrame
import xarray as xr
from numpy import interp
from tqdm import tqdm
from lumia.utils import debug


@debug.trace_args()
def read_conc_file(pattern: str):
    ds = xr.open_mfdataset(pattern)
    ds['height_above_ground'] = ds.height_above_reference_ellipsoid - ds.height_above_reference_ellipsoid.sel(hlevel=0)
    return ds


def interp_file(conc : xr.Dataset, endpoints : DataFrame, tracer_name : str, field : str = 'mix_interpolated'):
    """
    This is specific to the CAMS CO2 files (i.e. LMDZ):
    1) interpolate columns at the lat/lon/time of the trajectories endpoints
    2) calculate the mid-layer height (as an average of the lower and upper bound ...), for each column
    3) interpolate the columns at the height of the particles
    4) put everything in a dataframe, group by obsid and calculate the average.
    
    The point 1 and 2 are done for all releases at once using xarray for best performance.
    """
    # Interpolate columns at the particles positions:
    columns = conc.interp(
        longitude = ('particles', endpoints.lon),
        latitude = ('particles', endpoints.lat),
        time = ('particles', endpoints.time)
    )
            
    # Interpolate the columns at the particles height:
    layer = columns.height_above_ground.values
    column_conc = columns[tracer_name].values
    level = (layer[:, :1] + layer[:, :-1]) / 2.
    conc_interp = []
    for ipos in range(len(endpoints)):
        conc_interp.append(
            interp(
                endpoints.height.values[ipos], 
                level[ipos, :], 
                column_conc[ipos, :]
            )
        )
    
    # Calculate the average for each release:
    df = DataFrame.from_dict({field: conc_interp, 'obsid': endpoints.obsid})
    df = df.groupby('obsid').mean()
    return df.reset_index()