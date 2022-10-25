#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import os
import xarray as xr

#Import ICOS tools:
from icoscp.sparql import sparqls, runsparql
from icoscp.sparql.runsparql import RunSparql


bDEBUG =False

# GLOBALS
# path to netcdf files in CP data app
path_cp = '/data/dataAppStorage/netcdf/'


# ********************************************

def findDobjFromName(filename):
    '''
    Description: Function that returns the data object
                 for a specified level 3 product netcdf filename.

    Input:       netcdf filename

    Output:      Data Object ID (var_name: "dobj", var_type: String)
    '''

    query = '''
        prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
        select ?dobj
        where{
        ?dobj cpmeta:hasName "'''+filename+'''"^^xsd:string .
        FILTER NOT EXISTS {[] cpmeta:isNextVersionOf ?dobj}
        FILTER EXISTS {?dobj cpmeta:hasSizeInBytes ?size }
        }
    '''
    return query


# ***********************************************************************************************


def check_cp(cp_path,sFileName, iVerbosityLv=1):
    """ Find the requested level3 netcdf file (by name) on the ICOS data portal (using sparql queries) 
        and directly access that file via the data app """
    cp_name = ''
    dobj_L3 = RunSparql(sparql_query=findDobjFromName(sFileName),output_format='csv').run()
    if len(dobj_L3.split('/')) > 1:
        # the dobj key/val pair is something like dobj : https://meta.icos-cp.eu/objects/nBGgNpQxPYXBYiBuGGFp2VRF
        cp_name = (cp_path+dobj_L3.split('/')[-1]).strip()
        # Grab the ID from the end of the value streing "nBGgNpQxPYXBYiBuGGFp2VRF"
        if(iVerbosityLv>0):
            print("cp_name= "+cp_name)
    if not(os.path.exists(cp_name)):
        cp_name = ''
        print('file '+sFileName+' does not exist on the Carbon Portal or you are not running this script on the Carbon Portal.')
    else:
        if(iVerbosityLv>1):
            print("Found "+cp_name)
    return cp_name



# ***********************************************************************************************

def readLv3NcFileFromCarbonPortal(sFileName, iVerbosityLv=1):
    """Attempts to find the corresponding DOI/unique-identifier for the requested FileName. This
    relies on a sparql query. Tries to read the requested netCdf file from the carbon portal. 
    Returns (True, xarray-dataset) if successful; (False, None) if unsuccessful. """
    #sFileName='VPRM_ECMWF_NEE_2020_CP.nc'
    #VPRM_ECMWF_GEE_2020_CP.nc
    # find level3 netcdf file with known filename in ICOS CP data portal
    inputname = check_cp(path_cp,sFileName, iVerbosityLv)
    
    # or use PID directly 
    #inputname = path_cp + 'jXPT5pqJgz7MSm5ki95sgqJK'
    if(iVerbosityLv>1):
        print("readLv3NcFileFromCarbonPorta: Looking for "+inputname)
    
    if len(inputname) < 1:
        print('File not found on the ICOS data portal')
        return (False, None)
    else:
        xrDS = xr.open_dataset(inputname)
        return (True, xrDS)
    # In[5]:
    # xrDS
    # In[6]:
    # xrDS['GEE'][:][:]



# ***********************************************************************************************

from optparse import OptionParser
class CmdArgs():
    """
    Read arguments from the command line.
    """
    def __init__(self):
        """
        constructs the CmdArgs object
        """
        parser = OptionParser(description="readLv3NcFileFromCarbonPortal.py [options]                          version: 0.0.1\n Reads a netCdf file from the carbon portal \
    Returns (True, xarray-dataset) if successful; (False, None) if unsuccessful.\n\
    \n", epilog="Example:\n readLv3NcFileFromCarbonPortal.py -f VPRM_ECMWF_GEE_2020_CP.nc ")
        parser.add_option("-f","--desiredFile", dest="cFileName", default=None, help="Provide a common file name after ICOS convention.")
        parser.add_option("-v","--iVerbosityLv",  dest="iVerbosityLv", type=int, default=1,  help="iVerbosity level (0..2, default=1). ")
        self.parser = parser
        (options, args) = parser.parse_args()

        # Update the class dictionary with cmd line options.
        self.__dict__.update(options.__dict__)
        # strings may come in as Unicode which gdal cannot handle - make sure strings are plain strings
        if (None != self.cFileName):
            self.cFileName=str(self.cFileName)
    def print_help(self):
        self.parser.print_help()


# ***********************************************************************************************

if __name__ == '__main__':
    """ Main script of readLv3NcFileFromCarbonPortal (for testing).  The typical use is to
    jump directly to the readLv3NcFileFromCarbonPortal() function from anothger script."""
    #sFileName='VPRM_ECMWF_NEE_2020_CP.nc'
    #VPRM_ECMWF_GEE_2020_CP.nc
    sFileName = 'VPRM_ECMWF_GEE_2020_CP.nc'
    cmdargs = CmdArgs()
    if (cmdargs.cFileName is None) :
        cmdargs.print_help()
        #sys.exit(0)
        sFileName=cmdargs.cFileName
    else:
        readLv3NcFileFromCarbonPortal(sFileName, cmdargs.iVerbosityLv)




