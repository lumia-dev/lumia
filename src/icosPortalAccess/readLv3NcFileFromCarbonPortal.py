#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import os
from datetime import datetime
import xarray as xr

#Import ICOS tools:

# latest version: 2022-11-07a

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

# ********************************************

def findDobjFromPartialNameAndDate(sFilenameKeyword, pdTimeStart='', pdTimeEnd=''):
    '''
    Description: Function that returns the data object
                 for a specified level 3 product netcdf filename.

    Input:       partial filename (keyword like VPRM), optional start and end time as pandas time stamps
                    timeStart/timeEnd are of Type pandas._libs.tslibs.timestamps.Timestamp
    Output:      Data Object ID (var_name: "dobj", var_type: String)

   '''
    sFilenameKeyword='VPRM_ECMWF_NEE_2020_CP.nc'
    # First test: sFilenameKeyword is "VPRM"
    timeStart=pdTimeStart.strftime('%Y-%m-%dT%X.000Z')  # becomes "2018-01-01T00:00:00.000Z"
    timeEnd=pdTimeEnd.strftime('%Y-%m-%dT%X.000Z')     # becomes "2018-02-01T00:00:00.000Z"
    timeStart="2020-01-01T00:00:00.000Z"
    timeEnd="2020-12-31T00:00:00.000Z"
    query = '''
        prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
        prefix prov: <http://www.w3.org/ns/prov#>
        prefix xsd: <http://www.w3.org/2001/XMLSchema#>
        select ?dobj ?fileName ?size ?submTime ?timeStart ?timeEnd
        where{
        ?dobj cpmeta:hasObjectSpec <http://meta.icos-cp.eu/resources/cpmeta/biosphereModelingSpatial> .
        ?dobj cpmeta:hasSizeInBytes ?size .
        ?dobj cpmeta:hasName ?fileName .
        ?dobj cpmeta:wasSubmittedBy/prov:endedAtTime ?submTime .
        ?dobj cpmeta:hasStartTime ?timeStart .
        ?dobj cpmeta:hasEndTime ?timeEnd .
        ?dobj cpmeta:hasKeyword "VPRM"^^xsd:string .
        ?dobj cpmeta:hasKeyword "NEE"^^xsd:string .
        FILTER NOT EXISTS {[] cpmeta:isNextVersionOf ?dobj}
        FILTER( ?timeStart > '2017-12-31T00:00:00.000Z'^^xsd:dateTime && ?timeEnd < '2019-01-02T00:00:00.000Z'^^xsd:dateTime)
        FILTER EXISTS {?dobj cpmeta:hasSizeInBytes ?size }
       }
    '''
    return query


# ***********************************************************************************************
def remove_unwanted_characters(string):
    """removes non-ASCII characters, curly braces, square brackets, CR, LF,  and quotes from the string."""
    # return ''.join(char for char in string if ord(char) < 128) removes all non-ASCII characters
    # neither do we want curly braces, square brackets or quotes 
    return ''.join(char for char in string if ((ord(char) > 44)and(ord(char) < 123)and(ord(char) !=34)and(ord(char) !=39)and(ord(char) !=91)and(ord(char) !=93)))


# ***********************************************************************************************


def check_cp(cp_path,sFilenameKeyword, timeStart, timeEnd, iVerbosityLv=1):
    from icoscp.sparql.runsparql import RunSparql
    """ Find the requested level3 netcdf file (by name) on the ICOS data portal (using sparql queries) 
        and directly access that file via the data app """
    cp_name = ''
    # dobj_L3 = RunSparql(sparql_query=findDobjFromName(sFileName),output_format='nc').run()
    dobj_L3 = RunSparql(sparql_query=findDobjFromPartialNameAndDate(sFilenameKeyword, timeStart, timeEnd),output_format='nc').run()
    if len(dobj_L3.split('/')) > 1:
        # the dobj key/val pair is something like dobj : https://meta.icos-cp.eu/objects/nBGgNpQxPYXBYiBuGGFp2VRF
        tmps = (cp_path+dobj_L3.split('/')[-1]).strip()
        # tmps contains some junk, e.g.: "/data/dataAppStorage/netcdf/cF7K5TwNEt3a1Hdt50TdlBNI\"         }       }     ]   } }"
        cp_name = remove_unwanted_characters(tmps)
        # Grab the PID from the end of the value string "nBGgNpQxPYXBYiBuGGFp2VRF"
        if(iVerbosityLv>0):
            print("Found this PID/Landing page: "+cp_name)
    try:
        f=open(cp_name, 'rb')
        f.close()
    except:
        # if not(os.path.isfile(str(cp_name))):
        cp_name = ''
        print('file '+sFileName+' does not exist on the Carbon Portal or you are not running this script on the Carbon Portal.')
        return('')
    else:
        if(iVerbosityLv>1):
            print("Found "+cp_name)
    return cp_name



# ***********************************************************************************************

def readLv3NcFileFromCarbonPortal(sSearchMask, start: datetime, end: datetime, iVerbosityLv=1):
    """Attempts to find the corresponding DOI/unique-identifier for the requested FileName. This
    relies on a sparql query. Tries to read the requested netCdf file from the carbon portal. 
    Returns (xarray-dataset) if successful; (None) if unsuccessful. """
    #VPRM_ECMWF_GEE_2020_CP.nc
    # find level3 netcdf file with known filename in ICOS CP data portal
    # or use PID directly
    #inputname = path_cp + 'jXPT5pqJgz7MSm5ki95sgqJK'
    if(iVerbosityLv>1):
        print("readLv3NcFileFromCarbonPortal: Looking for "+sSearchMask)
    
    # sFileName='VPRM_ECMWF_NEE_2020_CP.nc'
    sFilenameKeyword='VPRM'
    inputname = check_cp(path_cp,sFilenameKeyword, start, end, iVerbosityLv)
    
    # inputname = check_cp(path_cp,sSearchMask, iVerbosityLv)
        
    if len(inputname) < 1:
        print('File not found on the ICOS data portal')
        return (None)
    else:
        # xrDS = xr.open_dataset(inputname)
        # return (xrDS)
        return(inputname)
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




