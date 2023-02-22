
#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import os
import re
import datetime
from loguru import logger

#Import ICOS tools:
# from icoscp.sparql import sparqls, runsparql
from icoscp.sparql.runsparql import RunSparql

# latest version: 2022-11-07a

bDEBUG =False

# GLOBALS
# path to netcdf files in CP data app
path_cp = '/data/dataAppStorage/netcdf/'

#
# The pre-processed data used by Lumia (as a-priori) is described e.g. here:
# https://meta.icos-cp.eu/objects/sNkFBomuWN94yAqEXXSYAW54
# There you can find links to the anthropogenic (EDGARv4.3), ocean (Mikaloff-Fletcher 2007) and to the diagnostic biosphere model VPRM
#


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

# *******************************************************
def getStartTimeForSparqlQuery(pdStartTime, timeStep=None):
    '''
    Function getStartTimeForSparqlQuer
    
    @param pdStartTime pandas.timeStamp  date/time from when on observations will be queried
    @returns a DateTimeStamp string of format 'YYYY-01-01T00:00:01.000Z'
                start/end times must be multiples of 12 months starting at New Year's at midnight with a buffer of 1 second either side.
                Note: timeEnd < '2019-01-01T00:00:00.000Z'  fails, but if I set
                it to timeEnd < '2019-01-01T00:00:01.000Z' then it works. 
  '''
    if(pdStartTime is None):
        logger.error('Start date of observation interval not provided.')
        sys.exit(-1)
    # TODO: instead of always beginning 1/Jan at midnight we could take the exact start time minus one time step (to be on the safe side)
    iYr=int(pdStartTime.strftime('%Y'))
    iYr=iYr - 1
    sTimeStart=str(iYr)+'-12-31T23:59:59.000Z'
    return sTimeStart


# *********************************************************
def getTimeForSparqlQuery(pdTime,  startOrEndTime=None,  timeStep=None):
    '''
    Function getEndTimeForSparqlQuery
    
    @param pdTime pandas.timeStamp  date/time until when on observations will be queried
    @returns a DateTimeStamp string of format 'YYYY-01-01T00:00:01.000Z'
                start/end times must be multiples of 12 months starting at New Year's at midnight with a buffer of 1 second either side.
                Note: timeEnd < '2019-01-01T00:00:00.000Z'  fails, but if I set
                it to timeEnd < '2019-01-01T00:00:01.000Z' then it works. 
  '''
    if((pdTime is None) or (startOrEndTime is None)):
        logger.error('Start date of observation interval or startOrEndTime operator not provided.')
        sys.exit(-1)
    if('start' in startOrEndTime):
        operation='subtractTime'
    else:
        operation='addTime'
    delta=int(60)
    if((timeStep is not None) and (len(timeStep)>1)):
        if('h' in timeStep):
            delta=int(timeStep[:-1])*60
        elif('m' in timeStep):
            delta=int(timeStep[:-1])
        elif('d' in timeStep):
            delta=int(timeStep[:-1])*60*24
    if('add' in operation):
        endTime=pdTime + datetime.timedelta(minutes=delta)            
    else:
        endTime=pdTime - datetime.timedelta(minutes=delta)            
    sSparqlTime=str(endTime)
    sSparqlTime=sSparqlTime[:10]+'T'+sSparqlTime[11:19]+'.000Z'
    return sSparqlTime



# ********************************************
    
def findDobjFromPartialNameAndDate(sKeyword, pdTimeStart=None, pdTimeEnd=None,  iYear=0):
    '''
    Function    findDobjFromPartialNameAndDate

    @param       partial filename (keyword like VPRM), optional start and end times
                        timeStart/timeEnd are of Type pandas._libs.tslibs.timestamps.Timestamp
    @returns      Data Object ID (var_name: "dobj", var_type: String)
    Description: Function that returns the data object
                        for a specified level 3 product netcdf filename.
   '''
    # sFilename='VPRM_ECMWF_NEE_2020_CP.nc'
    # First test: sKeyword is "VPRM"
    # sKeyword='VPRM'
    # sFilename2ndKeyword='NEE'
    #    ?dobj cpmeta:hasKeyword "'''+sFilename2ndKeyword+'''"^^xsd:string .
    # From RC file:
    #    [Fluxes]
    #    emissions.co2.categories : biosphere, fossil, ocean
    
    #    #  #VPRM_ECMWF_GEE_2020_CP.nc return the following from sparqle:
    #      cp_name= https://meta.icos-cp.eu/objects/xLjxG3d9euFZ9SOUj69okhaU
    
    #    emissions.co2.fossil.origin         : @EDGARv4.3_BP2019
    #        https://meta.icos-cp.eu/collections/XgzB--i8yDQYIPk4tf0l970K     Gerbig
    #        https://meta.icos-cp.eu/collections/unv31HYRKgullLjJ99O5YCsG    Ute
    
    #    emissions.co2.biosphere.origin      : @VPRM
    
    #    emissions.co2.ocean.origin          : @mikaloff01
    #    emissions.co2.interval              : 1h
    #    emissions.co2.prefix                : flux_co2.
    
    sTimeStart=getStartTimeForSparqlQuery(pdTimeStart, iYear)
    sTimeEnd=getTimeForSparqlQuery(pdTimeEnd,  startOrEndTime='endTime',  timeStep=None)  # getEndTimeForSparqlQuery(pdTimeEnd, iYear)
    if(sKeyword=='VPRM'):
        sDataType='biosphereModelingSpatial'
    if(sKeyword=='anthropogenic'):
        sDataType='co2EmissionInventory' # ForCo2
    query = '''
        prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
        prefix prov: <http://www.w3.org/ns/prov#>
        prefix xsd: <http://www.w3.org/2001/XMLSchema#>
        select ?dobj ?fileName ?size ?submTime ?timeStart ?timeEnd
        where{
        ?dobj cpmeta:hasObjectSpec <http://meta.icos-cp.eu/resources/cpmeta/'''+sDataType+'''> .
        ?dobj cpmeta:hasSizeInBytes ?size .
        ?dobj cpmeta:hasKeyword "'''+sKeyword+'''"^^xsd:string .
        ?dobj cpmeta:hasName ?fileName .
        ?dobj cpmeta:wasSubmittedBy/prov:endedAtTime ?submTime .
        ?dobj cpmeta:hasStartTime ?timeStart .
        ?dobj cpmeta:hasEndTime ?timeEnd .
        FILTER NOT EXISTS {[] cpmeta:isNextVersionOf ?dobj}
        FILTER( ?timeStart > "'''+sTimeStart+'''"^^xsd:dateTime && ?timeEnd <"'''+sTimeEnd+'''"^^xsd:dateTime)
        FILTER EXISTS {?dobj cpmeta:hasSizeInBytes ?size }
       }
    '''
    #    ?dobj cpmeta:hasKeyword "VPRM"^^xsd:string .
    # https://data.icos-cp.eu/portal/#%7B%22filterCategories%22%3A%7B%22project%22%3A%5B%22misc%22%5D%2C%22type%22%3A%5B%22co2EmissionInventory%22%5D%2C%22submitter%22%3A%5B%22oCP%22%5D%2C%22level%22%3A%5B3%5D%7D%7D
    return query
#         FILTER( ?timeStart > '2019-12-31T23:59:59.000Z'^^xsd:dateTime && ?timeEnd < '2021-01-01T00:00:01.000Z'^^xsd:dateTime)
#        ?dobj cpmeta:hasKeyword "VPRM"^^xsd:string .
#        ?dobj cpmeta:hasKeyword "NEE"^^xsd:string .
# station query - can do multiple combined queries says Anders


# ***********************************************************************************************
def readObservationsFromCarbonPortal(tracer='CO2', pdTimeStart: datetime=None, pdTimeEnd: datetime=None, timeStep=None,  sDataType=None,  iVerbosityLv=1):
    """
    FunctionreadObservationsFromCarbonPortal
    
    @param sKeyword :    the type of product we want to query, like NEE (Net Ecosystem Exchange of CO2)
    @type string 
    @param tracer :    the name of the tracer like co2, ch4, etc.
    @type string 
    @param start :  from when on we want to get the observations
    @type datetime
    @param end : until when on we want to get the observations
    @type datetime
    @param year :  alternatively provide the calendar year
    @type int
    @param iVerbosityLv : defines how much detail of program progress is printed to stdout (defaults to 1)
    @type integer between 0 and 3 (optional)
    @return inputname : the full path + file name on the ICOS Carbon Portal central storage system holding the 
                                        requested flux information (1-year-record typically)
    @rtype string
 
    Attempts to find matching ICOS CO2 observations in form of their individual unique-identifier (PID) for the requested data record. 
    The latter should refer to a level2(?) netcdf file (by name) on the ICOS data portal. 
    The function relies on a sparql query and tries to read the requested netCdf file from the carbon portal. 
    Returns (xarray-dataset) if successful; (None) if unsuccessful.
    """
    # sTimeStart=getStartTimeForSparqlQuery(pdTimeStart, timeStep)
    sTimeStart=getTimeForSparqlQuery(pdTimeStart,  startOrEndTime='startTime',  timeStep=timeStep)
    # sTimeEnd=getEndTimeForSparqlQuery(pdTimeEnd, timeStep)
    sTimeEnd=getTimeForSparqlQuery(pdTimeEnd,  startOrEndTime='endTime',  timeStep=timeStep)
    LcTracer=tracer.lower()
    if(sDataType is None):
        sDataType='atcCo2L2DataObject'  #'ICOS ATC CO2 Release'

    #=findDobjFromPartialNameAndDate(sKeyword, timeStart, timeEnd, iRequestedYear)
    query = '''
    prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
    prefix prov: <http://www.w3.org/ns/prov#>
    prefix xsd: <http://www.w3.org/2001/XMLSchema#>
    select ?dobj ?hasNextVersion ?spec ?fileName ?size ?submTime ?timeStart ?timeEnd
    where {
        VALUES ?spec {<http://meta.icos-cp.eu/resources/cpmeta/'''+sDataType+'''>}
        ?dobj cpmeta:hasObjectSpec ?spec .
        BIND(EXISTS{[] cpmeta:isNextVersionOf ?dobj} AS ?hasNextVersion)
        VALUES ?station {<http://meta.icos-cp.eu/resources/stations/AS_PAL> <http://meta.icos-cp.eu/resources/stations/AS_TRN> <http://meta.icos-cp.eu/resources/stations/AS_GAT> <http://meta.icos-cp.eu/resources/stations/AS_HPB> <http://meta.icos-cp.eu/resources/stations/AS_IPR> <http://meta.icos-cp.eu/resources/stations/AS_OPE> <http://meta.icos-cp.eu/resources/stations/AS_KIT> <http://meta.icos-cp.eu/resources/stations/AS_SMR> <http://meta.icos-cp.eu/resources/stations/AS_SAC> <http://meta.icos-cp.eu/resources/stations/AS_ZEP> <http://meta.icos-cp.eu/resources/stations/AS_TOH> <http://meta.icos-cp.eu/resources/stations/AS_KRE> <http://meta.icos-cp.eu/resources/stations/AS_SVB> <http://meta.icos-cp.eu/resources/stations/AS_HTM> <http://meta.icos-cp.eu/resources/stations/AS_JFJ> <http://meta.icos-cp.eu/resources/stations/AS_PUY> <http://meta.icos-cp.eu/resources/stations/AS_NOR> <http://meta.icos-cp.eu/resources/stations/AS_LIN>}
                ?dobj cpmeta:wasAcquiredBy/prov:wasAssociatedWith ?station .
        ?dobj cpmeta:hasSizeInBytes ?size .
    ?dobj cpmeta:hasName ?fileName .
    ?dobj cpmeta:wasSubmittedBy/prov:endedAtTime ?submTime .
    ?dobj cpmeta:hasStartTime | (cpmeta:wasAcquiredBy / prov:startedAtTime) ?timeStart .
    ?dobj cpmeta:hasEndTime | (cpmeta:wasAcquiredBy / prov:endedAtTime) ?timeEnd .
        FILTER NOT EXISTS {[] cpmeta:isNextVersionOf ?dobj}
    FILTER( !(?timeStart > \''''+sTimeEnd+'''\'^^xsd:dateTime || ?timeEnd < \''''+sTimeStart+'''\'^^xsd:dateTime) ) 
        {
            {FILTER NOT EXISTS {?dobj cpmeta:hasVariableName ?varName}}
            UNION
            {
                ?dobj cpmeta:hasVariableName ?varName
                FILTER (?varName = "'''+LcTracer+'''")
            }
        }
    }
    order by desc(?submTime)
    offset 0 limit 20
    '''
    # example: sFileName='VPRM_ECMWF_NEE_2020_CP.nc'
    dobj_L3 = RunSparql(query,output_format='nc').run()
    logger.info(f'dobj_L3= {dobj_L3}')
    # Returns VPRM NEE, GEE, and respiration in a string structure, though in this order, as url, stored in the dobj.value(s):
    # "value" : "https://meta.icos-cp.eu/objects/xLjxG3d9euFZ9SOUj69okhaU" ! VPRM NEE biosphere model result for 2018: net ecosystem exchange of CO2
    print('readObservationsFromCarbonPortal() is not implemented yet.',  flush=True)
    return




# ***********************************************************************************************
def remove_unwanted_characters(string):
    """removes non-ASCII characters, curly braces, square brackets, CR, LF,  and quotes from the string."""
    # return ''.join(char for char in string if ord(char) < 128) removes all non-ASCII characters
    # neither do we want curly braces, square brackets or quotes 
    return ''.join(char for char in string if ((ord(char) > 44)and(ord(char) < 123)and(ord(char) !=34)and(ord(char) !=39)and(ord(char) !=91)and(ord(char) !=93)))


# ***********************************************************************************************



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




