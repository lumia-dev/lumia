
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
def getStartTimeForSparqlQuery(pdStartTime, iYr=0):
    '''
    Function getStartTimeForSparqlQuer
    
    @param pdStartTime pandas.timeStamp  date/time from when on observations will be queried
    @returns a DateTimeStamp string of format 'YYYY-01-01T00:00:01.000Z'
                start/end times must be multiples of 12 months starting at New Year's at midnight with a buffer of 1 second either side.
                Note: timeEnd < '2019-01-01T00:00:00.000Z'  fails, but if I set
                it to timeEnd < '2019-01-01T00:00:01.000Z' then it works. 
  '''
    if(not(pdStartTime is None)):
        iYr=int(pdStartTime.strftime('%Y'))
    iYr=iYr - 1
    # pdTimeStart=pdTimeStart - datetime.timedelta(seconds=3) # days, seconds, then other fields
    sTimeStart=str(iYr)+'-12-31T23:59:59.000Z'
    return sTimeStart


# *********************************************************
def getEndTimeForSparqlQuery(pdEndTime,  iYr=0):
    '''
    Function getEndTimeForSparqlQuery
    
    @param pdEndTime pandas.timeStamp  date/time until when on observations will be queried
    @returns a DateTimeStamp string of format 'YYYY-01-01T00:00:01.000Z'
                start/end times must be multiples of 12 months starting at New Year's at midnight with a buffer of 1 second either side.
                Note: timeEnd < '2019-01-01T00:00:00.000Z'  fails, but if I set
                it to timeEnd < '2019-01-01T00:00:01.000Z' then it works. 
  '''
    if(not(pdEndTime is None)):
        iYr=int(pdEndTime.strftime('%Y'))
    iYr=iYr + 1
    # pdTimeEnd=pdTimeEnd + datetime.timedelta(seconds=3) # days, seconds, then other fields
    # sTimeEnd=pdTimeEnd.strftime('%Y-%m-%dT%X.000Z')     # becomes "2018-02-01T00:00:00.000Z"
    sTimeEnd=str(iYr)+'-01-01T00:00:01.000Z'
    return sTimeEnd


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
    sTimeEnd=getEndTimeForSparqlQuery(pdTimeEnd, iYear)
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
def remove_unwanted_characters(myString):
    """removes non-ASCII characters, curly braces, square brackets, CR, LF,  and quotes from the string."""
    # return ''.join(char for char in string if ord(char) < 128) removes all non-ASCII characters
    # neither do we want curly braces, square brackets or quotes 
    return ''.join(char for char in myString if ((ord(char) > 44)and(ord(char) < 123)and(ord(char) !=34)and(ord(char) !=39)and(ord(char) !=91)and(ord(char) !=93)))


# ***********************************************************************************************


def queryCarbonPortal4FluxFileName(cp_path,sKeyword, timeStart, timeEnd,  iRequestedYear,  sScndKeyWord=None, iVerbosityLv=1):
    """
    Function queryCarbonPortal4FluxFileName
    
    @param cp_path  the full path + file name on the ICOS Carbon Portal central storage system holding the 
                                        requested flux information (1-year-record typically)
    @type string
    @param sKeyword :    the type of product we want to query, like VPRM, Edgar or Mikaloff
    @param sScndKeyWord:  additional qualifier for a product we want to query, like NEE (Net Ecosystem Exchange of CO2)
    @type string 
    @param timeStart :  from when on we want to get the observations
    @type datetime
    @param  timeEnd : until when on we want to get the observations
    @type datetime
    @param iVerbosityLv : defines how much detail of program progress is printed to stdout (defaults to 1)
    @type integer between 0 and 3 (optional)

    @return cp_name : the full path + file name on the ICOS Carbon Portal central storage system holding the 
                                        requested flux information (1-year-record typically)
    @rtype string
    
    Attempts to find the corresponding unique-identifier (PID) for the requested data record. This
    relies on a sparql query. 
    Returns full path+name if successful; (empty) string if unsuccessful.

    """ 
    # example: sFileName='VPRM_ECMWF_NEE_2020_CP.nc'
    dobj_L3 = RunSparql(sparql_query=findDobjFromPartialNameAndDate(sKeyword, timeStart, timeEnd, iRequestedYear),output_format='nc').run()
    # Returns VPRM NEE, GEE, and respiration in a string structure, though in this order, as uri, stored in the dobj.value(s):
    # "value" : "https://meta.icos-cp.eu/objects/xLjxG3d9euFZ9SOUj69okhaU" ! VPRM NEE biosphere model result for 2018: net ecosystem exchange of CO2
    bScndKWordFound=True
    if ('mikaloff'==sKeyword[:8]):  # may have a trailing number that we need to remove
        sKeyword='mikaloff'
    sFileNameOnCarbonPortal=None
    sPID=''
    try:
        if len(dobj_L3.split('/')) > 1:
            # TODO: SPARQL in its present form does not allow to provide to combine multiple key words with a logical operator
            # like KeyWord1 AND KeyWord2. Hence dobj_L3 typically returns multiple PIDs and we need to extract the right one.
            # the dobj key/val pair is something like dobj : https://meta.icos-cp.eu/objects/nBGgNpQxPYXBYiBuGGFp2VRF
            # anthropogenic emissions is different.
            # there is only one record, one file name like EDGARv4.3_BP2021_CO2_EU2_2018.nc
            if(sScndKeyWord is not None):
                bScndKWordFound=False
                bGrabNextUrl=False
                if(sKeyword=='anthropogenic'):
                    sExtendedKeyWord=sScndKeyWord+'_'   # e.g. _NEE_ to search for a whole word
                else:
                    sExtendedKeyWord='_'+sScndKeyWord+'_'   # e.g. _NEE_ to search for a whole word
                words=dobj_L3.split("\"") # split at quotation marks
                cwLst=[]
                for word in words:
                    if(re.search('[a-zA-Z]', word) is not None):
                        cwLst.append(word)  # VPRM_ECMWF_NEE_2018_CP.nctimeStart   is a word
                        if(sExtendedKeyWord in word):
                            bScndKWordFound=True
                        if(bScndKWordFound==True):
                            if('dobj' in word):
                                bGrabNextUrl=True
                        if((bGrabNextUrl==True) and ('http' in word)):
                            sPID=word.split('/')[-1]  # Grab the last part of the url without directories
                            # Grab the PID from the end of the http value string, may look like gibberish "nBGgNpQxPYXBYiBuGGFp2VRF"
                            bGrabNextUrl=False
                            break
        else:
            bScndKWordFound=False
        if((bScndKWordFound==False)and (sScndKeyWord is not None)):
            if(sKeyword is None):
                sKeyword='None'
            if(sScndKeyWord is None):
                sScndKeyWord='None'
            logger.error(f"Error in readLv3NcFileFromCarbonPortal(): No matching data records found for Year={iRequestedYear}, 1stKeyword={sKeyword}, 2ndKeyword={sScndKeyWord}")
            return(None)
        # words=dobj_L3.split('/')
        # tmps = (cp_path+dobj_L3.split('/')[-1]).strip()
        # tmps="/data/dataAppStorage/netcdf/xLjxG3d9euFZ9SOUj69okhaU"
        # tmps contains some junk, e.g.: "/data/dataAppStorage/netcdf/cF7K5TwNEt3a1Hdt50TdlBNI\"         }       }     ]   } }"
        # Done earlier now... sFileNameOnCarbonPortal = remove_unwanted_characters(tmps)
        sFileNameOnCarbonPortal = cp_path+sPID
        if(iVerbosityLv>0):
            if(sKeyword is None):
                logger.info(f"Found this PID/Landing page for keyword {sKeyword}: {sFileNameOnCarbonPortal}") # timeStart
            else:
                logger.info(f"Found this PID/Landing page for keywords {sKeyword} and {sScndKeyWord}: {sFileNameOnCarbonPortal}") 
    except:
        logger.error(f"Error in queryCarbonPortal4FluxFileName(): No matching data records found for Year={iRequestedYear} and Keywords= {sKeyword} and {sScndKeyWord}.") 
        # print('The SPARQL query for flux observations for the requested time interval found no matching data records.')
        return(None)
    try:
        # Make sure this file actually exists and is accessible on the portal
        f=open(sFileNameOnCarbonPortal, 'rb')
        f.close()
    except:
        logger.error(f"Error: The file {sFileNameOnCarbonPortal} cannot be read or does not exist on the Carbon Portal or you are not running this script on the Carbon Portal.")
        sFileNameOnCarbonPortal=None
        return('')
    else:
        if(iVerbosityLv>1):
            print("Successfully found the data file "+sFileNameOnCarbonPortal+" on the carbon portal.", flush=True)
    return sFileNameOnCarbonPortal


def queryCarbonPortal4PID(cp_path,sKeyword, timeStart, timeEnd,  iRequestedYear,  sScndKeyWord=None, iVerbosityLv=1):
    """
    Function queryCarbonPortal4PID
    
    @param cp_path  the full path + file name on the ICOS Carbon Portal central storage system holding the 
                                        requested flux information (1-year-record typically)
    @type string
    @param sKeyword :    the type of product we want to query, like VPRM, Edgar or Mikaloff
    @param sScndKeyWord:  additional qualifier for a product we want to query, like NEE (Net Ecosystem Exchange of CO2)
    @type string 
    @param timeStart :  from when on we want to get the observations
    @type datetime
    @param  timeEnd : until when on we want to get the observations
    @type datetime
    @param iVerbosityLv : defines how much detail of program progress is printed to stdout (defaults to 1)
    @type integer between 0 and 3 (optional)

    @return pidUrl : the full url + pid of the data object on the ICOS Carbon Portal central storage system 
    @rtype string
    
    Attempts to find the corresponding unique-identifier (PID) for the requested data record. This
    relies on a sparql query. 
    Returns full path+name if successful; (empty) string if unsuccessful.

    """ 
    # example: sFileName='VPRM_ECMWF_NEE_2020_CP.nc'
    dobj_L3 = RunSparql(sparql_query=findDobjFromPartialNameAndDate(sKeyword, timeStart, timeEnd, iRequestedYear),output_format='nc').run()
    # Returns VPRM NEE, GEE, and respiration in a string structure, though in this order, as uri, stored in the dobj.value(s):
    # "value" : "https://meta.icos-cp.eu/objects/xLjxG3d9euFZ9SOUj69okhaU" ! VPRM NEE biosphere model result for 2018: net ecosystem exchange of CO2
    bScndKWordFound=True
    if ('mikaloff'==sKeyword[:8]):  # may have a trailing number that we need to remove
        sKeyword='mikaloff'
    sFileNameOnCarbonPortal=None
    sPID=''
    try:
        if len(dobj_L3.split('/')) > 1:
            # TODO: SPARQL in its present form does not allow to provide to combine multiple key words with a logical operator
            # like KeyWord1 AND KeyWord2. Hence dobj_L3 typically returns multiple PIDs and we need to extract the right one.
            # the dobj key/val pair is something like dobj : https://meta.icos-cp.eu/objects/nBGgNpQxPYXBYiBuGGFp2VRF
            # anthropogenic emissions is different.
            # there is only one record, one file name like EDGARv4.3_BP2021_CO2_EU2_2018.nc
            if(sScndKeyWord is not None):
                bScndKWordFound=False
                bGrabNextUrl=False
                if(sKeyword=='anthropogenic'):
                    sExtendedKeyWord=sScndKeyWord+'_'   # e.g. _NEE_ to search for a whole word
                else:
                    sExtendedKeyWord='_'+sScndKeyWord+'_'   # e.g. _NEE_ to search for a whole word
                words=dobj_L3.split("\"") # split at quotation marks
                cwLst=[]
                for word in words:
                    if(re.search('[a-zA-Z]', word) is not None):
                        cwLst.append(word)  # VPRM_ECMWF_NEE_2018_CP.nctimeStart   is a word
                        if(sExtendedKeyWord in word):
                            bScndKWordFound=True
                        if(bScndKWordFound==True):
                            if('dobj' in word):
                                bGrabNextUrl=True
                        if((bGrabNextUrl==True) and ('http' in word)):
                            sPID=word.split('/')[-1]  # Grab the last part of the url without directories
                            # Grab the PID from the end of the http value string, may look like gibberish "nBGgNpQxPYXBYiBuGGFp2VRF"
                            bGrabNextUrl=False
                            break
        else:
            bScndKWordFound=False
        if((bScndKWordFound==False)and (sScndKeyWord is not None)):
            if(sKeyword is None):
                sKeyword='None'
            if(sScndKeyWord is None):
                sScndKeyWord='None'
            logger.error(f"Error in queryCarbonPortal4PID(): No matching data records found for Year={iRequestedYear} and Keywords= {sKeyword} and {sScndKeyWord}.") 
            return(None)
        # words=dobj_L3.split('/')
        # tmps = (cp_path+dobj_L3.split('/')[-1]).strip()
        # tmps="/data/dataAppStorage/netcdf/xLjxG3d9euFZ9SOUj69okhaU"
        # tmps contains some junk, e.g.: "/data/dataAppStorage/netcdf/cF7K5TwNEt3a1Hdt50TdlBNI\"         }       }     ]   } }"
        # Done earlier now... sFileNameOnCarbonPortal = remove_unwanted_characters(tmps)
        dobjName="https://meta.icos-cp.eu/objects/"+sPID
        # Use: 
        # dob = Dobj("https://meta.icos-cp.eu/objects/"+sPID)
        # dataFrame = dob.get()    to read the data object
        if(iVerbosityLv>0):
            if(sKeyword is None):
                logger.info(f"Found this PID {dobjName}")
            else:
                logger.info(f"Found this PID {dobjName} for keyword {sKeyword}")
    except:
        logger.error(f"Error in queryCarbonPortal4Pid(): No matching data records found for Year={iRequestedYear} and Keywords= {sKeyword} and {sScndKeyWord}.") 
        return(None)
    return dobjName




# ***********************************************************************************************

def readLv3NcFileFromCarbonPortal(sKeyword, start: datetime=None, end: datetime=None, year=0,  sScndKeyWord=None,  iVerbosityLv=1):
    """
    Function readLv3NcFileFromCarbonPortal
    
    @param sKeyword :    the type of product we want to query, like NEE (Net Ecosystem Exchange of CO2)
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
 
    Attempts to find the corresponding unique-identifier (PID) for the requested data record. 
    The latter should refer to a level3 netcdf file (by name) on the ICOS data portal. 
    The function relies on a sparql query and tries to read the requested netCdf file from the carbon portal. 
    Returns (xarray-dataset) if successful; (None) if unsuccessful.
    """
    #VPRM_ECMWF_GEE_2020_CP.nc
    # find level3 netcdf file with known filename in ICOS CP data portal
    # or use PID directly
    #inputname = path_cp + 'jXPT5pqJgz7MSm5ki95sgqJK'
    if(iVerbosityLv>1):
        print("readLv3NcFileFromCarbonPortal: Looking for %s flux files for year %d." %(sKeyword, year),  flush=True)
    # sFileName='VPRM_ECMWF_NEE_2020_CP.nc'
    # sKeyword='VPRM'
    inputname = queryCarbonPortal4FluxFileName(path_cp,sKeyword, start, end, year, sScndKeyWord,  iVerbosityLv)
    # pidUrl = queryCarbonPortal4PID(path_cp,sKeyword, start, end, year, sScndKeyWord,  iVerbosityLv)
    
        
    if ((inputname is None) or (len(inputname) < 1)):
        logger.error(f"No flux file for keyword >>{sKeyword}<< not found on the ICOS data portal for year {year}") 
        return (None)
    else:
        return(inputname)



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




