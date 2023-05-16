
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
from icoscp.cpb import metadata as meta

bDEBUG =False

# GLOBALS
# TODO: observational data is in a different path copmpared to level3 netcdf files....
path_cp = '/data/dataAppStorage/asciiAtcProductTimeSer/'
# https://meta.icos-cp.eu/objects/LLz6BZr6LCt1Pt0w-U_DLxWZ
# /data/dataAppStorage/asciiAtcProductTimeSer/LLz6BZr6LCt1Pt0w-U_DLxWZ.cpb
#
# The pre-processed data used by Lumia (as a-priori) is described e.g. here:
# https://meta.icos-cp.eu/objects/sNkFBomuWN94yAqEXXSYAW54
#
# Observational data available on the ICOS data portal you can discover via the online tool at
# old: https://data.icos-cp.eu/portal/#%7B%22filterCategories%22%3A%7B%22theme%22%3A%5B%22atmosphere%22%5D%2C%22level%22%3A%5B2%5D%2C%22valType%22%3A%5B%22co2MixingRatio%22%5D%2C%22stationclass%22%3A%5B%22ICOS%22%5D%2C%22project%22%3A%5B%22icos%22%5D%2C%22type%22%3A%5B%22atcCo2L2DataObject%22%5D%2C%22variable%22%3A%5B%22http%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fco2atcMoleFrac%22%5D%7D%2C%22filterTemporal%22%3A%7B%22df%22%3A%222017-12-31%22%2C%22dt%22%3A%222018-01-31%22%7D%2C%22tabs%22%3A%7B%22resultTab%22%3A1%7D%7D

# You can try out sparql queries and export the ones you are happy with, with the button
# "Open the SPARQL query basic search result" page - the icon looks like an arrow pointing up and then to the right out of a stylized box.

# However, it is usually better to keep the url rather than the exported resulting script, as the url is better suited to discover new data, whenever new data sets are added. 
# Also, you may want to tidy up the resulting query in a similar fashion to the example provided here, that Oleg prettied up for me:
#
# https://data.icos-cp.eu/portal/#%7B%22filterCategories%22%3A%7B%22valType%22%3A%5B%22co2MixingRatioMolMol%22%2C%22co2MixingRatio%22%5D%2C%22theme%22%3A%5B%22atmosphere%22%5D%7D%7D
#
'''
prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
prefix prov: <http://www.w3.org/ns/prov#>
prefix xsd: <http://www.w3.org/2001/XMLSchema#>
select ?dobj ?samplingHeight ?spec ?fileName ?size ?submTime ?timeStart ?timeEnd
where {
    VALUES ?spec {
        <http://meta.icos-cp.eu/resources/cpmeta/atcCo2L2DataObject>
        <http://meta.icos-cp.eu/resources/cpmeta/atcCo2Product>
        <http://meta.icos-cp.eu/resources/cpmeta/ObspackTimeSerieResult>
    }
    ?dobj cpmeta:hasObjectSpec ?spec .
    ?dobj cpmeta:hasSizeInBytes ?size .
    ?dobj cpmeta:hasName ?fileName .
    ?dobj cpmeta:wasSubmittedBy/prov:endedAtTime ?submTime .
    ?dobj cpmeta:wasAcquiredBy / prov:startedAtTime ?timeStart .
    ?dobj cpmeta:wasAcquiredBy / prov:endedAtTime ?timeEnd .
    OPTIONAL{?dobj cpmeta:wasAcquiredBy / cpmeta:hasSamplingHeight ?samplingHeight }
    FILTER NOT EXISTS {[] cpmeta:isNextVersionOf ?dobj}
FILTER( !(?timeStart > '2018-12-30T23:00:00.000Z'^^xsd:dateTime || ?timeEnd < '2017-12-31T23:00:00.000Z'^^xsd:dateTime) )

}
order by desc(?submTime)
'''



# *****************************************************************************************
def getTimeForSparqlQuery(pdTime,  startOrEndTime=None,  timeStep=None):
    '''
    Function getEndTimeForSparqlQuery
    
    @param pdTime pandas.timeStamp  date/time from/until when observations will be queried
    @returns a DateTimeStamp string of format 'YYYY-01-01T00:00:01.000Z'
    @timeStep is the run.timestep defined in the user's rc configuration file'
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


# ***********************************************************************************************
def getCo2DryMolFractionObjectsFromSparql(pdTimeStart: datetime=None, pdTimeEnd: datetime=None, timeStep=None,iVerbosityLv=1):
    '''
    Function 

    @param pdTimeStart :  from when on we want to get the observations
    @type datetime
    @param pdTimeEnd : until when on we want to get the observations
    @type datetime
    @param iVerbosityLv : defines how much detail of program progress is printed to stdout (defaults to 1)
    @type integer between 0 and 3 (optional)
    @return :  dobj (the data object returned by the SPARQL query)
    @rtype dobj (a structured dictionary of strings)
    '''
    # sTimeStart=getStartTimeForSparqlQuery(pdTimeStart, timeStep)
    sTimeStart=getTimeForSparqlQuery(pdTimeStart,  startOrEndTime='startTime',  timeStep=timeStep)
    # sTimeEnd=getEndTimeForSparqlQuery(pdTimeEnd, timeStep)
    sTimeEnd=getTimeForSparqlQuery(pdTimeEnd,  startOrEndTime='endTime',  timeStep=timeStep)
    query = '''
    prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
    prefix prov: <http://www.w3.org/ns/prov#>
    prefix xsd: <http://www.w3.org/2001/XMLSchema#>
    select ?dobj ?samplingHeight ?spec ?fileName ?size ?submTime ?timeStart ?timeEnd
    where {
        VALUES ?spec {
            <http://meta.icos-cp.eu/resources/cpmeta/atcCo2L2DataObject>
            <http://meta.icos-cp.eu/resources/cpmeta/atcCo2Product>
            <http://meta.icos-cp.eu/resources/cpmeta/ObspackTimeSerieResult>
        }
        ?dobj cpmeta:hasObjectSpec ?spec .
        ?dobj cpmeta:hasSizeInBytes ?size .
        ?dobj cpmeta:hasName ?fileName .
        ?dobj cpmeta:wasSubmittedBy/prov:endedAtTime ?submTime .
        ?dobj cpmeta:wasAcquiredBy / prov:startedAtTime ?timeStart .
        ?dobj cpmeta:wasAcquiredBy / prov:endedAtTime ?timeEnd .
        OPTIONAL{?dobj cpmeta:wasAcquiredBy / cpmeta:hasSamplingHeight ?samplingHeight }
        FILTER NOT EXISTS {[] cpmeta:isNextVersionOf ?dobj}
    FILTER( !(?timeStart > \''''+sTimeEnd+'''\'^^xsd:dateTime || ?timeEnd < \''''+sTimeStart+'''\'^^xsd:dateTime) ) 
    }
    order by desc(?submTime)
    '''
    # example: sFileName='VPRM_ECMWF_NEE_2020_CP.nc'
    logger.info(f'SPARQL query= {query}')    
    dobj = RunSparql(query,output_format='nc').run()
    logger.info(f'dobj= {dobj}')
    return(dobj)



# ***********************************************************************************************
def getDobjFromSparql(tracer='CO2', pdTimeStart: datetime=None, pdTimeEnd: datetime=None, timeStep=None,  sDataType=None,  iVerbosityLv=1):
    '''
    Function  -- for historical and possible future use with ch4. 
    Presently Lumia only uses the function getCo2DryMolFractionObjectsFromSparql()

    @param tracer :    the name of the tracer like co2, ch4, etc.
    @type string 
    @param pdTimeStart :  from when on we want to get the observations
    @type datetime
    @param pdTimeEnd : until when on we want to get the observations
    @type datetime
    @param sDataType optional name of the data type. Default is 'atcCo2L2DataObject'  #'ICOS ATC CO2 Release'
    @type string
    @param iVerbosityLv : defines how much detail of program progress is printed to stdout (defaults to 1)
    @type integer between 0 and 3 (optional)
    @return :  dobj (the data object returned by the SPARQL query)
    @rtype dobj (a structured dictionary of strings)
    '''
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
    logger.info(f'SPARQL query= {query}')    
    dobj = RunSparql(query,output_format='nc').run()
    logger.info(f'dobj= {dobj}')
    return(dobj)



# ***********************************************************************************************
def extractFnamesFromDobj(dobj, cpDir=None, iVerbosityLv=1):
    '''
    Function 

    @param dobj the object returned from the SPARQL query that contains the PIDs of all the files we want to extract
    @cpDir Location where to look for the existance of the files we expect to find from the SPARQL query
    @return a list of strings. Each strings contains the name of one file on the carbon portal that we later need to read.
    @rtype List[strings]
    
    At the bottom of this python file you may find an example of a dobj for co2 observations for 20180101 - 20180201
    It is from such a list of strings that we need to extract each and every PID for the individual files,
    at least one per observation site.
    
    The relevant section looks typically as in this example:
        "dobj" : {
          "type" : "uri",
          "value" : "https://meta.icos-cp.eu/objects/LLz6BZr6LCt1Pt0w-U_DLxWZ"
        },
    And we want to extract "LLz6BZr6LCt1Pt0w-U_DLxWZ"
    '''
    sFileNameOnCarbonPortal=None
    sPID=''
    fNameLst=[]
    if (cpDir is None):
        cpDir=path_cp
    try:
        if len(dobj.split('/')) > 1:
            # 
            bGrabNextUrl=False
            words=dobj.split("\"") # split at quotation marks
            cwLst=[]
            for word in words:
                if(re.search('[a-zA-Z]', word) is not None):
                    cwLst.append(word)  # 
                    if('dobj' in word):
                        bGrabNextUrl=True
                    if((bGrabNextUrl==True) and ('https' in word)):
                        sPID=word.split('/')[-1]  # Grab the last part of the url without directories
                        # Grab the PID from the end of the http value string, may look like gibberish "nBGgNpQxPYXBYiBuGGFp2VRF"
                        bGrabNextUrl=False
                        #  /data/dataAppStorage/asciiAtcProductTimeSer/LLz6BZr6LCt1Pt0w-U_DLxWZ.cpb
                        sFileNameOnCarbonPortal = cpDir+sPID+'.cpb'
                        try:
                            # Make sure this file actually exists and is accessible on the portal
                            f=open(sFileNameOnCarbonPortal, 'rb')
                            f.close()
                            if(iVerbosityLv>0):
                                logger.info(f"Found ICOS co2 observations data file on the portal at {sFileNameOnCarbonPortal}")
                            fNameLst.append(sPID)
                        except:
                            logger.error('The file '+sFileNameOnCarbonPortal+' cannot be read or does not exist on the Carbon Portal or you are not running this script on the Carbon Portal. Please check first of all the directory you provided for observations.file.cpDir in your .yml resource file.')
                            # sys.exit(-1)   /data/dataAppStorage/asciiAtcProductTimeSer/ZZb1E_dJQtRICzobwg0ib86C
    except:
        logger.error("No valid observational data found in SPARQL query dobj=")
        logger.error(f"{dobj}")
        return(None)
    return fNameLst


# /*****************************************************************************************************************/
def getSitecodeCsr(siteCode):
    stationDict={
        'bik':	'dtBI5i', 
        'bir':	'dcBIRi', 
        'bis':	'dcBISi', 
        'brm':	'dtBR5i', 
        'bsd':	'htBS3i', 
        'ces':	'dtCB4i', 
        'cmn' : 'nmCMNi', 
        'crp':	'dsCRPi', 
        'dec':	'dsDECi', 
        'eec':	'dsEECi', 
        'ers':	'hmER2i', 
        'fkl':	'hsFKLi', 
        'gat':	'dtGA5i', 
        'gic': 'hcGICi', 
        'hei':	'duHEIi', 
        'hpb':	'dtHP4i', 
        'htm':	'dtHT3i', 
        'hun':	'dtHU4i', 
        'ipr':	'htIP3i', 
        'jfj':	'nmJFJi', 
        'kre':	'dtKR3i', 
        'lhw':	'hcLHWi', 
        'lin':	'dtLINi', 
        'lmp':	'dsLMPi', 
        'lmu':	'dtLMUi', 
        'lut':	'dcLUTi', 
        'mhd':	'dsMHDi', 
        'mlh':	'hsMLHi', 
        'nor':	'dtNO3i', 
        'ohp':	'htOHPi', 
        'ope':	'dtOP3i', 
        'pal':	'dcPALi', 
        'prs':	'nmPTRi', 
        'pui':	'dtPUIi', 
        'puy':	'nmPUYi', 
        'rgl':	'dtRG2i', 
        'sac':	'hsSA3i', 
        'smr':	'dtSM3i', 
        'ssl':	'nmSI2i', 
        'svb':	'dtSV3i', 
        'tac':	'dtTA3i', 
        'trn':	'dtTR4i', 
        'uto':	'hsUTOi', 
        'wao':	'dsWAOi'    
    }
    if (siteCode in stationDict):
        return(stationDict[siteCode])
    else:
        # TODO: # 'dt'+siteCode+'i'  is not good enough. I need to get the correct names from the CarboScope (CSR) naming convention
        return(None)  


# ***********************************************************************************************
def readObservationsFromCarbonPortal(tracer='CO2', cpDir=None, pdTimeStart: datetime=None, pdTimeEnd: datetime=None, timeStep=None,  sDataType=None,  iVerbosityLv=1):
    """
    Function readObservationsFromCarbonPortal
    
    @param tracer :    the name of the tracer like co2, ch4, etc.
    @type string 
    @param pdTimeStart :  from when on we want to get the observations
    @type datetime
    @param pdTimeEnd : until when on we want to get the observations
    @type datetime
    @param iVerbosityLv : defines how much detail of program progress is printed to stdout (defaults to 1)
    @type integer between 0 and 3 (optional)
    @return ???:  the actual data object? file list? mind you, columns are different....
    @rtype 
 
    readObservationsFromCarbonPortal attempts to find matching ICOS CO2 observations in form of their individual unique-identifier (PID)
    for the requested data records. The latter should refer to a list of level2 netcdf file (by name) on the ICOS data portal. 
    The function relies on a sparql query and tries to read the requested cpb files from the carbon portal using the icoscp package. 
    Returns (...-dataset) if successful; (None) if unsuccessful.
    """
    if(tracer=='CO2'):
        dobj=getCo2DryMolFractionObjectsFromSparql(pdTimeStart=pdTimeStart, pdTimeEnd=pdTimeEnd,  timeStep=timeStep, iVerbosityLv=iVerbosityLv)
    else:        
        dobj=getDobjFromSparql(tracer=tracer, pdTimeStart=pdTimeStart, pdTimeEnd=pdTimeEnd, timeStep=timeStep,  sDataType=sDataType,  iVerbosityLv=iVerbosityLv)
    dobjLst=extractFnamesFromDobj(dobj, cpDir=cpDir, iVerbosityLv=iVerbosityLv)
    logger.debug(f"dobjLst={dobjLst}")
    return(dobjLst, cpDir)


'''
Example of a dobj as returned by the sparql query.

{
  "head" : {
    "vars" : [
      "dobj",
      "hasNextVersion",
      "spec",
      "fileName",
      "size",
      "submTime",
      "timeStart",
      "timeEnd"
    ]
  },
  "results" : {
    "bindings" : [
      {
        "timeEnd" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#dateTime",
          "type" : "literal",
          "value" : "2022-02-28T23:00:00Z"
        },
        "fileName" : {
          "type" : "literal",
          "value" : "ICOS_ATC_L2_L2-2022.1_TRN_50.0_CTS_CO2.zip"
        },
        "timeStart" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#dateTime",
          "type" : "literal",
          "value" : "2016-08-11T00:00:00Z"
        },
        "size" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#long",
          "type" : "literal",
          "value" : "924117"
        },
        "hasNextVersion" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#boolean",
          "type" : "literal",
          "value" : "false"
        },
        "submTime" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#dateTime",
          "type" : "literal",
          "value" : "2022-07-11T11:45:29.264Z"
        },
        "dobj" : {
          "type" : "uri",
          "value" : "https://meta.icos-cp.eu/objects/LLz6BZr6LCt1Pt0w-U_DLxWZ"
        },
        "spec" : {
          "type" : "uri",
          "value" : "http://meta.icos-cp.eu/resources/cpmeta/atcCo2L2DataObject"
        }
      },
      {
        "timeEnd" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#dateTime",
          "type" : "literal",
          "value" : "2022-02-28T23:00:00Z"
        },
        "fileName" : {
          "type" : "literal",
          "value" : "ICOS_ATC_L2_L2-2022.1_TRN_5.0_CTS_CO2.zip"
        },
        "timeStart" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#dateTime",
          "type" : "literal",
          "value" : "2016-08-11T00:00:00Z"
        },
        "size" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#long",
          "type" : "literal",
          "value" : "939308"
        },
        "hasNextVersion" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#boolean",
          "type" : "literal",
          "value" : "false"
        },
        "submTime" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#dateTime",
          "type" : "literal",
          "value" : "2022-07-11T11:45:18.113Z"
        },
        "dobj" : {
          "type" : "uri",
          "value" : "https://meta.icos-cp.eu/objects/NPsg10gQAes_Bw8t7G7CFUVB"
        },
        "spec" : {
          "type" : "uri",
          "value" : "http://meta.icos-cp.eu/resources/cpmeta/atcCo2L2DataObject"
        }
      },
      
# [..] many more entries of the same type .....

      {
        "timeEnd" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#dateTime",
          "type" : "literal",
          "value" : "2022-02-28T23:00:00Z"
        },
        "fileName" : {
          "type" : "literal",
          "value" : "ICOS_ATC_L2_L2-2022.1_PUY_10.0_CTS_CO2.zip"
        },
        "timeStart" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#dateTime",
          "type" : "literal",
          "value" : "2016-08-25T08:00:00Z"
        },
        "size" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#long",
          "type" : "literal",
          "value" : "943125"
        },
        "hasNextVersion" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#boolean",
          "type" : "literal",
          "value" : "false"
        },
        "submTime" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#dateTime",
          "type" : "literal",
          "value" : "2022-07-08T08:40:20.200Z"
        },
        "dobj" : {
          "type" : "uri",
          "value" : "https://meta.icos-cp.eu/objects/xvn9YXe-a0kxPvcJe7sdLOcy"
        },
        "spec" : {
          "type" : "uri",
          "value" : "http://meta.icos-cp.eu/resources/cpmeta/atcCo2L2DataObject"
        }
      },
      {
        "timeEnd" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#dateTime",
          "type" : "literal",
          "value" : "2022-02-28T23:00:00Z"
        },
        "fileName" : {
          "type" : "literal",
          "value" : "ICOS_ATC_L2_L2-2022.1_PAL_12.0_CTS_CO2.zip"
        },
        "timeStart" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#dateTime",
          "type" : "literal",
          "value" : "2017-09-16T00:00:00Z"
        },
        "size" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#long",
          "type" : "literal",
          "value" : "735602"
        },
        "hasNextVersion" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#boolean",
          "type" : "literal",
          "value" : "false"
        },
        "submTime" : {
          "datatype" : "http://www.w3.org/2001/XMLSchema#dateTime",
          "type" : "literal",
          "value" : "2022-07-08T08:39:53.587Z"
        },
        "dobj" : {
          "type" : "uri",
          "value" : "https://meta.icos-cp.eu/objects/YV8lCiSkhsz3W059wr_JxSMg"
        },
        "spec" : {
          "type" : "uri",
          "value" : "http://meta.icos-cp.eu/resources/cpmeta/atcCo2L2DataObject"
        }
      }
    ]
  }
}
'''
