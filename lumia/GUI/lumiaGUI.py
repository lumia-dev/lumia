#!/usr/bin/env python3

import os
#import io
import sys
import pandas as pd
import argparse
from datetime import datetime,  timedelta
import yaml
import time
import _thread
import re
from loguru import logger
#from pandas.api.types import is_float_dtype
import customtkinter as ctk
import tkinter as tk
import tkinter.font as tkFont
#import literalString as litstr
# from tkinter import ttk


def grabFirstEntryFromList(myList):
  try:
    return myList[0]  
  except:
    return myList

class AsLiteralString(str):
  pass

def representAsLiteralString(dumper, data):
  return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="") # , style='|' is a bad idea here

yaml.add_representer(str, representAsLiteralString)
yaml.representer.SafeRepresenter.add_representer(str, representAsLiteralString) 

def swapListElements(list, pos1, pos2):
    # pos1 and pos2 should be counted from zero
    if(pos1 != pos2):
        list[pos1], list[pos2] = list[pos2], list[pos1]
    return list

def formatMyString(inString, minLen, units=""):
    ''' Make the incoming string at least as long as minLen by adding spaces to its left and add the optional units'''
    if(inString is None) or (minLen is None):
        return""
    minLen=minLen - len(units)
    if(len(inString)<1) or (minLen < 1):
        return""
    outStr=inString
    while (len(outStr) < minLen):
        outStr=' '+outStr
    return(outStr+units)

def nestedKeyExists(element, *keys):
    '''
    Check if *keys (nested) exists in `element` (dict).
    '''
    if not isinstance(element, dict):
        raise AttributeError('nestedKeyExists() expects dict as first argument.')
    if len(keys) == 0:
        raise AttributeError('nestedKeyExists() expects at least two arguments, one given.')

    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True

def extract_numbers_from_string(s):
    # This regular expression pattern matches integers and floating-point numbers.
    pattern = r"[-+]?[.]?[d]+(?:,ddd)[.]?d(?:[eE][-+]?d+)?"
    numbers = re.findall(pattern, s)
    
    # Convert the extracted strings to either int or float.
    for i in range(len(numbers)):
        if "." in numbers[i] or "e" in numbers[i] or "E" in numbers[i]:
            numbers[i] = float(numbers[i])
        else:
            numbers[i] = int(numbers[i])
    
    return numbers

def calculateEstheticFontSizes(sFontFamily,  iAvailWidth,  iAvailHght, sLongestTxt,  nCols=1, nRows=1,
                                                        xPad=20,  yPad=10,  maxFontSize=20,  bWeCanStackColumns=False):
    FontSize=int(14)  # for normal text
    font = tkFont.Font(family=sFontFamily, size=FontSize)
    tooSmall=8 # pt
    bCanvasTooSmall=False
    bWeMustStack=False
    colW=int(iAvailWidth/nCols)-(0.5*xPad)+0.5
    colH=int( iAvailHght/nRows)-(0.5*yPad)+0.5
    logger.debug(f"avail colWidth= {colW} pxl: colHeight= {colH} pxl")
    (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
    logger.debug(f"{sFontFamily}, {FontSize}pt: (w={w},h={h})pxl")
    # Make the font smaller until the text fits into one column width
    while(w>colW):
        FontSize=FontSize-1
        font = tkFont.Font(family=sFontFamily, size=FontSize)
        (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
        if (FontSize==tooSmall):
            FontSize=FontSize+1
            font = tkFont.Font(family=sFontFamily, size=FontSize)
            bCanvasTooSmall=True
            (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
            break
        logger.debug(f"{sFontFamily}, {FontSize}pt: (w={w},h={h})pxl")
    # If the screen is too small, check if we could stack the output vertically using fewer columns
    if((bCanvasTooSmall) and (bWeCanStackColumns) and (nCols>1)):
        nCols=int((nCols*0.5)+0.5)
        font = tkFont.Font(family=sFontFamily, size=FontSize)
        (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
        if(w<colW+1):
            bCanvasTooSmall=False
            bWeMustStack=True
            
    # Now make the font as big as we can
    bestFontSize=FontSize
    while((FontSize<maxFontSize)): # and (not bMaxReached) ):
        FontSize=FontSize+1
        font = tkFont.Font(family=sFontFamily, size=FontSize)
        (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
        if(w<=colW):
            bestFontSize=FontSize
    FontSize=bestFontSize
    (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
    logger.debug(f"{sFontFamily} {FontSize}: (w={w},h={h})pxl")
    if(h>colH):
        logger.debug("We may need a vertical scrollbar...")
    fsNORMAL=FontSize # 12
    fsTINY=int((9*FontSize/fsNORMAL)+0.5)  # 9
    fsSMALL=int((10*FontSize/fsNORMAL)+0.5)  # 10
    fsLARGE=int((15*FontSize/fsNORMAL)+0.5)  # 15
    fsHUGE=int((19*FontSize/fsNORMAL)+0.5)  # 19
    fsGIGANTIC=int((24*FontSize/fsNORMAL)+0.5)  # 24
    return(fsTINY,  fsSMALL,  fsNORMAL,  fsLARGE,  fsHUGE,  fsGIGANTIC,  bWeMustStack)


# LumiaGui Class =============================================================
class LumiaGui(ctk.CTk):
    # =====================================================================
    # The layout of the window will be written
    # in the init function itself
    def __init__(self, sLogCfgPath, ymlContents, ymlFile,  sNow):  # *args, **kwargs):
        #super().__init__(*args, **kwargs)
        # self.protocol("WM_DELETE_WINDOW", self.closed) 

        # Get the screen resolution to scale the GUI in the smartest way possible...
        if os.environ.get('DISPLAY','') == '':
            print('no display found. Using :0.0')
            os.environ.__setitem__('DISPLAY', ':0.0')
        root=ctk.CTk()
        #root = tk.Tk()
        if(os.path.isfile("LumiaGui.stop")):
            sCmd="rm LumiaGui.stop"
            self.runSysCmd(sCmd,  ignoreError=True)
        if(os.path.isfile("LumiaGui.go")):
            sCmd="rm LumiaGui.go"
            self.runSysCmd(sCmd,  ignoreError=True)

        screenWidth = root.winfo_screenwidth()
        screenHeight = root.winfo_screenheight()
        if((screenWidth/screenHeight) > (1920/1080.0)):  # multiple screens?
            screenWidth=int(screenHeight*(1920/1080.0))
        maxW = int(0.92*screenWidth)
        maxW = 1650 # TODO: remove
        maxH = int(0.92*screenHeight)
        nCols=8 # sum of labels and entry fields per row
        nRows=12 # number of rows in the GUI
        xPadding=int(0.008*maxW)
        wSpacer=int(2*0.008*maxW)
        yPadding=int(0.008*maxH)
        vSpacer=int(2*0.008*maxH)
        myFontFamily="Georgia"
        sLongestTxt="Start date (00:00h):"  # "Latitude (≥33°N):"
        (fsTINY,  fsSMALL,  fsNORMAL,  fsLARGE,  fsHUGE,  fsGIGANTIC,  bWeMustStackColumns)= \
            calculateEstheticFontSizes(myFontFamily,  maxW,  maxH, sLongestTxt, nCols, nRows, xPad=xPadding, 
                                                        yPad=yPadding, maxFontSize=20,  bWeCanStackColumns=False)
        hDeadSpace=wSpacer+(nCols*xPadding*2)+wSpacer
        vDeadSpace=2*yPadding #vSpacer+(nRows*yPadding*2)+vSpacer
        colWidth=int((maxW - hDeadSpace)/(nCols*1.0))
        rowHeight=int((maxH - vDeadSpace)/(nRows*1.0))
        # Dimensions of the window
        appWidth, appHeight = maxW, maxH
        #activeTextColor='gray10'
        #inactiveTextColor='gray50'
        
        self.lonMin = tk.DoubleVar(value=-25.0)
        self.lonMax = tk.DoubleVar(value=45.0)
        self.latMin = tk.DoubleVar(value=23.0)
        self.latMax = tk.DoubleVar(value=83.0)
 
        # Sets the title of the window to "LumiaGui"
        root.title("LUMIA run configuration")  
        # Sets the dimensions of the window to 800x900
        root.geometry(f"{appWidth}x{appHeight}")   

        # Row 0:  Title Label
        # ################################################################

        self.LatitudesLabel = ctk.CTkLabel(root,
                                text="LUMIA  --  Configure your next LUMIA run",  font=("Arial MT Extra Bold",  fsGIGANTIC))
        self.LatitudesLabel.grid(row=0, column=0,
                            columnspan=8,padx=xPadding, pady=yPadding,
                            sticky="ew")

        # Row 1:  Headings left/right column
        # ################################################################

        root.LatitudesLabel = ctk.CTkLabel(root, anchor="w",
                                text="Time interval",  font=("Georgia",  fsLARGE))
        root.LatitudesLabel.grid(row=1, column=0,  
                            columnspan=2, padx=xPadding, pady=yPadding,
                            sticky="ew")
 
        # Row 2: Time interval
        # ################################################################

        self.TimeStartLabel = ctk.CTkLabel(root, anchor="w",
                                text="Start date (00:00h):", width=colWidth,  font=("Georgia",  fsNORMAL))
        self.TimeStartLabel.grid(row=2, column=0, 
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")

        self.TimeEndLabel = ctk.CTkLabel(root,
                                text="End date: (23:59h)", width=colWidth,  font=("Georgia",  fsNORMAL))
        self.TimeEndLabel.grid(row=2, column=2,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")
        # Time Entry Fields
        # Get the current values from the yamlFile or previous user entry:
        
        startDate=ymlContents['observations']['start'][:10]  # e.g. '2018-01-01 00:00:00'
        endDate=ymlContents['observations']['end'][:10]   # e.g. '2018-02-01 23:59:59'
        self.sStartDate=tk.StringVar(value=startDate)
        self.sEndDate=tk.StringVar(value=endDate)

        s=self.sStartDate.get()
        # prep the StartDate field  
        self.TimeStartEntry = ctk.CTkEntry(root,textvariable=self.sStartDate,  
                          placeholder_text=ymlContents['observations']['start'][:10], width=colWidth)
        self.TimeStartEntry.grid(row=2, column=1,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="ew")
                            
        # prep the EndDate field
        self.TimeEndEntry = ctk.CTkEntry(root,textvariable=self.sEndDate, 
                          placeholder_text=ymlContents['observations']['end'][:10], width=colWidth)
        self.TimeEndEntry.grid(row=2, column=3,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="ew")
        # Lat/Lon
        self.LatitudesLabel = ctk.CTkLabel(root, anchor="w",
                                text="Geographical extent of the area modelled (in deg. North/East)",  font=("Georgia",  fsLARGE))
        self.LatitudesLabel.grid(row=2, column=4,
                            columnspan=4,padx=xPadding, pady=yPadding,
                            sticky="ew")

                            
        # Row 3: 
        # ################################################################

        # ObservationsFileLocation
        self.iObservationsFileLocation= tk.IntVar(value=1)
        #iObservationsFileLocation.set(1) # Read observations from local file
        if ('CARBONPORTAL' in ymlContents['observations']['file']['location']):
            self.iObservationsFileLocation.set(2)
        self.ObsFileLocationLocalRadioButton = ctk.CTkRadioButton(root,
                                   text="Observational CO2 data from local file", font=("Georgia",  fsNORMAL),
                                   variable=self.iObservationsFileLocation,  value=1)
        self.ObsFileLocationLocalRadioButton.grid(row=3, column=0,
                                  columnspan=3, padx=xPadding, pady=yPadding,
                                  sticky="ew")

        # row 3: Latitudes Label
        txt=f"Latitude (≥{self.latMin.get()}°N):" # "Latitude (≥33°N):"
        self.LatitudeMinLabel = ctk.CTkLabel(root,anchor="w",
                                text=txt, width=colWidth,  font=("Georgia",  fsNORMAL))
        self.LatitudeMinLabel.grid(row=3, column=4,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")
        self.LatitudeMaxLabel = ctk.CTkLabel(root,
                                text="max (≤73°N):", width=colWidth,  font=("Georgia",  fsNORMAL))
        self.LatitudeMaxLabel.grid(row=3, column=6,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")
 
        # Latitudes Entry Fields
        # min
        # gridString=ymlContents['run']['grid']  # '${Grid:{lon0:-15, lat0:33, lon1:35, lat1:73, dlon:0.25, dlat:0.25}}'

        Lat0=ymlContents['run']['region']['lat0']  # 33.0
        Lat1=ymlContents['run']['region']['lat1']   #73.0
        Lon0=ymlContents['run']['region']['lon0']  # -15.0
        Lon1=ymlContents['run']['region']['lon1']   #35.0
        s=str(Lat0)
        s=f'{Lat0:.3f}'
        self.sLat0=tk.StringVar(value=s)
        self.sLat1=tk.StringVar(value=f'{Lat1:.2f}')
        self.sLon0=tk.StringVar(value=f'{Lon0:.2f}')
        self.sLon1=tk.StringVar(value=f'{Lon1:.2f}')
        
        # grid: ${Grid:{lon0:-15, lat0:33, lon1:35, lat1:73, dlon:0.25, dlat:0.25}}
        # sRegion="lon0=%.3f, lon1=%.3f, lat0=%.3f, lat1=%.3f, dlon=%.3f, dlat=%.3f, nlon=%d, nlat=%d"%(regionGrid.lon0, regionGrid.lon1,  regionGrid.lat0,  regionGrid.lat1,  regionGrid.dlon,  regionGrid.dlat,  regionGrid.nlon,  regionGrid.nlat)

        self.Latitude0Entry = ctk.CTkEntry(root,textvariable=self.sLat0,
                          placeholder_text=self.sLat0, width=colWidth)
        self.Latitude0Entry.grid(row=3, column=5,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="ew")
        # max
        self.Latitude1Entry = ctk.CTkEntry(root,textvariable=self.sLat1,
                          placeholder_text=self.sLat1, width=colWidth)
        self.Latitude1Entry.grid(row=3, column=7,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="ew")
                 
        # Row 4
        # ################################################################
        # 

        #  Entry LocalFile
        self.observationsFilePathLabel = ctk.CTkLabel(root,
                                text="obs.file.path:", width=colWidth,  font=("Georgia",  fsNORMAL))
        self.observationsFilePathLabel.grid(row=4, column=0,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")
        self.ObsFileLocationLocalEntry = ctk.CTkEntry(root,
                          placeholder_text= ymlContents['observations']['file']['path'], width=colWidth)
        self.ObsFileLocationLocalEntry.grid(row=4, column=1,
                            columnspan=3, padx=xPadding,
                            pady=yPadding, sticky="ew")

        # row 4 Longitudes  my_string = f'{my_float:.3f}'
        self.LongitudeMinLabel = ctk.CTkLabel(root, text="Longitude (≥-15°E):", anchor="w", font=("Georgia",  fsNORMAL), width=colWidth)
        self.LongitudeMinLabel.grid(row=4, column=4,
                           padx=xPadding, pady=yPadding,
                           sticky="ew")
        self.LongitudeMaxLabel = ctk.CTkLabel(root, text="max (≤35°E):",  font=("Georgia",  fsNORMAL), width=colWidth)
        self.LongitudeMaxLabel.grid(row=4, column=6,
                           padx=xPadding, pady=yPadding,
                           sticky="ew")
        # Longitude0 Entry Field
        # min
        self.Longitude0Entry = ctk.CTkEntry(root, textvariable=self.sLon0, 
                            placeholder_text=self.sLon0, width=colWidth)
        self.Longitude0Entry.grid(row=4, column=5,
                           columnspan=1, padx=xPadding,
                           pady=yPadding, sticky="ew")
        # max
        self.Longitude1Entry = ctk.CTkEntry(root,textvariable=self.sLon1,
                            placeholder_text=self.sLon1, width=colWidth)
        self.Longitude1Entry.grid(row=4, column=7,
                           columnspan=1, padx=xPadding,
                           pady=yPadding, sticky="ew")
        
        # Row 5
        # ################################################################
        # 
        self.ObsFileLocationCPortalRadioButton = ctk.CTkRadioButton(root,
                                   text="Obsdata Ranking", font=("Georgia",  fsNORMAL), 
                                   variable=self.iObservationsFileLocation,  value=2)
        self.ObsFileLocationCPortalRadioButton.grid(row=5, column=0,
                                  columnspan=2, padx=xPadding, pady=yPadding,
                                  sticky="ew")

        # Filter stations?
        self.ActivateStationFiltersRbVar = tk.IntVar(value=1)
        if ((ymlContents['observations']['filters']['bStationAltitude'])or(ymlContents['observations']['filters']['bSamplingHeight'])):
            self.ActivateStationFiltersRbVar.set(2)
        self.ActivateStationFiltersRadioButton = ctk.CTkRadioButton(root,
                                   text="Filter stations", font=("Georgia",  18), 
                                   variable=self.ActivateStationFiltersRbVar,  value=2)
        self.ActivateStationFiltersRadioButton.grid(row=5, column=2,
                                  columnspan=1, padx=xPadding, pady=yPadding,
                                  sticky="ew")
        self.useAllStationsRadioButton = ctk.CTkRadioButton(root,
                                   text="Use all stations", font=("Georgia",  18), 
                                   variable=self.ActivateStationFiltersRbVar,  value=1)
        self.useAllStationsRadioButton.grid(row=5, column=3,
                                  columnspan=1, padx=xPadding, pady=yPadding,
                                  sticky="ew")
                                
        # Emissions data (a prioris)
        self.LatitudesLabel = ctk.CTkLabel(root, anchor="w",
                                text="Emissions data (a priori)",  font=("Georgia",  fsLARGE))
        self.LatitudesLabel.grid(row=5, column=4,  
                            columnspan=3, padx=xPadding, pady=yPadding,
                            sticky="ew")

        # Row 6
        # ################################################################
        # 
        # ObservationsFileLocation
        rankingList=ymlContents['observations']['file']['ranking']
        self.ObsFileRankingTbxVar = tk.StringVar(value="ObsPack")
        self.ObsFileRankingBox = ctk.CTkTextbox(root,
                                         width=colWidth,
                                         height=(2*rowHeight+vSpacer))
        self.ObsFileRankingBox.grid(row=6, column=0,
                             columnspan=1, rowspan=3, padx=xPadding,
                             pady=yPadding, sticky="nsew")
        txt=""
        rank=4
        for sEntry in  rankingList:
            txt+=str(rank)+': '+sEntry+'\n'
            rank=rank-1
        self.ObsFileRankingBox.insert('0.0', txt)  # insert at line 0 character 0

        # Station altitude filter
        # bTest=ymlContents['observations']['filters']['bStationAltitude']
        self.bFilterStationAltitudeCkbVar = tk.BooleanVar(value=ymlContents['observations']['filters']['bStationAltitude'])
        self.filterStationAltitudeCkb = ctk.CTkCheckBox(root,
                                text="Station altitudes:",  font=("Georgia",  18), 
                                variable=self.bFilterStationAltitudeCkbVar,
                                onvalue=True, offvalue=False)  
        self.filterStationAltitudeCkb.grid(row=6, column=1,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")
        stationMinAlt = ymlContents['observations']['filters']['stationMinAlt']     # in meters amsl
        stationMaxAlt = ymlContents['observations']['filters']['stationMaxAlt']  # in meters amsl
        self.stationMinAlt=tk.StringVar(value=f'{stationMinAlt}')
        self.stationMaxAlt=tk.StringVar(value=f'{stationMaxAlt}')
        # min Altitude Entry
        self.stationMinAltEntry = ctk.CTkEntry(root,textvariable=self.stationMinAlt,
                          placeholder_text=self.stationMinAlt, width=colWidth)
        self.stationMinAltEntry.grid(row=6, column=2,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="ew")
        # max Altitude Entry
        self.stationMaxAltEntry = ctk.CTkEntry(root,textvariable=self.stationMaxAlt,
                          placeholder_text=self.stationMaxAlt, width=colWidth)
        self.stationMaxAltEntry.grid(row=6, column=3,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="ew")
                             

        # Emissions data (a prioris)
        self.observationsFilePathLabel = ctk.CTkLabel(root,
                                text="Land/Vegetation NEE:", width=colWidth,  font=("Georgia",  fsNORMAL))
        self.observationsFilePathLabel.grid(row=6, column=4,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")

        # Land/Vegetation Net Exchange combo box
        self.LandNetExchangeModelCkbVar = tk.StringVar(value=ymlContents['emissions']['co2']['categories']['biosphere'])
        self.LandNetExchangeOptionMenu = ctk.CTkOptionMenu(root,
                                        values=["LPJ-GUESS","VPRM"],  dropdown_font=("Georgia",  fsNORMAL), 
                                        variable=self.LandNetExchangeModelCkbVar)
        self.LandNetExchangeOptionMenu.grid(row=6, column=5,
                                        padx=xPadding, pady=yPadding,
                                        columnspan=2, sticky="ew")
 

        # Row 7
        # ################################################################
        # 

        # inlet height filter
        self.bFilterSamplingHeightCkbVar = tk.BooleanVar(value=ymlContents['observations']['filters']['bSamplingHeight'])
        self.filterSamplingHeightCkb = ctk.CTkCheckBox(root,
                                text="Sampling height:",  font=("Georgia",  fsNORMAL), 
                                variable=self.bFilterSamplingHeightCkbVar, 
                                onvalue=True, offvalue=False)  
        self.filterSamplingHeightCkb.grid(row=7, column=1,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")
        inletMinHght = ymlContents['observations']['filters']['inletMinHeight']     # in meters amsl
        inletMaxHght = ymlContents['observations']['filters']['inletMaxHeight']  # in meters amsl
        self.inletMinHght=tk.StringVar(value=f'{inletMinHght}')
        self.inletMaxHght=tk.StringVar(value=f'{inletMaxHght}')
        # min inlet height
        self.inletMinHghtEntry = ctk.CTkEntry(root,textvariable=self.inletMinHght,
                          placeholder_text=self.inletMinHght, width=colWidth)
        self.inletMinHghtEntry.grid(row=7, column=2,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="ew")
        # max inlet height
        self.inletMaxHghtEntry = ctk.CTkEntry(root,textvariable=self.inletMaxHght,
                          placeholder_text=self.inletMaxHght, width=colWidth)
        self.inletMaxHghtEntry.grid(row=7, column=3,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="ew")


        # Emissions data (a prioris)
        self.observationsFilePathLabel = ctk.CTkLabel(root,
                                text="Fossil emissions:", width=colWidth,  font=("Georgia",  fsNORMAL))
        self.observationsFilePathLabel.grid(row=7, column=4,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")
        # Fossil emissions combo box
        # Latest is presently https://meta.icos-cp.eu/collections/GP-qXikmV7VWgG4G2WxsM1v3
        self.FossilEmisCkbVar = tk.StringVar(value=ymlContents['emissions']['co2']['categories']['fossil']) #"EDGARv4_LATEST"
        self.FossilEmisOptionMenu = ctk.CTkOptionMenu(root, dropdown_font=("Georgia",  fsNORMAL), 
                                        values=["EDGARv4_LATEST","EDGARv4.3_BP2021_CO2_EU2_2020","EDGARv4.3_BP2021_CO2_EU2_2019", "EDGARv4.3_BP2021_CO2_EU2_2018"], 
                                        variable=self.FossilEmisCkbVar)
                #  https://hdl.handle.net/11676/Ce5IHvebT9YED1KkzfIlRwDi (2022) https://doi.org/10.18160/RFJD-QV8J EDGARv4.3 and BP statistics 2023
                # https://hdl.handle.net/11676/6i-nHIO0ynQARO3AIbnxa-83  EDGARv4.3_BP2021_CO2_EU2_2020.nc  
                # https://hdl.handle.net/11676/ZU0G9vak8AOz-GprC0uY-HPM  EDGARv4.3_BP2021_CO2_EU2_2019.nc
                # https://hdl.handle.net/11676/De0ogQ4l6hAsrgUwgjAoGDoy EDGARv4.3_BP2021_CO2_EU2_2018.nc
        self.FossilEmisOptionMenu.grid(row=7, column=5,
                                        padx=xPadding, pady=yPadding,
                                        columnspan=2, sticky="ew")
        

        # Row 8
        # ################################################################
        # 
        # Emissions data (a prioris)
        self.observationsFilePathLabel = ctk.CTkLabel(root,
                                text="Ocean net exchange:", width=colWidth,  font=("Georgia",  fsNORMAL))
        self.observationsFilePathLabel.grid(row=8, column=4,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")
        # Ocean Net Exchange combo box
        self.OceanNetExchangeCkbVar = tk.StringVar(value=ymlContents['emissions']['co2']['categories']['ocean'])  # "mikaloff01"
        self.OceanNetExchangeOptionMenu = ctk.CTkOptionMenu(root,
                                        values=["mikaloff01"], dropdown_font=("Georgia",  fsNORMAL), 
                                        variable=self.OceanNetExchangeCkbVar)
        self.OceanNetExchangeOptionMenu.grid(row=8, column=5,
                                        padx=xPadding, pady=yPadding,
                                        columnspan=2, sticky="ew")
 

        # Row 9
        # ################################################################
        # 
        self.TuningParamLabel = ctk.CTkLabel(root,
                                text="LUMIA may adjust:", width=colWidth,  font=("Georgia",  fsNORMAL))
        self.TuningParamLabel.grid(row=9, column=0,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")
        #self.LandVegCkbVar = tk.StringVar(value="Vegetation")
        #self.LandVegCkb = ctk.CTkCheckBox(root,
        #                     text="Land/Vegetation",
        #                     variable=self.LandVegCkbVar,
        #                     onvalue="Vegetation", offvalue="c1")
        self.LandVegCkbVar = tk.BooleanVar(value=True)
        self.LandVegCkb = ctk.CTkCheckBox(root,
                             text="Land/Vegetation", font=("Georgia",  fsNORMAL), 
                             variable=self.LandVegCkbVar,
                             onvalue=True, offvalue=False)
        self.LandVegCkb.grid(row=9, column=1,
                          padx=xPadding, pady=yPadding,
                          sticky="ew")

        self.FossilCkbVar = tk.BooleanVar(value=False)
        self.FossilCkb = ctk.CTkCheckBox(root,
                            text="Fossil (off)", font=("Georgia",  fsNORMAL),
                            variable=self.FossilCkbVar,
                             onvalue=True, offvalue=False)                             
        self.FossilCkb.grid(row=9, column=2,
                          padx=xPadding, pady=yPadding,
                          sticky="ew")

        self.OceanCkbVar = tk.BooleanVar(value=False)
        self.OceanCkb = ctk.CTkCheckBox(root,
                            text="Ocean (off)", font=("Georgia",  fsNORMAL),
                            variable=self.OceanCkbVar,
                             onvalue=True, offvalue=False)                            
        self.OceanCkb.grid(row=9, column=3,
                          padx=xPadding, pady=yPadding,
                          sticky="ew")

        #self.bIgnoreWarningsCkbVar = tk.BooleanVar(value=False) # tk.NORMAL
        #self.ignoreWarningsCkb = ctk.CTkCheckBox(root,state=tk.DISABLED, 
        #                    text="Ignore Warnings", font=("Georgia",  fsNORMAL),
        #                    text_color=inactiveTextColor, text_color_disabled=activeTextColor, 
        #                    variable=self.bIgnoreWarningsCkbVar,
        #                     onvalue=True, offvalue=False)                            
        #self.ignoreWarningsCkb.grid(row=9, column=7,
        #                  padx=xPadding, pady=yPadding,
        #                  sticky="ew")

       # Row 10
        # ################################################################
        # 
        # Text Box
        self.displayBox = ctk.CTkTextbox(root, width=200,
                                        text_color="red", font=("Georgia",  fsSMALL),  height=100)
        self.displayBox.grid(row=10, column=0, columnspan=6,rowspan=2, 
                             padx=20, pady=20, sticky="nsew")
                             
        #self.displayBox.delete("0.0", "end")  # delete all text
        self.displayBox.configure(state=tk.DISABLED)  # configure textbox to be read-only

        def GuiClosed():
            if tk.messagebox.askokcancel("Quit", "Is it OK to abort your Lumia run?"):
                logger.info("LumiaGUI was canceled.")
                sCmd="touch LumiaGui.stop"
                self.runSysCmd(sCmd)
                #root.destroy
                global LOOP_ACTIVE
                LOOP_ACTIVE = False
        root.protocol("WM_DELETE_WINDOW", GuiClosed)
        def CancelAndQuit(): 
            logger.info("LumiaGUI was canceled.")
            sCmd="touch LumiaGui.stop"
            self.runSysCmd(sCmd)
            #self.bPleaseCloseTheGui.set(True)
            global LOOP_ACTIVE
            LOOP_ACTIVE = False

        # Cancel Button
        self.CancelButton = ctk.CTkButton(master=root, font=("Georgia", 18), text="Cancel",
            command=CancelAndQuit)
        self.CancelButton.grid(row=10, column=7,
                                        columnspan=1, padx=xPadding,
                                        pady=yPadding, sticky="ew")

        # Row 11  :  RUN Button
        self.bIgnoreWarningsCkbVar = tk.BooleanVar(value=False) # tk.NORMAL
        self.ignoreWarningsCkb = ctk.CTkCheckBox(root,state=tk.DISABLED, 
                            text="Ignore Warnings", font=("Georgia",  fsNORMAL),
                            variable=self.bIgnoreWarningsCkbVar, text_color='gray5',  text_color_disabled='gray70', 
                             onvalue=True, offvalue=False)                            
        self.ignoreWarningsCkb.grid(row=9,  column=7, 
                          padx=xPadding, pady=yPadding,
                          sticky="nw")

        def GoButtonHit():
            # def generateResults(self):
            bGo=False
            (bErrors, sErrorMsg, bWarnings, sWarningsMsg) = self.checkGuiValues(ymlContents=ymlContents, sNow=sNow)
            self.displayBox.configure(state=tk.NORMAL)  # configure textbox to be read-only
            self.displayBox.delete("0.0", "end")  # delete all text
            if((bErrors) and (bWarnings)):
                self.displayBox.insert("0.0", "Please fix the following errors:\n"+sErrorMsg+sWarningsMsg)
            elif(bErrors):
                self.displayBox.insert("0.0", "Please fix the following errors:\n"+sErrorMsg)
            elif(bWarnings):
                self.displayBox.insert("0.0", "You chose to ignore the following warnings:\n"+sWarningsMsg)
                if(self.bIgnoreWarningsCkbVar.get()):
                    bGo=True
                self.ignoreWarningsCkb.configure(state=tk.NORMAL)
            else:
                bGo=True
            self.displayBox.configure(state=tk.DISABLED)  # configure textbox to be read-only
    
            if(bGo):
                # Save  all details of the configuration and the version of the software used:
                #current_date = datetime.now()
                #sNow=current_date.isoformat("T","minutes")
                try:
                    with open(ymlFile, 'w') as outFile:
                        yaml.dump(ymlContents, outFile)
                except:
                    sTxt=f"Fatal Error: Failed to write to text file {ymlFile} in local run directory. Please check your write permissions and possibly disk space etc."
                    CancelAndQuit(sTxt)

                sLogCfgFile=sLogCfgPath+"Lumia-runlog-"+sNow+"-config.yml"    
                sCmd="cp "+ymlFile+" "+sLogCfgFile
                self.runSysCmd(sCmd)
                sCmd="touch LumiaGui.go"
                self.runSysCmd(sCmd)
                logger.info("Done. LumiaGui completed successfully. Config and Log file written.")
                # self.bPleaseCloseTheGui.set(True)
                global LOOP_ACTIVE
                LOOP_ACTIVE = False
                
        self.RunButton = ctk.CTkButton(root, font=("Georgia", fsNORMAL), 
                                         text="RUN",
                                         command=GoButtonHit)
        self.RunButton.grid(row=11, column=7,
                                        columnspan=1, padx=xPadding,
                                        pady=yPadding, sticky="ew")
                                        
        def loop_function():
            global LOOP_ACTIVE
            LOOP_ACTIVE = True
            while LOOP_ACTIVE:
                #print(chk.state())
                time.sleep(3)
            logger.info("Closing the GUI...")
            #root.destroy()
            root.event_generate("<Destroy>")
        _thread.start_new_thread(loop_function, ())
        root.mainloop()     
        #root.destroy()
        return
        

    def AskUserAboutWarnings(self):
        GoBackAndFixIt=True
        return(GoBackAndFixIt)
    
 
    # This function is used to get the selected
    # options and text from the available entry
    # fields and boxes and then generates
    # a prompt using them
    def checkGuiValues(self, ymlContents, sNow):
        bErrors=False
        sErrorMsg=""
        bWarnings=False
        sWarningsMsg=""

        # Get the Start- & End Dates/Times and do some sanity checks 
        # start format: '2018-01-01 00:00:00'    
        bTimeError=False
        strStartTime=self.TimeStartEntry.get()
        strEndTime=self.TimeEndEntry.get()
        if (len(strStartTime)<10):
            bTimeError=True
            sErrorMsg+='Invalid Start Date entered.\n'
        if (len(strStartTime)<10):
            bTimeError=True
            sErrorMsg+='Invalid End Date entered.\n'

        if (not bTimeError): 
            strStartTime=strStartTime[0:10]+' 00:00:00'
            strEndTime=strEndTime[0:10]+' 23:59:59'
            try:
                # date_obj = datetime.strptime(strStartTime[0:10], '%Y-%m-%d')
                tStart=pd.Timestamp(strStartTime)
            except:
                bTimeError=True
                sErrorMsg+='Invalid or corrupted Start Date entered. Please use the ISO format YYY-MM-DD when entering dates.\n'
            try:
                tEnd=pd.Timestamp(strEndTime)
            except:
                bTimeError=True
                sErrorMsg+='Invalid or corrupted End Date entered. Please use the ISO format YYY-MM-DD when entering dates.\n'
        if (not bTimeError): 
            current_date = datetime.now()
            rightNow=current_date.isoformat("T","minutes")
            tMin=pd.Timestamp('1970-01-01 00:00:00')
            tMax=pd.Timestamp(rightNow[0:10]+' 23:59:59')
            if(tStart < tMin):
                bWarnings=True
                sWarningsMsg+='It is highly unusual that your chosen Start Date is before 1970-01-01. Are you sure?!\n'
            if(tEnd > tMax):
                bTimeError=True
                sErrorMsg+='Cristal balls to look into the future are not scientifically approved. The best this code is capable of is Today or any date in the past after the StartDate.\n'
            if(tStart+timedelta(days=1) > tEnd): # we need a minimum time span of 1 day. 
                bTimeError=True
                sErrorMsg+='May I kindly request you to chose an End Date minimum 1 day AFTER the Start Date? Thanks dude.\n'
        if(bTimeError):
            bErrors=True
        else:    
            ymlContents['observations']['start'] = tStart.strftime('%Y-%m-%d %H:%M:%S')
            ymlContents['observations']['end'] = tEnd.strftime('%Y-%m-%d %H:%M:%S')
            
        # Get the latitudes & langitudes of the selected region and do some sanity checks 
        bLatLonError=False
        Lat0=float(self.Latitude0Entry.get())
        Lat1=float(self.Latitude1Entry.get())
        Lon0=float(self.Longitude0Entry.get())
        Lon1=float(self.Longitude1Entry.get())

        if(Lat0 < float(self.latMin.get())):
            bLatLonError=True
            sErrorMsg+=f"Error: Region cannot extend below {self.latMin.get()}°N.\n"
        if(Lat1 > float(self.latMax.get())):
            bLatLonError=True
            sErrorMsg+=f"Error: Region cannot extend above {self.latMax.get()}°N.\n"
        if(Lat0 > Lat1):
            bLatLonError=True
            sErrorMsg+=f"Maximum latitude {self.latMax.get()}°N cannot be smaller than minimum latitude {self.latMin.get()}°N.\n"
        if(Lon0 < float(self.lonMin.get())):
            bLatLonError=True
            sErrorMsg+=f"Error: Region cannot extend west of {self.lonMin.get()}°E.\n"
        if(Lon1 > float(self.lonMax.get())):
            bLatLonError=True
            sErrorMsg+=f"Error: Region cannot extend east of {self.lonMax.get()}°E.\n"
        if(Lon0 > Lon1):
            bLatLonError=True
            sErrorMsg+=f"Most eastern longitude {self.lonMax.get()}°E cannot be west of the minimum longitude {self.lonMin.get()}°E.\n"
        if(bLatLonError):
            bErrors=True
        else:
            ymlContents['run']['region']['lat0'] = Lat0
            ymlContents['run']['region']['lat1'] = Lat1
            ymlContents['run']['region']['lon0'] = Lon0
            ymlContents['run']['region']['lon1'] = Lon1
            # run:  grid: ${Grid:{lon0:-15, lat0:33, lon1:35, lat1:73, dlon:0.25, dlat:0.25}}
            ymlContents['run']['grid'] = '${Grid:{lon0:%.3f,lat0:%.3f,lon1:%.3f,lat1:%.3f,dlon:0.25, dlat:0.25}}' % (Lon0,Lon1, Lat0, Lat1)

        # ObservationsFileLocation
        if (self.iObservationsFileLocation.get()==2):
            ymlContents['observations']['file']['location'] = 'CARBONPORTAL'
        else:
            ymlContents['observations']['file']['location'] = 'LOCAL'
            # TODO: get the file name
            
        # Emissions data (a prioris)
        # Land/Vegetation Net Exchange combo box
        sLandVegModel=self.LandNetExchangeOptionMenu.get()  # 'VPRM', 'LPJ-GUESS'
        ymlContents['emissions']['co2']['categories']['biosphere']=sLandVegModel
        # Fossil emissions combo box
        sFossilEmisDataset = self.FossilEmisOptionMenu.get()  # "EDGARv4_LATEST")
        ymlContents['emissions']['co2']['categories']['fossil']=sFossilEmisDataset
        # Ocean Net Exchange combo box
        sOceanNetExchangeDataset = self.OceanNetExchangeCkbVar.get()  # "mikaloff01"
        ymlContents['emissions']['co2']['categories']['ocean']=sOceanNetExchangeDataset

        # Station Filters if any
        bStationFilterError=False
        stationMinAltCommonSense= -100 #m Dead Sea
        stationMaxAltCommonSense= 9000 #m Himalaya
        inletMinHeightCommonSense = 0    # in meters
        inletMaxHeightCommonSense = 850 # in meters World's heighest buildings
        intAllStations=self.ActivateStationFiltersRbVar.get()
        if(intAllStations==2):
            #if ((ymlContents['observations']['filters']['bStationAltitude'])or():
            if(self.bFilterSamplingHeightCkbVar.get()):
                ymlContents['observations']['filters']['bSamplingHeight']=True
                mnh=int(self.inletMinHghtEntry.get())
                mxh=int(self.inletMaxHghtEntry.get())
                if(inletMinHeightCommonSense > mnh):
                    bStationFilterError=True # ≥≤
                    sErrorMsg+="I can't think of any ICOS station with an inlet height below ground....please fix the minimum inlet height to ≥0m. Thanks.\n"
                if(inletMaxHeightCommonSense+1 < mxh):
                    bStationFilterError=True
                    sErrorMsg+=f"I can't think of any ICOS station with an inlet height higher than {inletMaxHeightCommonSense}m above ground. Please review your entry. Thanks.\n"
                if(mnh > mxh):
                    bStationFilterError=True
                    sErrorMsg+="Error: You have entered a maximum inlet height that is below the lowest inlet height. This can only be attributed to human error.\n"
            else:
                ymlContents['observations']['filters']['bSamplingHeight']=False
            if(ymlContents['observations']['filters']['bSamplingHeight']):
                ymlContents['observations']['filters']['inletMaxHeight']=mxh
                ymlContents['observations']['filters']['inletMinHeight']=mnh
                
            if(self.bFilterStationAltitudeCkbVar.get()):
                ymlContents['observations']['filters']['bStationAltitude']=True
                mnh=int(self.stationMinAltEntry.get())
                mxh=int(self.stationMaxAltEntry.get())
                if( stationMinAltCommonSense > mnh):
                    bStationFilterError=True # ≥≤
                    sErrorMsg+=f"I can't think of any ICOS station located at an altitude below the sea level of the Dead Sea....please fix the minimum station altitude to ≥ {stationMinAltCommonSense}m. Thanks.\n"
                if(stationMaxAltCommonSense+1 < mxh):
                    bStationFilterError=True
                    sErrorMsg+=f"I can't think of any ICOS station located at an altitude higher than {stationMaxAltCommonSense}m above ground. Please review your entry. Thanks.\n"
                if(mnh > mxh):
                    bStationFilterError=True
                    sErrorMsg+="Error: You have entered a maximum station altitude that is below the lowest station altitude. This can only be attributed to human error.\n"
            else:
                ymlContents['observations']['filters']['bStationAltitude']=False
            if(ymlContents['observations']['filters']['bStationAltitude']):
                ymlContents['observations']['filters']['stationMaxAlt']=mxh
                ymlContents['observations']['filters']['stationMinAlt']=mnh
                
                
        if(bStationFilterError):
            bErrors=True
        # Get the adjust Land/Fossil/Ocean checkBoxes and do some sanity checks 
        if((not self.LandVegCkb.get()) and  (not self.FossilCkb.get()) and (not self.OceanCkb.get())):
            bErrors=True
            sErrorMsg+="Error: At least one of Land, Fossil or Ocean needs to be adjustable, preferably Land.\n"
        elif(not self.LandVegCkb.get()):
            bWarnings=True
            sWarningsMsg+="Warning: It is usually a bad idea NOT to adjust the land/vegetation net exchange. Are you sure?!\n"
        if(self.FossilCkb.get()):
            bWarnings=True
            sWarningsMsg+="Warning: It is unusual wanting to adjust the fossil emissions in LUMIA. Are you sure?!\n"
        if(self.OceanCkb.get()):
            bWarnings=True
            sWarningsMsg+="Warning: It is unusual wanting to adjust the ocean net exchange in LUMIA. Are you sure?!\n"
        if(bErrors==False):
            ymlContents['optimize']['emissions']['co2']['biosphere']['adjust'] = self.LandVegCkb.get()
            ymlContents['optimize']['emissions']['co2']['fossil']['adjust'] = self.FossilCkb.get()
            ymlContents['optimize']['emissions']['co2']['ocean']['adjust'] = self.OceanCkb.get()                
            
        # Deal with any errors or warnings
        return(bErrors, sErrorMsg, bWarnings, sWarningsMsg)
        
    def runSysCmd(self, sCmd,  ignoreError=False):
        try:
            os.system(sCmd)
        except:
            if(ignoreError==False):
                sTxt=f"Fatal Error: Failed to execute system command >>{sCmd}<<. Please check your write permissions and possibly disk space etc."
                logger.warning(sTxt)
                # self.CancelAndQuit(sTxt)


class GridCTkCheckBox(ctk.CTkCheckBox):
    def __init__(self, root, myGridID,  *args, **kwargs):
        ctk.CTkCheckBox.__init__(self, root, *args, **kwargs) 
        self.widgetGridID= myGridID

class GridCTkLabel(ctk.CTkLabel):
    def __init__(self, root, myGridID,  *args, **kwargs):
        ctk.CTkLabel.__init__(self, root, *args, **kwargs)
        self.widgetGridID= tk.IntVar(value=myGridID)


class GridCTkOptionMenu(ctk.CTkOptionMenu):
    def __init__(self, root, myGridID, *args, **kwargs):
        ctk.CTkOptionMenu.__init__(self, root, *args, **kwargs)
        self.widgetGridID= myGridID


# LumiaGui Class =============================================================
class RefineObsSelectionGUI(ctk.CTk):
    # =====================================================================
    # The layout of the window is now written
    # in the init function itself
    def __init__(self, sLogCfgPath, ymlContents,ymlFile,  fDiscoveredObservations, widgetsLst, sNow):  # *args, **kwargs):
        # Get the screen resolution to scale the GUI in the smartest way possible...
        nWidgetsPerRow=5
        if os.environ.get('DISPLAY','') == '':
            print('no display found. Using :0.0')
            os.environ.__setitem__('DISPLAY', ':0.0')
        root=ctk.CTk()
        if(os.path.isfile("LumiaGui.stop")):
            sCmd="rm LumiaGui.stop"
            self.runSysCmd(sCmd,  ignoreError=True)
        if(os.path.isfile("LumiaGui.go")):
            sCmd="rm LumiaGui.go"
            self.runSysCmd(sCmd,  ignoreError=True)

        activeTextColor='gray10'
        inactiveTextColor='gray50'
       # re-organise the fDiscoveredObservations dataframe,
        # It is presently sorted by country, station, dataRanking (dClass), productionTime and samplingHeight -- in that order
        #   columnNames=['pid', 'selected','stationID', 'country', 'isICOS','latitude','longitude','altitude','samplingHeight','size', 
        #                 'nRows','dataLevel','obsStart','obsStop','productionTime','accessUrl','fileName','dClass','dataSetLabel'] 
        # 
        # 1) Set the first dataset for each station, highest dClass and heighest samplingHeight to selected and all other ones to not-selected 
        # 2) if there are multiple samplingHeights for otherwise the same characterestics, then stick the lower heights into a list 
        #    of samplingHeight + PID pairs for an otherwise single entry.
        #
        #        #data=[[pid, pidMetadata['specificInfo']['acquisition']['station']['id'], pidMetadata['coverageGeo']['geometry']['coordinates'][0], pidMetadata['coverageGeo']['geometry']['coordinates'][1], pidMetadata['coverageGeo']['geometry']['coordinates'][2], pidMetadata['specificInfo']['acquisition']['samplingHeight'], pidMetadata['size'], pidMetadata['specification']['dataLevel'], pidMetadata['references']['temporalCoverageDisplay'], pidMetadata['specificInfo']['productionInfo']['dateTime'], pidMetadata['accessUrl'], pidMetadata['fileName'], int(0), pidMetadata['specification']['self']['label']]]
        dfAllObs = pd.read_csv (fDiscoveredObservations)
        nRows=int(0)
        bCreateDf=True
        bTrue=True
        isDifferent=True
        #AllObsColumnNames=['pid', 'selected','stationID', 'country', 'isICOS','latitude','longitude','altitude','samplingHeight','size', 
        #            'nRows','dataLevel','obsStart','obsStop','productionTime','accessUrl','fileName','dClass','dataSetLabel'] 
        bUseStationAltitudeFilter=ymlContents['observations']['filters']['bStationAltitude']
        bUseSamplingHeightFilter=ymlContents['observations']['filters']['bSamplingHeight']
        stationMinAlt = ymlContents['observations']['filters']['stationMinAlt']     # in meters amsl
        stationMaxAlt = ymlContents['observations']['filters']['stationMaxAlt']  # in meters amsl
        inletMinHght = ymlContents['observations']['filters']['inletMinHeight']     # in meters amsl
        inletMaxHght = ymlContents['observations']['filters']['inletMaxHeight']  # in meters amsl
        #inletMinHght = 20  # TODO remove this line. for testing only
        newColumnNames=['selected','country', 'stationID', 'altOk', 'altitude', 'HghtOk', 'samplingHeight', 'isICOS', 'latitude', 'longitude', 'dClass', 'dataSetLabel', 'pid', 'includeCountry', 'includeStation']
        for index, row in dfAllObs.iterrows():
            hLst=[row['samplingHeight'] ]
            pidLst=[ row['pid']]
            #print(row['altitude'])
            bStationAltOk = (((row['altitude'] >= stationMinAlt) &
                                (row['altitude'] <= stationMaxAlt) ) | (bUseStationAltitudeFilter==False)) 
            bSamplHghtOk = (((row['samplingHeight'] >= inletMinHght) &
                                (row['samplingHeight'] <= inletMaxHght) ) | (bUseSamplingHeightFilter==False))
            newRow=[bTrue,row['country'], row['stationID'], bStationAltOk, row['altitude'],  
                            bSamplHghtOk, hLst, row['isICOS'], row['latitude'], row['longitude'], row['dClass'], row['dataSetLabel'],  pidLst, True,  True]
            
            if(bCreateDf):
                newDf=pd.DataFrame(data=[newRow], columns=newColumnNames)     
                bCreateDf=False
                nRows+=1
            else:
                #data=[pid, pidMetadata['specificInfo']['acquisition']['station']['id'], pidMetadata['coverageGeo']['geometry']['coordinates'][0], pidMetadata['coverageGeo']['geometry']['coordinates'][1], pidMetadata['coverageGeo']['geometry']['coordinates'][2], pidMetadata['specificInfo']['acquisition']['samplingHeight'], pidMetadata['size'], pidMetadata['specification']['dataLevel'], pidMetadata['references']['temporalCoverageDisplay'], pidMetadata['specificInfo']['productionInfo']['dateTime'], pidMetadata['accessUrl'], pidMetadata['fileName'], int(0), pidMetadata['specification']['self']['label']]
                isDifferent = ((row['stationID'] not in newDf['stationID'][nRows-1]) |
                                        (int(row['dClass']) != int(newDf['dClass'][nRows-1]) )
                )
                if(isDifferent):  # keep the new row as an entry that differs from the previous row in more than samplingHeight
                    newDf.loc[nRows] = newRow
                    nRows+=1
                else:
                    #newDf['samplingHeight'][nRows-1]+=[row['samplingHeight'] ]  # ! works on a copy of the column, not the original object
                    # Don't do as in the previous line. See here: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
                    newDf.at[(nRows-1) ,  ('samplingHeight')]+=[row['samplingHeight'] ]
                    #newDf['pid'][nRows-1]+=[ row['pid']]
                    newDf.at[(nRows-1) ,  ('pid')]+=[row['pid'] ]
                    #TODO: dClass
                
        if(not isDifferent):
            newDf.drop(newDf.tail(1).index,inplace=True) # drop the last row 
        newDf.to_csv('newDfObs.csv', mode='w', sep=',')  
        nObs=len(newDf)
        filtered = ((newDf['selected'] == True))
        #dfq= newDf[filtered]
        #nSelected=len(dfq)
        logger.info(f"There are {nObs} valid data sets in the selected geographical region ingoring multiple sampling heights.")
        #logger.info(f"Thereof {nSelected} are presently selected.")
        isDifferent=True
        nRows=0
        isSameStation = False
        for index, row in newDf.iterrows():
            if((row['altOk']==False) or (row['HghtOk']==False)):
                newDf.at[(nRows) ,  ('selected')] = False
            if(nRows>0):
                isSameStation = ((row['stationID'] in newDf['stationID'][nRows-1]))
            if(isSameStation):
                newDf.at[(nRows) ,  ('selected')] = False
            nRows+=1
        newDf.to_csv('selectedObs.csv', mode='w', sep=',')  
            

        screenWidth = root.winfo_screenwidth()
        screenHeight = root.winfo_screenheight()
        if((screenWidth/screenHeight) > (1920/1080.0)):  # multiple screens?
            screenWidth=int(screenHeight*(1920/1080.0))
        maxW = int(0.92*screenWidth)
        maxH = int(0.92*screenHeight)
        nCols=12 # sum of labels and entry fields per row
        nRows=32 #5+len(newDf) # number of rows in the GUI - not so important - window is scrollable
        xPadding=int(0.008*maxW)
        wSpacer=int(2*0.008*maxW)
        yPadding=int(0.008*maxH)
        vSpacer=int(2*0.008*maxH)
        myFontFamily="Georgia"
        sLongestTxt="Latitude NN :"
        (fsTINY,  fsSMALL,  fsNORMAL,  fsLARGE,  fsHUGE,  fsGIGANTIC,  bWeMustStackColumns)= \
            calculateEstheticFontSizes(myFontFamily,  maxW,  maxH, sLongestTxt, nCols, nRows, xPad=xPadding, 
                                                        yPad=yPadding, maxFontSize=20,  bWeCanStackColumns=False)
        hDeadSpace=wSpacer+(nCols*xPadding*2)+wSpacer
        vDeadSpace=2*yPadding 
        colWidth=int((maxW - hDeadSpace)/(nCols*1.0))
        rowHeight=int((maxH - vDeadSpace)/(nRows*1.0))
        # Dimensions of the window
        appWidth, appHeight = maxW, maxH
        
        # Sets the title of the window to "LumiaGui"
        root.title("LUMIA: Refine the selection of observations to be used")  
        # Sets the dimensions of the window to 800x900
        root.geometry(f"{appWidth}x{appHeight}")   
        root.grid_rowconfigure(0, weight=1)
        root.columnconfigure(0, weight=1)
        
        # Now we venture to make the root scrollable....
        #main_frame = tk.Frame(root)
        rootFrame = tk.Frame(root, bg="cadet blue")
        rootFrame.grid(sticky='news')
        excludedCountriesList = []
        excludedStationsList = []
        try:
            excludedCountriesList = ymlContents['observations']['filters']['CountriesExcluded']
        except:
            pass
        try:
            excludedStationsList = ymlContents['observations']['filters']['StationsExcluded']
        except:
            pass
            
        
        # Done this step - the GUI canvas with scrollbars has been created

        #def updateRowOfWidgets(gridList, rowindex, row):
        #    ''' updates the states of all the widgets belonging to one observational data set corresponding to a single line on the GUI '''
        #    print("..to be implemented ...")
        #    return
             
        def applyRules():
            bICOSonly=ymlContents['observations']['filters']['ICOSonly']
            bUseStationAltitudeFilter=ymlContents['observations']['filters']['bStationAltitude']
            bUseSamplingHeightFilter=ymlContents['observations']['filters']['bSamplingHeight']
            stationMinAlt = ymlContents['observations']['filters']['stationMinAlt']     # in meters amsl
            stationMaxAlt = ymlContents['observations']['filters']['stationMaxAlt']  # in meters amsl
            inletMinHght = ymlContents['observations']['filters']['inletMinHeight']     # in meters amsl
            inletMaxHght = ymlContents['observations']['filters']['inletMaxHeight']  # in meters amsl
            for ri, row in newDf.iterrows():
                #if ((row['stationID'] in 'ZSF') or (row['stationID'] in 'JAR') or (row['stationID'] in 'DEC')):
                    #id=row['stationID']
                    #print(f'stationID={id},  newDf-rowidx={ri}')
                row['altOk'] = (((float(row['altitude']) >= stationMinAlt) &
                                    (float(row['altitude']) <= stationMaxAlt) ) | (bUseStationAltitudeFilter==False)) 
                if(isinstance(row['samplingHeight'], list)):
                    sH=float(row['samplingHeight'][0])
                else:
                    sH=float(row['samplingHeight'])
                row['HghtOk'] = (((sH >= inletMinHght) &
                                                (sH <= inletMaxHght) ) | (bUseSamplingHeightFilter==False))
                bIcosOk=((bICOSonly==False)or(row['isICOS']==True))
                bSel=False
                countryInactive=row['country'] in excludedCountriesList
                stationInactive=row['stationID'] in excludedStationsList
                # The row['includeCountry'] flag tells us whether we draw it as we draw country names only once for all its data sets
                if((row['includeCountry']) and (row['includeStation']) and 
                    (not countryInactive) and (not stationInactive) and 
                    (row['altOk']) and (row['HghtOk']) and 
                    (int(row['dClass'])==4) and (bIcosOk)):
                    # if station and country are selected only then may we toggle the 'selected' entry to True
                    bSel=True
                bS=row['selected']
                row.iloc[0]=bSel  # row['selected'] is the same as row.iloc[0]
                newDf.at[(ri) ,  ('selected')] = bSel
                newDf.at[(ri) ,  ('altOk')] = row['altOk']
                newDf.at[(ri) ,  ('HghtOk')] = row['HghtOk']
                if((bSel != bS) and (int(row['dClass'])==4)): 
                    self.updateRowOfWidgets(widgetsLst, ri, row, nWidgetsPerRow, activeTextColor, inactiveTextColor)

        def stationAltitudeFilterAction():
            stationMinAltCommonSense= -100 #m Dead Sea
            stationMaxAltCommonSense= 9000 #m Himalaya
            bStationFilterActive=self.FilterStationAltitudesCkb.get() 
            sErrorMsg=""
            bStationFilterError=False        
            if(bStationFilterActive):
                ymlContents['observations']['filters']['bStationAltitude']=True
                mnh=int(self.stationMinAltEntry.get())
                mxh=int(self.stationMaxAltEntry.get())
                if( stationMinAltCommonSense > mnh):
                    bStationFilterError=True # ≥≤
                    sErrorMsg+=f"I can't think of any ICOS station located at an altitude below the sea level of the Dead Sea....please fix the minimum station altitude to ≥ {stationMinAltCommonSense}m. Thanks.\n"
                if(stationMaxAltCommonSense+1 < mxh):
                    bStationFilterError=True
                    sErrorMsg+=f"I can't think of any ICOS station located at an altitude higher than {stationMaxAltCommonSense}m above ground. Please review your entry. Thanks.\n"
                if(mnh > mxh):
                    bStationFilterError=True
                    sErrorMsg+="Error: You have entered a maximum station altitude that is below the lowest station altitude. This can only be attributed to human error.\n"
            else:
                ymlContents['observations']['filters']['bStationAltitude']=False
            if(bStationFilterError):
                self.FilterStationAltitudesCkb.set(False)
                ymlContents['observations']['filters']['bStationAltitude']=False
            elif(ymlContents['observations']['filters']['bStationAltitude']):
                ymlContents['observations']['filters']['stationMaxAlt']=mxh
                ymlContents['observations']['filters']['stationMinAlt']=mnh
            applyRules()                       
            
            
        def stationSamplingHghtAction():
            inletMinHeightCommonSense = 0    # in meters
            inletMaxHeightCommonSense = 850 # in meters World's heighest buildings
            sErrorMsg=""
            bStationFilterError=False
            bSamplingHghtFilterActive=self.FilterSamplingHghtCkb.get()
            if(bSamplingHghtFilterActive):
                ymlContents['observations']['filters']['bSamplingHeight']=True
                mnh=int(self.inletMinHghtEntry.get())
                mxh=int(self.inletMaxHghtEntry.get())
                if(inletMinHeightCommonSense > mnh):
                    bStationFilterError=True # ≥≤
                    sErrorMsg+="I can't think of any ICOS station with an inlet height below ground....please fix the minimum inlet height to ≥0m. Thanks.\n"
                if(inletMaxHeightCommonSense+1 < mxh):
                    bStationFilterError=True
                    sErrorMsg+=f"I can't think of any ICOS station with an inlet height higher than {inletMaxHeightCommonSense}m above ground. Please review your entry. Thanks.\n"
                if(mnh > mxh):
                    bStationFilterError=True
                    sErrorMsg+="Error: You have entered a maximum inlet height that is below the lowest inlet height. This can only be attributed to human error.\n"
            else:
                ymlContents['observations']['filters']['bSamplingHeight']=False
            if(bStationFilterError):
                self.FilterSamplingHghtCkb.set(False)
                ymlContents['observations']['filters']['bSamplingHeight']=False
            elif(ymlContents['observations']['filters']['bSamplingHeight']):
                ymlContents['observations']['filters']['inletMaxHeight']=mxh
                ymlContents['observations']['filters']['inletMinHeight']=mnh
            applyRules()

        def isICOSrbAction():
            isICOSrbValue=self.isICOSplusRadioButton.cget("variable")
            if(isICOSrbValue==2):
                ymlContents['observations']['filters']['ICOSonly']=True
            else:
                ymlContents['observations']['filters']['ICOSonly']=False
            applyRules()                       
    
    
        # Row 0:  Title Label
        # ################################################################

        self.LatitudesLabel = ctk.CTkLabel(rootFrame,
                                text="LUMIA  --  Refine the selection of observations to be used",  font=("Arial MT Extra Bold",  fsGIGANTIC))
        self.LatitudesLabel.grid(row=0, column=0,
                            columnspan=8,padx=xPadding, pady=yPadding,
                            sticky="nw")

        # Row 1-4:  Header part with pre-selctions
        # ################################################################
        
        # Col0
        # ObservationsFileLocation
        self.iObservationsFileLocation= tk.IntVar(value=1)
        #iObservationsFileLocation.set(1) # Read observations from local file
        if ('CARBONPORTAL' in ymlContents['observations']['file']['location']):
            self.iObservationsFileLocation.set(2)
        self.RankingLabel = ctk.CTkLabel(rootFrame,
                                   text="Obsdata Ranking", font=("Georgia",  fsNORMAL))
        self.RankingLabel.grid(row=1, column=0,
                                  columnspan=1, padx=xPadding, pady=yPadding,
                                  sticky="nw")

        # Ranking for Observation Files
        rankingList=ymlContents['observations']['file']['ranking']
        self.ObsFileRankingTbxVar = tk.StringVar(value="ObsPack")
        self.ObsFileRankingBox = ctk.CTkTextbox(rootFrame,
                                         width=colWidth,
                                         height=int(2.4*rowHeight+vSpacer))
        self.ObsFileRankingBox.grid(row=2, column=0,
                             columnspan=1, rowspan=2, padx=xPadding,
                             pady=yPadding, sticky="nsew")
        txt=""
        rank=4
        for sEntry in  rankingList:
            txt+=str(rank)+': '+sEntry+'\n'
            rank=rank-1
        self.ObsFileRankingBox.insert('0.0', txt)  # insert at line 0 character 0

        # Col2
        #  ##################################################
        self.ObsLv1CkbVar = tk.BooleanVar(value=True)
        self.ObsLv1Ckb = ctk.CTkCheckBox(rootFrame,
                            text="Level1", font=("Georgia",  fsNORMAL),
                            variable=self.ObsLv1CkbVar,
                             onvalue=True, offvalue=False)                             
        self.ObsLv1Ckb.grid(row=1, column=1,
                          padx=xPadding, pady=yPadding,
                          sticky="nw")

        self.ObsNRTCkbVar = tk.BooleanVar(value=True)
        self.ObsNRTCkb = ctk.CTkCheckBox(rootFrame,
                            text="NRT", font=("Georgia",  fsNORMAL),
                            variable=self.ObsNRTCkbVar,
                             onvalue=True, offvalue=False)                             
        self.ObsNRTCkb.grid(row=2, column=1,
                          padx=xPadding, pady=yPadding,
                          sticky="nw")

        self.ObsOtherCkbVar = tk.BooleanVar(value=True)
        self.ObsOtherCkb = ctk.CTkCheckBox(rootFrame,
                            text="Other", font=("Georgia",  fsNORMAL),
                            variable=self.ObsOtherCkbVar,
                             onvalue=True, offvalue=False)                             
        self.ObsOtherCkb.grid(row=3, column=1,
                          padx=xPadding, pady=yPadding,
                          sticky="nw")
        
        
        # Col 3    Filtering of station altitudes
        #  ##################################################

        self.FilterStationAltitudesCkbVar = tk.BooleanVar(value=ymlContents['observations']['filters']['bStationAltitude'])
        self.FilterStationAltitudesCkb = ctk.CTkCheckBox(rootFrame,
                            text="Filter station altitudes", font=("Georgia",  fsNORMAL),
                            variable=self.FilterStationAltitudesCkbVar,
                            onvalue=True, offvalue=False, command=stationAltitudeFilterAction)
        self.FilterStationAltitudesCkb.grid(row=1, column=3,columnspan=2, 
                          padx=xPadding, pady=yPadding,
                          sticky="nw")

        self.minAltLabel = ctk.CTkLabel(rootFrame,
                                   text="min alt:", font=("Georgia",  fsNORMAL))
        self.minAltLabel.grid(row=2, column=3,
                                  columnspan=1, padx=xPadding, pady=yPadding,
                                  sticky="nw")
        self.maxAltLabel = ctk.CTkLabel(rootFrame,
                                   text="max alt:", font=("Georgia",  fsNORMAL))
        self.maxAltLabel.grid(row=3, column=3,
                                  columnspan=1, padx=xPadding, pady=yPadding,
                                  sticky="nw")

        stationMinAlt = ymlContents['observations']['filters']['stationMinAlt']     # in meters amsl
        stationMaxAlt = ymlContents['observations']['filters']['stationMaxAlt']  # in meters amsl
        self.stationMinAlt=tk.StringVar(value=f'{stationMinAlt}')
        self.stationMaxAlt=tk.StringVar(value=f'{stationMaxAlt}')
        # min Altitude Entry
        self.stationMinAltEntry = ctk.CTkEntry(rootFrame,textvariable=self.stationMinAlt,
                          placeholder_text=self.stationMinAlt, width=colWidth)
        self.stationMinAltEntry.grid(row=2, column=4,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="nw")
        # max Altitude Entry
        self.stationMaxAltEntry = ctk.CTkEntry(rootFrame,textvariable=self.stationMaxAlt,
                          placeholder_text=self.stationMaxAlt, width=colWidth)
        self.stationMaxAltEntry.grid(row=3, column=4,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="nw")
                             

        # Col 5    -  sampling height filter
        # ################################################################
        # 

        self.FilterSamplingHghtCkbVar = tk.BooleanVar(value=ymlContents['observations']['filters']['bSamplingHeight'])
        self.FilterSamplingHghtCkb = ctk.CTkCheckBox(rootFrame,
                            text="Filter sampling heights", font=("Georgia",  fsNORMAL),
                            variable=self.FilterSamplingHghtCkbVar,
                             onvalue=True, offvalue=False, command=stationSamplingHghtAction)                             
        self.FilterSamplingHghtCkb.grid(row=1, column=5,columnspan=2, 
                          padx=xPadding, pady=yPadding,
                          sticky="nw")

        self.minHghtLabel = ctk.CTkLabel(rootFrame,
                                   text="min alt:", font=("Georgia",  fsNORMAL))
        self.minHghtLabel.grid(row=2, column=5,
                                  columnspan=1, padx=xPadding, pady=yPadding,
                                  sticky="nw")
        self.maxHghtLabel = ctk.CTkLabel(rootFrame,
                                   text="max alt:", font=("Georgia",  fsNORMAL))
        self.maxHghtLabel.grid(row=3, column=5,
                                  columnspan=1, padx=xPadding, pady=yPadding,
                                  sticky="nw")

        inletMinHght = ymlContents['observations']['filters']['inletMinHeight']     # in meters amsl
        inletMaxHght = ymlContents['observations']['filters']['inletMaxHeight']  # in meters amsl
        self.inletMinHght=tk.StringVar(value=f'{inletMinHght}')
        self.inletMaxHght=tk.StringVar(value=f'{inletMaxHght}')
        # min inlet height
        self.inletMinHghtEntry = ctk.CTkEntry(rootFrame,textvariable=self.inletMinHght,
                          placeholder_text=self.inletMinHght, width=colWidth)
        self.inletMinHghtEntry.grid(row=2, column=6,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="nw")
        # max inlet height
        self.inletMaxHghtEntry = ctk.CTkEntry(rootFrame,textvariable=self.inletMaxHght,
                          placeholder_text=self.inletMaxHght, width=colWidth)
        self.inletMaxHghtEntry.grid(row=3, column=6,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="nw")

        # Col7
        # ################################################################
        # 
        self.ICOSstationsLabel = ctk.CTkLabel(rootFrame,
                                   text="ICOS stations", font=("Georgia",  fsNORMAL))
        self.ICOSstationsLabel.grid(row=1, column=7,
                                  columnspan=1, padx=xPadding, pady=yPadding,
                                  sticky="nw")
        #
        self.isICOSRadioButtonVar = tk.IntVar(value=1)
        if (ymlContents['observations']['filters']['ICOSonly']==True):
            self.isICOSRadioButtonVar.set(2)
        self.isICOSplusRadioButton = ctk.CTkRadioButton(rootFrame,
                                   text="Any station", font=("Georgia",  fsNORMAL),
                                   variable=self.isICOSRadioButtonVar,  value=1,  command=isICOSrbAction)
        self.isICOSplusRadioButton.grid(row=2, column=7,
                            padx=xPadding, pady=yPadding,
                            sticky="nw")
         
        self.ICOSradioButton = ctk.CTkRadioButton(rootFrame,
                                   text="ICOS only", font=("Georgia",  fsNORMAL), 
                                   variable=self.isICOSRadioButtonVar,  value=2,  command=isICOSrbAction)
        self.ICOSradioButton.grid(row=3, column=7,
                            padx=xPadding, pady=yPadding,
                            sticky="nw")


        # Col_11
        # ################################################################
        # 

        def GuiClosed():
            if tk.messagebox.askokcancel("Quit", "Is it OK to abort your Lumia run?"):
                logger.info("LumiaGUI was canceled.")
                sCmd="touch LumiaGui.stop"
                self.runSysCmd(sCmd)
                #root.destroy
                global LOOP_ACTIVE
                LOOP_ACTIVE = False
        root.protocol("WM_DELETE_WINDOW", GuiClosed)
        def CancelAndQuit(): 
            logger.info("LumiaGUI was canceled.")
            sCmd="touch LumiaGui.stop"
            self.runSysCmd(sCmd)
            #self.bPleaseCloseTheGui.set(True)
            global LOOP_ACTIVE
            LOOP_ACTIVE = False

        # Cancel Button
        self.CancelButton = ctk.CTkButton(master=rootFrame, font=("Georgia", fsNORMAL), text="Cancel",
            fg_color='orange red', command=CancelAndQuit)
        self.CancelButton.grid(row=2, column=11,
                                        columnspan=1, padx=xPadding,
                                        pady=yPadding, sticky="nw")

        # Check choices
        # self.CheckButton = ctk.CTkButton(master=rootFrame, font=("Georgia", fsNORMAL), 
        #                                 text="Check choices", text_color='gray5',  text_color_disabled='gray70', 
        #                                 fg_color='OliveDrab1', command=CancelAndQuit)
        # self.CheckButton.grid(row=2, column=11,
        #                   padx=xPadding, pady=yPadding,
        #                   sticky="nw")



        #Col 10:  RUN Button
        def GoBtnHit():
            # get current time
            #current_date = datetime.now()
            #sNow=current_date.isoformat("T","minutes")
            # Write the list of observational files
            try:
                nObs=len(newDf)
                filtered = ((newDf['selected'] == True))
                dfq= newDf[filtered]
                nSelected=len(dfq)
                logger.info(f"There are {nObs} valid data sets in the selected geographical region ingoring multiple sampling heights.")
                logger.info(f"Thereof {nSelected} are presently selected.")
            except:
                pass
            try:
                newDf.to_csv('allObsInTimeSpaceSlab.csv', mode='w', sep=',')  
                dfq['pid2'] = dfq['pid'].apply(grabFirstEntryFromList)
                dfq['samplingHeight2'] = dfq['samplingHeight'].apply(grabFirstEntryFromList)
                dfq.drop(columns='pid',inplace=True) # drop columns with lists. These are replaced with single values from the first list entry
                dfq.drop(columns='samplingHeight',inplace=True) # drop columns with lists. These are replaced with single values from the first list entry
                dfq.rename(columns={'pid2': 'pid', 'samplingHeight2': 'samplingHeight'},inplace=True)
                dfq.to_csv("Lumia-Refined-ObsData-"+sNow+".csv", mode='w', sep=',')
            except:
                sTxt=f"Fatal Error: Failed to write to text the file allObsInTimeSpaceSlab.csv or Lumia-ObsData-{sNow}.csv in the local run directory. Please check your write permissions and possibly disk space etc."
                CancelAndQuit(sTxt)
            try:
                nC=len(excludedCountriesList)
                nS=len(excludedStationsList)
                if(nS==0):
                    logger.info("No observation stations were rejected")
                else:
                    s=""
                    for element in excludedStationsList:
                        s=s+element+', '
                    logger.info(f"{nS} observation stations ({s[:-2]}) were rejected")
                setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'observations',  'filters',  'StationsExcluded'],   
                                                                            value=excludedStationsList, bNewValue=True)
                if(nC==0):
                    logger.info("No countries were rejected")
                else:
                    s=""
                    for element in excludedCountriesList:
                        s=s+element+', '
                    logger.info(f"{nC} countries ({s[:-2]}) were rejected")
                setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'observations',  'filters',  'CountriesExcluded'],   
                                                                            value=excludedCountriesList, bNewValue=True)
            except:
                pass
            # Save  all details of the configuration and the version of the software used:
            setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'observations',  'filters',  'ICOSonly'],   
                                                                        value=ymlContents['observations']['filters']['ICOSonly'], bNewValue=True)
            sLogCfgFile=sLogCfgPath+"Lumia-runlog-"+sNow+"-config.yml"    
            try:
                with open(ymlFile, 'w') as outFile:
                    yaml.dump(ymlContents, outFile)
            except:
                sTxt=f"Fatal Error: Failed to write to text file {ymlFile} in local run directory. Please check your write permissions and possibly disk space etc."
                CancelAndQuit(sTxt)
            
            sCmd="cp "+ymlFile+" "+sLogCfgFile
            self.runSysCmd(sCmd)
            sCmd="touch LumiaGui.go"
            self.runSysCmd(sCmd)
            logger.info("Done. LumiaGui completed successfully. Config and Log file written.")
            # self.bPleaseCloseTheGui.set(True)
            global LOOP_ACTIVE
            LOOP_ACTIVE = False

        # ######################################################            
        self.RunButton = ctk.CTkButton(rootFrame, font=("Georgia", fsNORMAL), 
                                         text_color='gray5',  text_color_disabled='gray70',  text="RUN", 
                                         fg_color='green2', command=GoBtnHit) 
                                         #fg_color='green2', command=lambda widgetID=self.RunButton : GoButtonHit(widgetID, newDf)) 
                                         #         myWidgetSelect.configure(command=lambda widgetID=myWidgetSelect.widgetGridID : self.handleMyCheckboxEvent(myWidgetSelect.widgetGridID, widgetsLst, obsDf, row, nWidgetsPerRow, activeTextColor, inactiveTextColor)) 

        self.RunButton.grid(row=3, column=11,
                                        columnspan=1, padx=xPadding,
                                        pady=yPadding, sticky="nw")
                                        

        # Row 4 title for individual entries
        # ################################################################
        # newColumnNames=['selected','country', 'stationID', 'altOk', 'altitude', 'HghtOk', 'samplingHeight', 'isICOS', 'latitude', 'longitude', 'dClass', 'dataSetLabel', 'pid', 'includeCountry', 'includeStation']
        myLabels=". Selected        Country         StationID       SamplingHeight    Stat.altitude    Network   Latitude Longitude  DataRanking DataDescription"
        self.ColLabels = ctk.CTkLabel(rootFrame, justify="left", anchor="w",
                                   text=myLabels, font=("Georgia",  fsNORMAL))
        self.ColLabels.grid(row=4, column=0, columnspan=10, padx=2, pady=yPadding, sticky="nw")

        # Create a scrollable frame onto which to place the many widgets that represent all valid observations found
        #  ##################################################################
        # Create a frame for the canvas with non-zero row&column weights
        
        rootFrame.update() # above widgets are drawn and we can get column width measures
        x, y, width, height = rootFrame.grid_bbox()

        rootFrameCanvas = tk.Frame(rootFrame)
        rootFrameCanvas.grid(row=5, column=0,  columnspan=12,  rowspan=20, pady=(5, 0), sticky='nw') #, columnspan=11,  rowspan=10
        rootFrameCanvas.grid_rowconfigure(0, weight=8)
        rootFrameCanvas.grid_columnconfigure(0, weight=8)
        # Set grid_propagate to False to allow 5-by-5 buttons resizing later
        #rootFrameCanvas.grid_propagate(False)
        cWidth = appWidth - xPadding
        cHeight = appHeight - (7*rowHeight) - (3*yPadding)
        
        # Add a scrollableCanvas in that frame
        scrollableCanvas = tk.Canvas(rootFrameCanvas, bg="cadet blue", width=cWidth, height=cHeight, borderwidth=0, highlightthickness=0)
        scrollableCanvas.grid(row=0, column=0,  columnspan=12,  rowspan=10, sticky="news")
        
        # Link a scrollbar to the scrollableCanvas
        vsb = tk.Scrollbar(rootFrameCanvas, orient="vertical", command=scrollableCanvas.yview)
        vsb.grid(row=0, column=1, sticky='ns')
        scrollableCanvas.configure(yscrollcommand=vsb.set)
        
        # Create a frame to contain the widgets for all obs data sets found following initial user criteria
        scrollableFrame4Widgets = tk.Frame(scrollableCanvas, bg="#82d0d2") #  slightly lighter than "cadet blue"
        scrollableCanvas.create_window((0, 0), window=scrollableFrame4Widgets, anchor='nw')
        # scrollableFrame4Widgets.grid_rowconfigure(0, weight=1,uniform = 999)
        
        # 
        startRow=5
        yPadding=int(0.5*yPadding)
        # newColumnNames=['selected','country', 'stationID', 'altOk', 'altitude', 'HghtOk', 'samplingHeight', 'isICOS', 'latitude', 'longitude', 'dClass', 'dataSetLabel', 'pid', 'includeCountry', 'includeStation']
        sLastCountry=''
        sLastStation=''
        
        #def createRowOfWidgets(gridList, rowindex, row):
            
        num = 0  # index for 
        for rowidx, row in newDf.iterrows(): 
            guiRow=rowidx # startRow+rowidx
            
            if((rowidx==0) or (row['country'] not in sLastCountry)):
                # when the widgets are created,  row['includeCountry'] is used to indicate whether a CheckBox
                # is painted for that row - only the first row of each country will have this
                # Once the CheckBox was created, row['includeCountry'] is used to track wether the Country has been
                # selected or deselected by the user. At start (in this subroutine) all Countries are selected.
                row['includeCountry'] = True
                sLastCountry=row['country'] 
            else:
                #newDf.at[(rowidx) ,  ('includeCountry')] = False
                row['includeCountry']=False

            if((rowidx==0) or (row['stationID'] not in sLastStation)):
                row['includeStation']=True
                sLastStation=row['stationID']
            else:
                #newDf.at[(rowidx) ,  ('includeStation')] = False
                row['includeStation']=False

            sSamplingHeights=[str(row['samplingHeight'][0])] 
            #if(rowidx==0):
            #    print(row)
            for element in row['samplingHeight']:
                sElement=str(element)
                if(not sElement in sSamplingHeights):
                    sSamplingHeights.append(sElement)
                    
            #createRowOfWidgets(gridList, rowidx, row)                
            self.createRowOfWidgets(scrollableFrame4Widgets, widgetsLst, num,  rowidx, row,guiRow, 
                                                    sSamplingHeights, excludedCountriesList, excludedStationsList,  activeTextColor, 
                                                    inactiveTextColor, fsNORMAL, xPadding, yPadding, fsSMALL, obsDf=newDf)
            # After drawing the initial widgets, all entries of station and country are set to true unless they are on an exclusion list
            countryInactive=row['country'] in excludedCountriesList
            stationInactive=row['stationID'] in excludedStationsList
            newDf.at[(rowidx) ,  ('includeCountry')] = (not countryInactive)
            newDf.at[(rowidx) ,  ('includeStation')] =(not stationInactive)

        # Update buttons frames idle tasks to let tkinter calculate buttons sizes
        scrollableFrame4Widgets.update_idletasks()
              
        # Set the scrollableCanvas scrolling region
        scrollableCanvas.config(scrollregion=scrollableCanvas.bbox("all"))

            
        def loop_function():
            global LOOP_ACTIVE
            LOOP_ACTIVE = True
            while LOOP_ACTIVE:
                #print(chk.state())
                time.sleep(3)
            logger.info("Closing the GUI...")
            #root.destroy()
            root.event_generate("<Destroy>")
        _thread.start_new_thread(loop_function, ())
        root.mainloop()
        try:        
            root.destroy() # just incase this hasn't already been done
        except:
            pass
        return
        

    def createRowOfWidgets(self, scrollableFrame4Widgets, widgetsLst, num, rowidx, row, guiRow, 
                                                sSamplingHeights, excludedCountriesList, excludedStationsList, activeTextColor, 
                                                inactiveTextColor, fsNORMAL, xPadding, yPadding, fsSMALL, obsDf):
        ''' draw all the widgets belonging to one observational data set corresponding to a single line on the GUI '''
        nWidgetsPerRow=5
        gridRow=[]
        
        bSelected=row['selected']
        if(bSelected):
            sTextColor=activeTextColor
        else:
            sTextColor=inactiveTextColor
        countryInactive=row['country'] in excludedCountriesList
        stationInactive=row['stationID'] in excludedStationsList
        
        colidx=int(0)  # row['selected']
        # ###################################################
        gridID=int((100*rowidx)+colidx)  # encode row and column in the button's variable
        myWidgetVar= tk.BooleanVar(value=row['selected'])
        myWidgetSelect  = GridCTkCheckBox(scrollableFrame4Widgets, gridID,  text="",font=("Georgia", fsNORMAL),
                                                            text_color=sTextColor, text_color_disabled=sTextColor, 
                                                            variable=myWidgetVar, onvalue=True, offvalue=False) 
        myWidgetSelect.configure(command=lambda widgetID=myWidgetSelect.widgetGridID : self.handleMyCheckboxEvent(myWidgetSelect.widgetGridID, 
                                                    widgetsLst, obsDf, nWidgetsPerRow, excludedCountriesList, excludedStationsList, activeTextColor, inactiveTextColor)) 
        if(bSelected):
            myWidgetSelect.select()
        else:
            myWidgetSelect.deselect()
        if((countryInactive) or (stationInactive)):
            myWidgetSelect.deselect()
        myWidgetSelect.grid(row=guiRow, column=colidx,
                          columnspan=1, padx=xPadding, pady=yPadding, sticky='news')
        widgetsLst.append(myWidgetSelect) # colidx=1

        gridRow.append(row['selected'])   
        
        colidx+=1 # =1  row['includeCountry']
        # ###################################################
        num+=1
        if((rowidx==0) or (row['includeCountry'] == True)):
            gridID=int((100*rowidx)+colidx)
            myWidgetVar= tk.BooleanVar(value=row['includeCountry'])
            myWidgetCountry  = GridCTkCheckBox(scrollableFrame4Widgets, gridID, text=row['country'],text_color=sTextColor, text_color_disabled=sTextColor, 
                                                                font=("Georgia", fsNORMAL), variable=myWidgetVar, onvalue=True, offvalue=False)  
            myWidgetCountry.configure(command=lambda widgetID=myWidgetCountry.widgetGridID : self.handleMyCheckboxEvent(myWidgetCountry.widgetGridID, 
                                                        widgetsLst, obsDf, nWidgetsPerRow, excludedCountriesList, excludedStationsList, activeTextColor, inactiveTextColor)) 
            if(countryInactive):
                myWidgetCountry.deselect()
            else:
                myWidgetCountry.select()
            myWidgetCountry.grid(row=guiRow, column=colidx, columnspan=1, padx=xPadding, pady=yPadding,sticky='news')
            widgetsLst.append(myWidgetCountry)
        else:
            widgetsLst.append(None)
        gridRow.append(row['country'])
        
        colidx+=1 # =2   row['includeStation']
        # ###################################################
        num+=1
        gridID=int((100*rowidx)+colidx)
        myWidgetVar= tk.BooleanVar(value=row['includeStation'])
        myWidgetStationid  = GridCTkCheckBox(scrollableFrame4Widgets, gridID, text=row['stationID'],text_color=sTextColor, text_color_disabled=sTextColor, 
                                                            font=("Georgia", fsNORMAL), variable=myWidgetVar, onvalue=True, offvalue=False) 
        myWidgetStationid.configure(command=lambda widgetID=myWidgetStationid.widgetGridID : self.handleMyCheckboxEvent(myWidgetStationid.widgetGridID, 
                                                        widgetsLst, obsDf, nWidgetsPerRow, excludedCountriesList, excludedStationsList, activeTextColor, inactiveTextColor)) 
        if(stationInactive):
            myWidgetStationid.deselect()
        else:
            myWidgetStationid.select()
        myWidgetStationid.grid(row=guiRow, column=colidx, columnspan=1, padx=xPadding, pady=yPadding, sticky='news')
        widgetsLst.append(myWidgetStationid)
        gridRow.append(row['stationID'])

        colidx+=1 # =3  row['samplingHeight']
        # ###################################################
        gridID=int((100*rowidx)+colidx)
        myWidgetVar= tk.StringVar(value=str(row['samplingHeight'][0])) 
        myWidgetSamplingHeight  = GridCTkOptionMenu(scrollableFrame4Widgets, gridID, values=sSamplingHeights,
                                                            variable=myWidgetVar, text_color=sTextColor, text_color_disabled=sTextColor,
                                                            font=("Georgia", fsNORMAL), dropdown_font=("Georgia",  fsSMALL)) 
        myWidgetSamplingHeight.configure(command=lambda widget=myWidgetSamplingHeight.widgetGridID : self.handleMyOptionMenuEvent(myWidgetSamplingHeight.widgetGridID, widgetsLst, obsDf, sSamplingHeights, nWidgetsPerRow, activeTextColor, inactiveTextColor))  
        myWidgetSamplingHeight.grid(row=guiRow, column=colidx, columnspan=1, padx=xPadding, pady=yPadding, sticky='news')
        widgetsLst.append(myWidgetSamplingHeight)
        gridRow.append(row['samplingHeight'][0])

        colidx+=1 # =4  Remaining Labels in one string
        # ###################################################
        gridID=int((100*rowidx)+colidx)
        affiliationICOS="ICOS"
        if(not row['isICOS']):
            affiliationICOS="non-ICOS"
        sLat="{:.2f}".format(row['latitude'])
        sLon="{:.2f}".format(row['longitude'])
        myWidgetVar= formatMyString(str(row['altitude']), 9, 'm')\
                                +formatMyString(affiliationICOS, 12, '')\
                                +formatMyString((sLat), 11, '°N')\
                                +formatMyString((sLon), 10, '°E')\
                                +formatMyString(str(row['dClass']), 8, '')\
                                +'   '+row['dataSetLabel']
        myWidgetOtherLabels  = GridCTkLabel(scrollableFrame4Widgets, gridID, text=myWidgetVar,text_color=sTextColor, text_color_disabled=sTextColor, 
                                                            font=("Georgia", fsNORMAL), textvariable=myWidgetVar, justify="right", anchor="e") 
        myWidgetOtherLabels.grid(row=guiRow, column=colidx, columnspan=6, padx=xPadding, pady=yPadding, sticky='nw')
        widgetsLst.append(myWidgetOtherLabels)
        gridRow.append(myWidgetVar)
        # ###################################################
        # createRowOfWidgets() completed


    def handleMyOptionMenuEvent(self, gridID, widgetsLst,  obsDf, sSamplingHeights, nWidgetsPerRow, activeTextColor, inactiveTextColor):
        ri=int(0.01*gridID)  # row index for the widget on the grid
        ci=int(gridID-(100*ri))  # column index for the widget on the grid
        widgetID=(ri*nWidgetsPerRow)+ci  # calculate the widgetID to access the right widget in widgetsLst
        #print(f"OptionMenuEventHandler: (ri={ri}, ci4={ci}, widgetID={widgetID}, gridID={gridID})")
        if(widgetsLst[widgetID] is not None):
            if(ci==4):  # SamplingHeight
                newSamplingHeight=widgetsLst[widgetID].get() # a string var 
                nPos=0
                try:
                    for sHght in sSamplingHeights:
                        if ((len(newSamplingHeight) == len(sHght)) and (newSamplingHeight in sHght)):
                            # the len() check is necessary or 50m would first be found and matched in 250m
                            swapListElements(sSamplingHeights, 0, nPos)
                            f=[]
                            for s in sSamplingHeights:
                                f1=float(s)
                                f.append(f1)
                            obsDf.at[(ri) ,  ('samplingHeight')] =f
                            # TODO: keep PIDs in sync!
                            pids=obsDf.at[(ri) ,  ('pid')]
                            swapListElements(pids, 0, nPos)
                            obsDf.at[(ri) ,  ('pid')]=pids
                            #print(f"new samplingHeights={obsDf.at[(ri) ,  ('samplingHeight')]}")
                            break
                        nPos+=1
                except:
                    pass


            
    def handleMyCheckboxEvent(self, gridID, widgetsLst, obsDf, nWidgetsPerRow, excludedCountriesList, 
                                                        excludedStationsList, activeTextColor, inactiveTextColor):
        ri=int(0.01*gridID)  # row index for the widget on the grid
        ci=int(gridID-(100*ri))  # column index for the widget on the grid
        row=obsDf.iloc[ri]
        widgetID=(ri*nWidgetsPerRow)+ci  # calculate the corresponding index to access the right widget in widgetsLst
        bChkBxIsSelected=widgetsLst[widgetID].get()
        if(widgetsLst[widgetID] is not None):
            if(ci==0):
                if(bChkBxIsSelected):
                    obsDf.at[(ri) ,  ('selected')] =True
                    if(ri==33):
                        bs=obsDf.at[(ri) ,  ('selected')]
                        bc=obsDf.at[(ri) ,  ('includeCountry')]
                        logger.info(f"obsDf.at[(10) ,  (selected)]   set   to {bs}")
                        logger.info(f"obsDf.at[(10) ,(includeCountry)] set to {bc}")
                    row.iloc[0]=True
                    widgetsLst[widgetID].configure(text_color='blue', text='On')
                else:
                    obsDf.at[(ri) ,  ('selected')] =False
                    if(ri==33):
                        bs=obsDf.at[(ri) ,  ('selected')]
                        bc=obsDf.at[(ri) ,  ('includeCountry')]
                        logger.info(f"obsDf.at[(10) ,  (selected)]   set   to {bs}")
                        logger.info(f"obsDf.at[(10) ,(includeCountry)] set to {bc}")
                    row.iloc[0]=False
                    widgetsLst[widgetID].configure(text_color='green', text='Off')
                self.updateRowOfWidgets(widgetsLst, ri, row, nWidgetsPerRow, activeTextColor, inactiveTextColor)
            elif(ci==1):  # Country
                bSameCountry=True  # multiple rows may be affected
                nRows=len(obsDf)
                thisCountry=obsDf.at[(ri) ,  ('country')]
                while((bSameCountry) and (ri<nRows)):
                    if(bChkBxIsSelected):
                        # Set 'selected' to True only if the station, AltOk & HghtOk are presently selected AND dClass is the highest available, else not
                        try:
                            excludedCountriesList.remove(row['country'])
                        except:
                            pass
                        obsDf.at[(ri) ,  ('includeCountry')] =True
                        if ((row['includeStation']) and (int(row['dClass'])==4) and (row['altOk']) and (row['HghtOk'])) :  #bIncludeStation) 
                            obsDf.at[(ri) ,  ('selected')] =True
                            row.iloc[0]=True
                            if(widgetsLst[widgetID] is not None):
                                widgetsLst[widgetID].configure(text_color=activeTextColor)
                        else:
                            obsDf.at[(ri) ,  ('selected')] =False
                            row.iloc[0]=False
                            if(widgetsLst[widgetID] is not None):
                                widgetsLst[widgetID].configure(text_color=inactiveTextColor)
                        row.iloc[13]=True # 'includeCountry'
                    else:
                        # Remove country from list of excluded countries
                        if(row['country'] not in excludedCountriesList):
                            excludedCountriesList.append(row['country'])
                        obsDf.at[(ri) ,  ('includeCountry')] =False
                        obsDf.at[(ri) ,  ('selected')] =False
                        row.iloc[0]=False
                        row.iloc[13]=False  # 'includeCountry'
                        if(ri==33):
                            bs=obsDf.at[(ri) ,  ('selected')]
                            bc=obsDf.at[(ri) ,  ('includeCountry')]
                            logger.info(f"obsDf.at[(10) ,  (selected)]   set   to {bs}")
                            logger.info(f"obsDf.at[(10) ,(includeCountry)] set to {bc}")
                        if(widgetsLst[widgetID] is not None):
                            widgetsLst[widgetID].configure(text_color=inactiveTextColor)
                    self.updateRowOfWidgets(widgetsLst, ri, row, nWidgetsPerRow, activeTextColor, inactiveTextColor)
                    ri+=1
                    widgetID+=nWidgetsPerRow
                    if(ri>=nRows):
                        break
                    row=obsDf.iloc[ri]
                    if (thisCountry not in row['country']) :
                        bSameCountry=False
            elif(ci==2):  # stationID
                bSameStation=True  # multiple rows may be affected
                nRows=len(obsDf)
                thisStation=obsDf.at[(ri) ,  ('stationID')]
                #includeStationIdx=widgetID+2
                while((bSameStation) and (ri<nRows)):
                    if(bChkBxIsSelected):
                        try:
                            excludedStationsList.remove(row['stationID'])
                        except:
                            pass
                        widgetsLst[widgetID].select()
                        if((obsDf.at[(ri) ,  ('includeCountry')]==True) and
                            (obsDf.at[(ri) ,  ('altOk')]==True) and
                            (obsDf.at[(ri) ,  ('HghtOk')]==True) and
                            (row['dClass']==4)):
                            obsDf.at[(ri) ,  ('selected')] =True
                            row.iloc[0]=True
                            #obsDf.at[(ri) ,  ('includeStation')] =True
                        else:
                            obsDf.at[(ri) ,  ('selected')] =False
                            row.iloc[0]=False
                        obsDf.at[(ri) ,  ('includeStation')] =True
                        #row.iloc[2]=True  !! is not boolean, is the station 3-letter code. use iloc[14] for this
                        row.iloc[14]=True # 'includeStation'
                        widgetsLst[widgetID].configure(text_color=activeTextColor)
                    else:
                        widgetsLst[widgetID].deselect()
                        if(row['stationID'] not in excludedStationsList):
                            excludedStationsList.append(row['stationID'])
                        obsDf.at[(ri) ,  ('selected')] =False
                        row.iloc[0]=False
                        obsDf.at[(ri) ,  ('includeStation')] =False
                        row.iloc[14]=False # 'includeStation'
                        widgetsLst[widgetID].configure(text_color=inactiveTextColor)
                    self.updateRowOfWidgets(widgetsLst, ri, row, nWidgetsPerRow, activeTextColor, inactiveTextColor)
                    ri+=1
                    widgetID+=nWidgetsPerRow
                    #includeStationIdx+=nWidgetsPerRow
                    if(ri>=nRows):
                        break
                    row=obsDf.iloc[ri]
                    if (thisStation not in row['stationID']) :
                        bSameStation=False
 

 
    def updateRowOfWidgets(self, widgetsLst, rowidx, row, nWidgetsPerRow, activeTextColor, inactiveTextColor):
        ''' toggle active/inactiveTextColor and verify the ChkBox states all the widgets belonging to one observational data 
            set corresponding to a single line on the GUI.
            We only modify the state parameters of existing widgets.
        '''
        ri=rowidx
        nWidgetsPerRow=5
        colidx=int(0)  # row['selected']
        # ###################################################
        widgetID=(ri*nWidgetsPerRow)+colidx  # calculate the corresponding index to access the right widget in widgetsLst
        if(widgetsLst[widgetID] is not None):
            b=row['selected']
            if(b):
                widgetsLst[widgetID].select()
                widgetsLst[widgetID].configure(text_color=activeTextColor)
                if(widgetsLst[widgetID+1] is not None):
                    if(widgetsLst[widgetID+1].get() == True): # this Country is not excluded
                        widgetsLst[widgetID+1].configure(text_color=activeTextColor)
                widgetsLst[widgetID+2].configure(text_color=activeTextColor)
                widgetsLst[widgetID+3].configure(text_color=activeTextColor)
                widgetsLst[widgetID+4].configure(text_color=activeTextColor)
            else:
                widgetsLst[widgetID].deselect()
                widgetsLst[widgetID].configure(text_color=inactiveTextColor)
                if(widgetsLst[widgetID+1] is not None):
                    widgetsLst[widgetID+1].configure(text_color=inactiveTextColor)
                widgetsLst[widgetID+2].configure(text_color=inactiveTextColor)
                widgetsLst[widgetID+3].configure(text_color=inactiveTextColor)
                widgetsLst[widgetID+4].configure(text_color=inactiveTextColor)
 
        colidx+=1  # row['includeCountry']
        # ###################################################
        widgetID+=1 # calculate the corresponding index to access the right widget in widgetsLst
        if(widgetsLst[widgetID] is not None):
            bIncludeCountry=row['includeCountry']
            bChkBxIsSelected=widgetsLst[widgetID].get()
            if(bIncludeCountry != bChkBxIsSelected):  # update the widget selection status if necessary
                if(bIncludeCountry): # and  (row['dClass']==4) and  (row['altOk']==True) and (row['HghtOk']==True) ):
                    widgetsLst[widgetID].select()
                else:
                    widgetsLst[widgetID].deselect()
        
        colidx+=1  # row['includeStation'] 
        # ###################################################
        widgetID+=1 # calculate the corresponding index to access the right widget in widgetsLst
        if(widgetsLst[widgetID] is not None):
            try:  # TODO: fix this
                bIncludeStation=row['includeStation']
                bChkBxIsSelected=widgetsLst[widgetID].get()
                if(bIncludeStation != bChkBxIsSelected):  # update the widget selection status if necessary
                    if(bIncludeStation) and  (row['dClass']==4) : #  and  (row['altOk']==True) and (row['HghtOk']==True) ):
                        widgetsLst[widgetID].select()
                    else:
                        widgetsLst[widgetID].deselect()
            except:
                pass
        


    def AskUserAboutWarnings(self):
        GoBackAndFixIt=True
        return(GoBackAndFixIt)
    

        
        
    # This function is used to get the selected
    # options and text from the available entry
    # fields and boxes and then generates
    # a prompt using them
    def checkGuiValues(self, ymlContents,  sNow):
        bErrors=False
        sErrorMsg=""
        bWarnings=False
        sWarningsMsg=""

        # ObservationsFileLocation
        #if (self.iObservationsFileLocation.get()==2):
        #    ymlContents['observations']['file']['location'] = 'CARBONPORTAL'
        #else:
        #    ymlContents['observations']['file']['location'] = 'LOCAL'
            # TODO: get the file name
            

        # Station Filters if any
        bStationFilterError=False
        stationMinAltCommonSense= -100 #m Dead Sea
        stationMaxAltCommonSense= 9000 #m Himalaya
        inletMinHeightCommonSense = 0    # in meters
        inletMaxHeightCommonSense = 850 # in meters World's heighest buildings
        bStationFilterActive=self.FilterStationAltitudesCkb.get() 
        bSamplHghtFilterActive=self.FilterStationAltitudesCkb.get()
        if(bSamplHghtFilterActive):
            #if ((ymlContents['observations']['filters']['bStationAltitude'])or():
            ymlContents['observations']['filters']['bSamplingHeight']=True
            mnh=int(self.inletMinHghtEntry.get())
            mxh=int(self.inletMaxHghtEntry.get())
            if(inletMinHeightCommonSense > mnh):
                bStationFilterError=True # ≥≤
                sErrorMsg+="I can't think of any ICOS station with an inlet height below ground....please fix the minimum inlet height to ≥0m. Thanks.\n"
            if(inletMaxHeightCommonSense+1 < mxh):
                bStationFilterError=True
                sErrorMsg+=f"I can't think of any ICOS station with an inlet height higher than {inletMaxHeightCommonSense}m above ground. Please review your entry. Thanks.\n"
            if(mnh > mxh):
                bStationFilterError=True
                sErrorMsg+="Error: You have entered a maximum inlet height that is below the lowest inlet height. This can only be attributed to human error.\n"
        else:
            ymlContents['observations']['filters']['bSamplingHeight']=False
        if(bStationFilterError):
            self.FilterStationAltitudesCkb.set(False)
            ymlContents['observations']['filters']['bStationAltitude']=False
        if(ymlContents['observations']['filters']['bSamplingHeight']):
            ymlContents['observations']['filters']['inletMaxHeight']=mxh
            ymlContents['observations']['filters']['inletMinHeight']=mnh
        
        bStationFilterError=False        
        if(bStationFilterActive):
            ymlContents['observations']['filters']['bStationAltitude']=True
            mnh=int(self.stationMinAltEntry.get())
            mxh=int(self.stationMaxAltEntry.get())
            if( stationMinAltCommonSense > mnh):
                bStationFilterError=True # ≥≤
                sErrorMsg+=f"I can't think of any ICOS station located at an altitude below the sea level of the Dead Sea....please fix the minimum station altitude to ≥ {stationMinAltCommonSense}m. Thanks.\n"
            if(stationMaxAltCommonSense+1 < mxh):
                bStationFilterError=True
                sErrorMsg+=f"I can't think of any ICOS station located at an altitude higher than {stationMaxAltCommonSense}m above ground. Please review your entry. Thanks.\n"
            if(mnh > mxh):
                bStationFilterError=True
                sErrorMsg+="Error: You have entered a maximum station altitude that is below the lowest station altitude. This can only be attributed to human error.\n"
        else:
            ymlContents['observations']['filters']['bStationAltitude']=False
        if(bStationFilterError):
            self.FilterStationAltitudesCkb.set(False)
            ymlContents['observations']['filters']['bStationAltitude']=False
        if(ymlContents['observations']['filters']['bStationAltitude']):
            ymlContents['observations']['filters']['stationMaxAlt']=mxh
            ymlContents['observations']['filters']['stationMinAlt']=mnh
                               
        if(bStationFilterError):
            bErrors=True
            
        # Deal with any errors or warnings
        return(bErrors, sErrorMsg, bWarnings, sWarningsMsg)
        
    def runSysCmd(self, sCmd,  ignoreError=False):
        try:
            os.system(sCmd)
        except:
            if(ignoreError==False):
                sTxt=f"Fatal Error: Failed to execute system command >>{sCmd}<<. Please check your write permissions and possibly disk space etc."
                logger.warning(sTxt)
                # self.CancelAndQuit(sTxt)

    

def callLumiaGUI(ymlFile, tStart,  tEnd,  scriptDirectory,  step2=False,   fDiscoveredObservations=None,  sNow=''): 
    '''
    Function 
    callLumiaGUI exposes some paramters of the LUMIA config file (in yaml dta format) to a user
    that we have to assume may not be an expert user of LUMIA or worse.-
    Lazy expert users may of course also use this gui for a quick and convenient check of common run parameters...
    
    TODO: 4GUI: implemented different options for dealing with situations where the externally provided file on CO2 background 
    concentrations does not cover all observational data. The user can chose between A) excluding all observations without 
    background concentrations, B) providing one fixed estimate for all missing values or C) calculate a daily mean of all valid
    background concentrations across all observation sites with existing background concentrations
    --	what data is used for the background concentrations (TM5/CAMS for instance). 

    @param ymlContents the contents of the LUMIA YAML configuration file in a yaml object
    @type yaml return object (could be a dictionary or other Python structure
    @param sLogCfgFile Name of the YAML configuration file
    @type string (file name)
    @param sNow  current date+time as string in ISO format
    @type string
    '''
    ymlContents=None
    # Read the yaml configuration file
    tryAgain=False
    try:
        with open(ymlFile, 'r') as file:
            ymlContents = yaml.safe_load(file)
        sCmd="cp "+ymlFile+' '+ymlFile+'.bac' # create a backup file.
        os.system(sCmd)
    except:
        tryAgain=True
    if(tryAgain==True):
        sCmd="cp "+ymlFile+'.bac '+ymlFile # recover from most recent backup file.
        os.system(sCmd)
        try:
            with open(ymlFile, 'r') as file:
                ymlContents = yaml.safe_load(file)
            sCmd="cp "+ymlFile+' '+ymlFile+'.bac' # create a backup file.
            os.system(sCmd)
        except:
            tryAgain=True
            logger.error(f"Abort! Unable to read yaml configuration file {ymlFile} - failed to read its contents with yaml.safe_load()")
            sys.exit(1)
        

    setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'var4d',  'communication'],   value=None, bNewValue=True)
    if(not step2):
        # Read simulation time
        if tStart is None :
            start=pd.Timestamp(ymlContents['observations']['start'])
        else:
            start= pd.Timestamp(tStart)
        # start: '2018-01-01 00:00:00'    
        ymlContents['observations']['start'] = start.strftime('%Y-%m-%d 00:00:00')
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [  'time',  'start'],   value=start.strftime('%Y,%m,%d'), bNewValue=True)
            
        if tEnd is None :
            end=pd.Timestamp(ymlContents['observations']['end'])
        else:
            end= pd.Timestamp(tEnd)
        ymlContents['observations']['end'] = end.strftime('%Y-%m-%d 23:59:59')
        setKeyVal_Nested_CreateIfNecessary(ymlContents, ['time',  'end'],   value= end.strftime('%Y,%m,%d'), bNewValue=True)
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [  'path',  'data'],   value='/data')
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [  'run',  'paths',  'temp'],   value='/temp')
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'paths',  'footprints'],   value='/footprints')
        setKeyVal_Nested_CreateIfNecessary(ymlContents, ['correlation',  'inputdir'],   value='/data/corr' )
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'tag'],  value=args.tag, bNewValue=True)
        
        # Run-dependent paths
        #if(not nestedKeyExists(ymlContents, 'run',  'paths',  'output')):
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'paths',  'output'],   value=os.path.join('/output', args.tag), bNewValue=False)
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'var4d',  'communication'],   value=None)
        s=ymlContents['run']['paths']['temp']+'/congrad.nc'    
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'var4d',  'file'],   value=s)
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'emissions',  '*',  'archive'],   value='rclone:lumia:fluxes/nc/')
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'emissions',  '*',  'path'],   value= '/data/fluxes/nc')
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'model',  'transport',  'exec'],   value='/lumia/transport/multitracer.py')
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'transport',  'output'],   value= 'T')
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'transport',  'steps'],   value='forward')
        
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'branch'],   value='gitkraken://repolink/778bf0763fae9fad55be85dde4b42613835a3528/branch/LumiaDA?url=git%40github.com%3Alumia-dev%2Flumia.git',  bNewValue=True)
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'commit'],   value='gitkraken://repolink/778bf0763fae9fad55be85dde4b42613835a3528/commit/5e5e9777a227631d6ceeba4fd8cff9b241c55de1?url=git%40github.com%3Alumia-dev%2Flumia.git',  bNewValue=True)
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [  'softwareUsed',  'runflex',  'branch'],   value='gitkraken://repolink/b9411fbf7aeeb54d7bb34331a98e2cc0b6db9d5f/branch/v2?url=https%3A%2F%2Fgithub.com%2Flumia-dev%2Frunflex.git',  bNewValue=True)
        setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'runflex',  'commit'],   value='gitkraken://repolink/b9411fbf7aeeb54d7bb34331a98e2cc0b6db9d5f/commit/aad612b36a247046120bda30c8837acb5dec4f26?url=https%3A%2F%2Fgithub.com%2Flumia-dev%2Frunflex.git',  bNewValue=True)
    
    sLogCfgPath=""
    if ((ymlContents['run']['paths']['output'] is None) or len(ymlContents['run']['paths']['output']))<1:
        sLogCfgPath="./"
    else:
        sLogCfgPath=ymlContents['run']['paths']['output']+"/"
        sCmd=("mkdir -p "+ymlContents['run']['paths']['output'])
        try:
            os.system(sCmd)
        except:
            print(".")
    
    # 'system' is default  ctk.set_appearancence_mode("System")
    # Sets the appearance of the window
    # Supported modes : Light, Dark, System
    # "System" sets the appearance mode to
    # the appearance mode of the system
    ctk.set_appearance_mode("System")  
    # background color (R,G,B)=(13,57,64)  = #0D3940
    # Sets the color of the widgets in the window
    # Supported themes : green, dark-blue, blue   
    # ctk.set_default_color_theme("dark-blue")  # Themes: "blue" (standard), "green", "dark-blue"
    ctk.set_default_color_theme(scriptDirectory+"/doc/lumia-dark-theme.json") 
    # root = ctk.CTk()
    if(step2):
        widgetsLst = []
        RefineObsSelectionGUI(sLogCfgPath=sLogCfgPath, ymlContents=ymlContents, ymlFile=ymlFile, fDiscoveredObservations=fDiscoveredObservations, widgetsLst=widgetsLst, sNow=sNow) 
        return("Lumia-Refined-ObsData-"+sNow+".csv") 
    else:
        LumiaGui(sLogCfgPath=sLogCfgPath, ymlContents=ymlContents, ymlFile=ymlFile,  sNow=sNow) 
    return() 

    
     
def add_keys_nested_dict(d, keys, value=None):
    for key in keys:
        if key not in d:
            d[key] = {}
        d = d[key]
    d.setdefault(keys[-1], value)

def setKeyVal_Nested_CreateIfNecessary(myDict, keyLst,   value=None,  bNewValue=False):
    ''' Creates the nested key keyLst in the dictionary myDict if it does not already exist.
        If the key already exists, then the key value is overwritten only if bNewValue is set
    '''
    nKeys=len(keyLst)
    i=int(1)
    for key in keyLst:
        if key not in myDict:
            if(i==nKeys):
                myDict[key] = value
            else:
                myDict[key] = {}
        elif((i==nKeys) and (bNewValue)):
            myDict[key] = value
        i+=1
        myDict = myDict[key]
    #print(".")
        

    
p = argparse.ArgumentParser()
p.add_argument('--gui', dest='gui', default=False, action='store_true',  help="An optional graphical user interface is called at the start of Lumia to ease its configuration.")
p.add_argument('--start', dest='start', default=None, help="Start of the simulation in date+time ISO notation as in \'2018-08-31 00:18:00\'. Overwrites the value in the rc-file")
p.add_argument('--end', dest='end', default=None, help="End of the simulation as in \'2018-12-31 23:59:59\'. Overwrites the value in the rc-file")
p.add_argument('--rcf', dest='rcf', default=None)   # what used to be the resource file (now yaml file) - only yaml format is supported
p.add_argument('--ymf', dest='ymf', default=None)   # yaml configuration file where the user plans his or her Lumia run: parameters, input files etc.
p.add_argument('--step2', dest='step2', default=False, action='store_true',  help="Step 2 to refine the selection among observations discovered on the carbon portal.")
p.add_argument('--fDiscoveredObs', dest='fDiscoveredObservations', default=None,  help="If step2 is set you must specify the .csv file that lists the observations discovered by LUMIA, typically named DiscoveredObservations.csv")   # yaml configuration file where the user plans his or her Lumia run: parameters, input files etc.
p.add_argument('--tag', dest='tag', default='')
p.add_argument('--sNow', dest='sNow', default='',  help="A Timestamp string that can be handed from LUMIA for consistent naming of log files. If called stand-alone, the Timestamp is created automatically.")
p.add_argument('--verbosity', '-v', dest='verbosity', default='INFO')
#args = p.parse_args(sys.argv[1:])
args, unknown = p.parse_known_args(sys.argv[1:])
# Set the verbosity in the logger (loguru quirks ...)
logger.remove()
logger.add(sys.stderr, level=args.verbosity)

current_date = datetime.now()
sNow=current_date.isoformat("T","minutes") # sNow is the time stamp for all log files of a particular run
if(len(args.sNow) > 10):
    sNow=args.sNow

if(args.rcf is None):
    if(args.ymf is None):
        logger.error("LumiaGUI: Fatal error: no user configuration (yaml) file provided.")
        sys.exit(1)
    else:
        ymlFile = args.ymf
else:            
    ymlFile = args.rcf
if (not os.path.isfile(ymlFile)):
    logger.error(f"Fatal error in LumiaGUI: User specified configuration file {ymlFile} does not exist. Abort.")
    sys.exit(-3)
fDiscoveredObservations=None

fDiscoveredObservations='./DiscoveredObservations.csv'
if(args.fDiscoveredObservations is not None):
    fDiscoveredObservations=args.fDiscoveredObservations
bError=False
if((args.step2==True) and (fDiscoveredObservations is None)):
    logger.error("LumiaGUI called for refinement of the observations presumably found (step2) without providing the list of observations to be refined (check option --DiscoveredObs= please).")
    bError=True
elif(os.path.exists(fDiscoveredObservations)==False):
    logger.error(f"LumiaGUI called for refinement of the observations presumably found (step2), but the file name provided for the list of observations ({fDiscoveredObservations}) cannot be found or read. If you are not using the default file, you can provide your own with the --fDiscoveredObs switch.")
    bError=True
if(bError):    
    sCmd="touch LumiaGui.stop"
    try:
        os.system(sCmd)
    except:
        sTxt=f"Fatal Error: Failed to execute system command >>{sCmd}<<. Please check your write permissions and possibly disk space etc."
        logger.warning(sTxt)
    sys.exit(-4)
    
    
# Shall we call the GUI to tweak some parameters before we start the ball rolling?
if args.gui:
    scriptDirectory = os.path.dirname(os.path.abspath(sys.argv[0]))
    callLumiaGUI(ymlFile, args.start,  args.end,  scriptDirectory, args.step2,  fDiscoveredObservations, sNow=sNow)
    logger.info("LumiaGUI window closed")

