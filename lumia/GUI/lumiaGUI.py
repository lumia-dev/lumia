#!/usr/bin/env python3

import os
import sys
from pandas import Timestamp
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
    colW=int(iAvailWidth/nCols)-xPad
    colH=int( iAvailHght/nRows)-yPad
    logger.info(f"avail colWidth= {colW} pxl: colHeight= {colH} pxl")
    (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
    logger.info(f"{sFontFamily}, {FontSize}pt: (w={w},h={h})pxl")
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
        logger.info(f"{sFontFamily}, {FontSize}pt: (w={w},h={h})pxl")
    # If the screen is too small, check if we could stack the output vertically using fewer columns
    if((bCanvasTooSmall) and (bWeCanStackColumns) and (nCols>1)):
        nCols=int((nCols*0.5)+0.5)
        colW=int(iAvailWidth/nCols)-xPad
        font = tkFont.Font(family=sFontFamily, size=FontSize)
        (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
        if(w<colW+1):
            bCanvasTooSmall=False
            bWeMustStack=True
    # Now make the font as big as we can
    bMaxReached=False
    while((FontSize<maxFontSize) and (not bMaxReached) ):
        FontSize=FontSize+1
        font = tkFont.Font(family=sFontFamily, size=FontSize)
        (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
        if (FontSize==maxFontSize):
            bMaxReached=True
        if(w>colW):
            FontSize=FontSize-1
            font = tkFont.Font(family=sFontFamily, size=FontSize)
            (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
            bMaxReached=True
        (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
        logger.info(f"{sFontFamily} {FontSize}: (w={w},h={h})pxl")
    if(h>colH):
        logger.info("We may need a vertical scrollbar...")
    fsNORMAL=FontSize # 12
    fsTINY=int((9*FontSize/fsNORMAL)+0.5)  # 9
    fsSMALL=int((10*FontSize/fsNORMAL)+0.5)  # 10
    fsLARGE=int((15*FontSize/fsNORMAL)+0.5)  # 15
    fsHUGE=int((19*FontSize/fsNORMAL)+0.5)  # 19
    return(fsTINY,  fsSMALL,  fsNORMAL,  fsLARGE,  fsHUGE,  bWeMustStack)


# LumiaGui Class =============================================================
class LumiaGui(ctk.CTk):
    # =====================================================================
    # The layout of the window will be written
    # in the init function itself
    def __init__(self, sLogCfgPath, ymlContents):  # *args, **kwargs):
        #super().__init__(*args, **kwargs)
        # self.protocol("WM_DELETE_WINDOW", self.closed) 

        # Get the screen resolution to scale the GUI in the smartest way possible...
        root=ctk.CTk()
        #root = tk.Tk()
        if(os.path.isfile("LumiaGui.stop")):
            sCmd="rm LumiaGui.stop"
            self.runSysCmd(sCmd,  ignoreError=True)
        if(os.path.isfile("LumiaGui.go")):
            sCmd="rm LumiaGui.go"
            self.runSysCmd(sCmd,  ignoreError=True)
        # self.bPleaseCloseTheGui = tk.BooleanVar(value=False)

        screenWidth = root.winfo_screenwidth()
        screenHeight = root.winfo_screenheight()
        #if (screenWidth > 1900):
        #    screenWidth = 1900
        if((screenWidth/screenHeight) > (1920/1080.0)):  # multiple screens?
            screenWidth=int(screenHeight*(1920/1080.0))
        #maxW=1600
        #maxH=1260
        #if (maxW > (0.92*screenWidth)):
        maxW = int(0.92*screenWidth)
        #if (maxH > (0.92*screenHeight)):
        maxH = int(0.92*screenHeight)
        nCols=8 # sum of labels and entry fields per row
        nRows=12 # number of rows in the GUI
        xPadding=int(0.008*maxW)
        wSpacer=int(2*0.008*maxW)
        yPadding=int(0.008*maxH)
        vSpacer=int(2*0.008*maxH)
        myFontFamily="Georgia"
        sLongestTxt="Latitude (≥33°N):"
        (fsTINY,  fsSMALL,  fsNORMAL,  fsLARGE,  fsHUGE,  bWeMustStackColumns)= \
            calculateEstheticFontSizes(myFontFamily,  maxW,  maxH, sLongestTxt, nCols, nRows, xPad=xPadding, 
                                                        yPad=yPadding, maxFontSize=20,  bWeCanStackColumns=False)
        hDeadSpace=wSpacer+(nCols*xPadding*2)+wSpacer
        vDeadSpace=2*yPadding #vSpacer+(nRows*yPadding*2)+vSpacer
        colWidth=int((maxW - hDeadSpace)/(nCols*1.0))
        rowHeight=int((maxH - vDeadSpace)/(nRows*1.0))
        # Dimensions of the window
        appWidth, appHeight = maxW, maxH
        
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
                                text="LUMIA  --  Configure your next LUMIA run",  font=("Georgia",  fsHUGE))
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
        if ((ymlContents['observations']['filters']['bStationAltitude'])or(ymlContents['observations']['filters']['bInletHeight'])):
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
        for sEntry in  rankingList:
            txt+=sEntry+'\n'
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
                                        values=["LPJ-GUESS","VPRM"], 
                                        variable=self.LandNetExchangeModelCkbVar)
        self.LandNetExchangeOptionMenu.grid(row=6, column=5,
                                        padx=xPadding, pady=yPadding,
                                        columnspan=2, sticky="ew")
 

        # Row 7
        # ################################################################
        # 

        # inlet height filter
        self.bFilterInletHeightCkbVar = tk.BooleanVar(value=ymlContents['observations']['filters']['bInletHeight'])
        self.filterInletHeightCkb = ctk.CTkCheckBox(root,
                                text="Inlet height:",  font=("Georgia",  18), 
                                variable=self.bFilterInletHeightCkbVar, 
                                onvalue=True, offvalue=False)  
        self.filterInletHeightCkb.grid(row=7, column=1,
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
        self.FossilEmisOptionMenu = ctk.CTkOptionMenu(root,
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
                                        values=["mikaloff01"], 
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

        self.bIgnoreWarningsCkbVar = tk.BooleanVar(value=False) # tk.NORMAL
        self.ignoreWarningsCkb = ctk.CTkCheckBox(root,state=tk.DISABLED, 
                            text="Ignore Warnings", font=("Georgia",  fsNORMAL),
                            variable=self.bIgnoreWarningsCkbVar,
                             onvalue=True, offvalue=False)                            
        self.ignoreWarningsCkb.grid(row=9, column=7,
                          padx=xPadding, pady=yPadding,
                          sticky="ew")

       # Row 10
        # ################################################################
        # 
        # Text Box
        self.displayBox = ctk.CTkTextbox(root, width=200,
                                        text_color="red", font=("Georgia",  fsSMALL),  height=100)
        self.displayBox.grid(row=10, column=0, columnspan=6,
                             padx=20, pady=20, sticky="nsew")
                             
        #self.displayBox.delete("0.0", "end")  # delete all text
        self.displayBox.configure(state=tk.DISABLED)  # configure textbox to be read-only

        def GuiClosed():
            if tk.messagebox.askokcancel("Quit", "Do you want to abort your Lumia run?"):
                root.destroy
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
        def GoButtonHit():
            # def generateResults(self):
            bGo=False
            (bErrors, sErrorMsg, bWarnings, sWarningsMsg) = self.checkGuiValues(ymlContents=ymlContents)
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
                current_date = datetime.now()
                sNow=current_date.isoformat("T","minutes")
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
        return
        

    def AskUserAboutWarnings(self):
        GoBackAndFixIt=True
        return(GoBackAndFixIt)
    
 
    # This function is used to get the selected
    # options and text from the available entry
    # fields and boxes and then generates
    # a prompt using them
    def checkGuiValues(self, ymlContents):
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
                tStart=Timestamp(strStartTime)
            except:
                bTimeError=True
                sErrorMsg+='Invalid or corrupted Start Date entered. Please use the ISO format YYY-MM-DD when entering dates.\n'
            try:
                tEnd=Timestamp(strEndTime)
            except:
                bTimeError=True
                sErrorMsg+='Invalid or corrupted End Date entered. Please use the ISO format YYY-MM-DD when entering dates.\n'
        if (not bTimeError): 
            current_date = datetime.now()
            sNow=current_date.isoformat("T","minutes")
            tMin=Timestamp('1970-01-01 00:00:00')
            tMax=Timestamp(sNow[0:10]+' 23:59:59')
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
            if(self.bFilterInletHeightCkbVar.get()):
                ymlContents['observations']['filters']['bInletHeight']=True
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
                ymlContents['observations']['filters']['bInletHeight']=False
            if(ymlContents['observations']['filters']['bInletHeight']):
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



    

def callLumiaGUI(ymlFile, tStart,  tEnd,  scriptDirectory):    # ymlContents, sLogCfgPath):
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
    try:
        with open(ymlFile, 'r') as file:
            ymlContents = yaml.safe_load(file)
    except:
        logger.error(f"Abort! Unable to read yaml configuration file {ymlFile} - failed to read its contents with yaml.safe_load()")
        sys.exit(1)
        
        
    # Read simulation time
    if tStart is None :
        start=Timestamp(ymlContents['observations']['start'])
    else:
        start= Timestamp(tStart)
    # start: '2018-01-01 00:00:00'    
    ymlContents['observations']['start'] = start.strftime('%Y-%m-%d 00:00:00')
    setKeyVal_NestedLv2_CreateIfNotPresent(ymlContents, 'time',  'start',  start.strftime('%Y,%m,%d'))
        
    if tEnd is None :
        end=Timestamp(ymlContents['observations']['end'])
    else:
        end= Timestamp(tEnd)
    ymlContents['observations']['end'] = end.strftime('%Y-%m-%d 23:59:59')
    setKeyVal_NestedLv2_CreateIfNotPresent(ymlContents, 'time',  'end',  end.strftime('%Y,%m,%d'))
    
    setKeyVal_NestedLv2_CreateIfNotPresent(ymlContents, 'path',  'data',  '/data')
    setKeyVal_NestedLv3_CreateIfNotPresent(ymlContents, 'run',  'paths',  'temp',  '/temp')
    setKeyVal_NestedLv3_CreateIfNotPresent(ymlContents, 'run',  'paths',  'footprints',  '/footprints')
    setKeyVal_NestedLv2_CreateIfNotPresent(ymlContents, 'correlation',  'inputdir', '/data/corr' )
    if not ('tag' in ymlContents):
        add_keys_nested_dict(ymlContents, ['tag'], args.tag)
    #if (ymlContents['tag'] is None):
    #    ymlContents['tag'] = args.tag 
    
    # Run-dependent paths
    setKeyVal_NestedLv3_CreateIfNotPresent(ymlContents, 'run',  'paths',  'output',  os.path.join('/output', args.tag))
    s=ymlContents['run']['paths']['temp']+'/congrad.nc'    
    setKeyVal_NestedLv3_CreateIfNotPresent(ymlContents, 'var4d',  'communication',  'file',  s)
    setKeyVal_NestedLv3_CreateIfNotPresent(ymlContents, 'emissions',  '*',  'archive',  'rclone:lumia:fluxes/nc/')
    setKeyVal_NestedLv3_CreateIfNotPresent(ymlContents, 'emissions',  '*',  'path',  '/data/fluxes/nc')
    setKeyVal_NestedLv3_CreateIfNotPresent(ymlContents, 'model',  'transport',  'exec',  '/lumia/transport/multitracer.py')
    setKeyVal_NestedLv2_CreateIfNotPresent(ymlContents, 'transport',  'output', 'T' )
    setKeyVal_NestedLv3_CreateIfNotPresent(ymlContents, 'transport',  'output', 'steps',  'forward' )
    
    #ymlContents['softwareUsed']['lumia']['branch']= 'gitkraken://repolink/778bf0763fae9fad55be85dde4b42613835a3528/branch/LumiaDA?url=git%40github.com%3Alumia-dev%2Flumia.git'
    #ymlContents['softwareUsed']['lumia']['commit']= 'gitkraken://repolink/778bf0763fae9fad55be85dde4b42613835a3528/commit/5e5e9777a227631d6ceeba4fd8cff9b241c55de1?url=git%40github.com%3Alumia-dev%2Flumia.git'
    #ymlContents['softwareUsed']['runflex']['branch']= 'gitkraken://repolink/b9411fbf7aeeb54d7bb34331a98e2cc0b6db9d5f/branch/v2?url=https%3A%2F%2Fgithub.com%2Flumia-dev%2Frunflex.git'
    #ymlContents['softwareUsed']['runflex']['commit']= 'gitkraken://repolink/b9411fbf7aeeb54d7bb34331a98e2cc0b6db9d5f/commit/aad612b36a247046120bda30c8837acb5dec4f26?url=https%3A%2F%2Fgithub.com%2Flumia-dev%2Frunflex.git'
    setKeyVal_NestedLv3_CreateIfNotPresent(ymlContents, 'softwareUsed',  'lumia',  'branch',  'gitkraken://repolink/778bf0763fae9fad55be85dde4b42613835a3528/branch/LumiaDA?url=git%40github.com%3Alumia-dev%2Flumia.git')
    setKeyVal_NestedLv3_CreateIfNotPresent(ymlContents, 'softwareUsed',  'lumia',  'commit',  'gitkraken://repolink/778bf0763fae9fad55be85dde4b42613835a3528/commit/5e5e9777a227631d6ceeba4fd8cff9b241c55de1?url=git%40github.com%3Alumia-dev%2Flumia.git')
    setKeyVal_NestedLv3_CreateIfNotPresent(ymlContents, 'softwareUsed',  'runflex',  'branch',  'gitkraken://repolink/b9411fbf7aeeb54d7bb34331a98e2cc0b6db9d5f/branch/v2?url=https%3A%2F%2Fgithub.com%2Flumia-dev%2Frunflex.git')
    setKeyVal_NestedLv3_CreateIfNotPresent(ymlContents, 'softwareUsed',  'runflex',  'commit',  'gitkraken://repolink/b9411fbf7aeeb54d7bb34331a98e2cc0b6db9d5f/commit/aad612b36a247046120bda30c8837acb5dec4f26?url=https%3A%2F%2Fgithub.com%2Flumia-dev%2Frunflex.git')
    
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
    LumiaGui(sLogCfgPath=sLogCfgPath, ymlContents=ymlContents) 
    return 


    
     
def add_keys_nested_dict(d, keys, value=None):
    for key in keys:
        if key not in d:
            d[key] = {}
        d = d[key]
    d.setdefault(keys[-1], value)

def setKeyVal_NestedLv2_CreateIfNotPresent(yDict, key1,  key2,  value):
    if not ((key1 in yDict) and (key2 in yDict[key1])):
        add_keys_nested_dict(yDict, [key1, key2], value)

def setKeyVal_NestedLv3_CreateIfNotPresent(yDict, key1,  key2, key3,   value):
    setKeyVal_NestedLv2_CreateIfNotPresent(yDict, key1,  key2,  'None')
    if not ((key2 in yDict[key1]) and (key3 in yDict[key1][key2])):
        add_keys_nested_dict(yDict[key1], [key2, key3],  value)
    
    
p = argparse.ArgumentParser()
p.add_argument('--gui', dest='gui', default=False, action='store_true',  help="An optional graphical user interface is called at the start of Lumia to ease its configuration.")
p.add_argument('--start', dest='start', default=None, help="Start of the simulation in date+time ISO notation as in \'2018-08-31 00:18:00\'. Overwrites the value in the rc-file")
p.add_argument('--end', dest='end', default=None, help="End of the simulation as in \'2018-12-31 23:59:59\'. Overwrites the value in the rc-file")
p.add_argument('--rcf', dest='rcf', default=None)   # what used to be the resource file (now yaml file) - only yaml format is supported
p.add_argument('--ymf', dest='ymf', default=None)   # yaml configuration file where the user plans his or her Lumia run: parameters, input files etc.
p.add_argument('--tag', dest='tag', default='')
p.add_argument('--verbosity', '-v', dest='verbosity', default='INFO')
#args = p.parse_args(sys.argv[1:])
args, unknown = p.parse_known_args(sys.argv[1:])
# Set the verbosity in the logger (loguru quirks ...)
logger.remove()
logger.add(sys.stderr, level=args.verbosity)

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

# Shall we call the GUI to tweak some parameters before we start the ball rolling?
if args.gui:
    scriptDirectory = os.path.dirname(os.path.abspath(sys.argv[0]))
    callLumiaGUI(ymlFile, args.start,  args.end,  scriptDirectory)
    logger.info("LumiaGUI window closed")
