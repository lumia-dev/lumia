#!/usr/bin/env python3


import os
import sys
import housekeeping as hk
import pandas as pd
import argparse
from datetime import datetime,  timedelta
import yaml
import time
import _thread
import re
from loguru import logger
from pandas import to_datetime
from screeninfo import get_monitors

global AVAIL_LAND_NETEX_DATA  # Land/vegetation net exchange model emissions that are supported
AVAIL_LAND_NETEX_DATA=["LPJ-GUESS","VPRM"]
global AVAIL_FOSSIL_EMISS_DATA      # FOSSIL Emissions data sets supported
AVAIL_FOSSIL_EMISS_DATA=["EDGARv4_LATEST","EDGARv4.3_BP2021_CO2_EU2_2020","EDGARv4.3_BP2021_CO2_EU2_2019", "EDGARv4.3_BP2021_CO2_EU2_2018"]
global AVAIL_OCEAN_NETEX_DATA      # Ocean Net Exchange data sets supported
AVAIL_OCEAN_NETEX_DATA=["mikaloff01"]
        # Fossil emissions combo box.  Latest version at time of writing: https://meta.icos-cp.eu/collections/GP-qXikmV7VWgG4G2WxsM1v3
        #  https://hdl.handle.net/11676/Ce5IHvebT9YED1KkzfIlRwDi (2022) https://doi.org/10.18160/RFJD-QV8J EDGARv4.3 and BP statistics 2023
        # https://hdl.handle.net/11676/6i-nHIO0ynQARO3AIbnxa-83  EDGARv4.3_BP2021_CO2_EU2_2020.nc  
        # https://hdl.handle.net/11676/ZU0G9vak8AOz-GprC0uY-HPM  EDGARv4.3_BP2021_CO2_EU2_2019.nc
        # https://hdl.handle.net/11676/De0ogQ4l6hAsrgUwgjAoGDoy EDGARv4.3_BP2021_CO2_EU2_2018.nc


USE_TKINTER=True
scriptName=sys.argv[0]
if('.ipynb' in scriptName[-6:]):
    USE_TKINTER=False
# For testing of ipywidgets uncomment the next line (even if not a notebook)
# USE_TKINTER=False
if(USE_TKINTER):
    import guiElementsTk as ge
    import tkinter as tk    
    import customtkinter as ctk
else:
    import guiElements_ipyWdg as ge
    from IPython.display import display, HTML,  clear_output
from ipywidgets import widgets
import boringStuff as bs
MIN_SCREEN_RES=480 # pxl - just in case querying screen size fails for whatever reason...



# =============================================================================
# small helper functions for mostly mundane tasks
# =============================================================================

def cleanUp(self,  bWriteStop=True):  # of lumiaGuiApp
    if(bWriteStop): # the user selected Cancel - else the LumiaGui.go message has already been written
        logger.info("LumiaGUI was canceled.")
        sCmd="touch LumiaGui.stop"
        hk.runSysCmd(sCmd)

def prepareCallToLumiaGUI(ymlFile,  scriptDirectory, iVerbosityLv=1): 
    '''
    Function 
    callLumiaGUI exposes some paramters of the LUMIA config file (in yaml data format) to a user
    that we have to assume may not be an expert user of LUMIA or worse.-
    Lazy expert users may of course also use this gui for a quick and convenient check of common run parameters...
    
    TODO: 4GUI: implemented different options for dealing with situations where the externally provided file on CO2 background 
    concentrations does not cover all observational data. The user can chose between A) excluding all observations without 
    background concentrations, B) providing one fixed estimate for all missing values or C) calculate a daily mean of all valid
    background concentrations across all observation sites with existing background concentrations
    --	what data is used for the background concentrations (TM5/CAMS for instance). 

    @param ymlFile : the LUMIA YAML configuration file in yaml (or rc) data format (formatted text)
    @type string (file name)
    @param scriptDirectory : where in the storage this script lives.  - e.g. used to look for .git files for the self-documentation of each run
    @type string (file name)
    '''
    # Read the yaml configuration file
    ymlContents=readMyYamlFile(ymlFile)
        
    # sNow=ymlContents['run']['thisRun']['uniqueIdentifierDateTime'] # sNow is the time stamp for all log files of a particular run
    # All output is written into  subdirectories (defined by run.paths.output and run.paths.temp) followed by a directory level named 
    # after the run.thisRun.uniqueIdentifierDateTime key which is also what all subsequent output filenames are starting with.

    # hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'uniqueIdentifierDateTime'],   value=sNow, bNewValue=True)
    # sOutputPrfx=ymlContents[ 'run']['thisRun']['uniqueOutputPrefix']
    # sTmpPrfx=ymlContents[ 'run']['thisRun']['uniqueTmpPrefix'] 

    # Ensure we can write any log files into the intended directory
    sLogCfgPath=""
    if ((ymlContents['run']['paths']['output'] is None) or len(ymlContents['run']['paths']['output']))<1:
        sLogCfgPath="./"
    else:
        sLogCfgPath=ymlContents['run']['paths']['output']+"/"
        sCmd=("mkdir -p "+ymlContents['run']['paths']['output'])
        try:
            os.system(sCmd)
        except:
            logger.error('Abort. Unable to write log files. Unable to create requested output directory.')
    
    # remove old message files - these are only relevant if LumiaGUI is used in an automated workflow as they signal
    # success or failure of this step in the workflow
    if(os.path.isfile("LumiaGui.stop")):
        sCmd="rm LumiaGui.stop"
        hk.runSysCmd(sCmd,  ignoreError=True)
    if(os.path.isfile("LumiaGui.go")):
        sCmd="rm LumiaGui.go"
        hk.runSysCmd(sCmd,  ignoreError=True)
        
    myDsp=os.environ.get('DISPLAY','')
    if (myDsp == ''):
        logger.warning('DISPLAY not listed in os.environ. On simple systems DISPLAY is usually :0.0  ...so I will give that one a shot. Proceeding with fingers crossed...')
        os.environ.__setitem__('DISPLAY', ':0.0')
    else:
        logger.debug(f'found Display {myDsp}')

    root=None
    if(USE_TKINTER):
        root = LumiaTkGui() # is the ctk.CTk() root window
        lumiaGuiAppInst=lumiaGuiApp(root)
        lumiaGuiAppInst.sLogCfgPath = sLogCfgPath 
        lumiaGuiAppInst.ymlContents = ymlContents
        lumiaGuiAppInst.ymlFile = ymlFile
        guiPg1TpLv=lumiaGuiAppInst.guiPage1AsTopLv(iVerbosityLv)
        lumiaGuiAppInst.runPage2(iVerbosityLv)  # of lumiaGuiApp
        root.mainloop()
        sys.exit(0)
    else:
        notify_output = widgets.Output()
        display(notify_output)
    '''
    (bFirstGuiPageSuccessful, ymlContents)=LumiaGuiPage1(root, sLogCfgPath, ymlContents, ymlFile, bRefine=False, iVerbosityLv=1) 
    
    # Go hunt for the data
    
    # Present to the user the data sets found
    '''
    logger.info('LumiaGUI completed successfully. The updated Lumia config file has been written to:')
    logger.info(ymlFile)
    return


# Plan the layout of the GUI - get screen dimensions, choose a reasonable font size for it, xPadding, etc.
def stakeOutSpacesAndFonts(guiWindow, nCols, nRows):
    guiWindow.appWidth, guiWindow.appHeight,  guiWindow.xPadding, guiWindow.yPadding,  guiWindow.xoffset = displayGeometry(maxAspectRatio=1.2)
    wSpacer=2*guiWindow.xPadding
    guiWindow.vSpacer=2*guiWindow.yPadding
    likedFonts=["Georgia", "Liberation","Arial", "Microsoft","Ubuntu","Helvetica"]
    sLongestTxt="Start date (00:00h):"  # "Latitude (≥33°N):"
    for myFontFamily in likedFonts:
        (bFontFound, guiWindow.fsTINY,  guiWindow.fsSMALL,  guiWindow.fsNORMAL,  guiWindow.fsLARGE,  guiWindow.fsHUGE,  guiWindow.fsGIGANTIC,  bWeMustStackColumns, bSuccess)= \
            bs.calculateEstheticFontSizes(myFontFamily,  guiWindow.appWidth,  guiWindow.appHeight, sLongestTxt, nCols, nRows, 
                                                            xPad=guiWindow.xPadding, yPad=guiWindow.yPadding, maxFontSize=20, 
                                                            USE_TKINTER=USE_TKINTER, bWeCanStackColumns=False)
        if(not bSuccess):
            if(not bFontFound):
                myFontFamily="Times New Roman"  # should exist on any operating system with western language fonts installed...
        if(bFontFound):
            break
    if(not bFontFound):
        myFontFamily="Times New Roman"  # should exist on any operating system with western language fonts installed...
    guiWindow.myFontFamily=myFontFamily
    hDeadSpace=wSpacer+(nCols*guiWindow.xPadding*2)+wSpacer
    vDeadSpace=2*guiWindow.yPadding #vSpacer+(nRows*guiWindow.yPadding*2)+vSpacer
    guiWindow.colWidth=int((guiWindow.appWidth - hDeadSpace)/(nCols*1.0))
    guiWindow.rowHeight=int((guiWindow.appHeight - vDeadSpace)/(nRows*1.0))
    return()



# =============================================================================
# Tkinter solution for GUI
# =============================================================================
class lumiaGuiApp:
    def __init__(self, root):
        self.root = root
        self.guiPg1TpLv=None
        self.label1 = tk.Label(self.root, text="App main window - hosting the second GUI page.")
        self.root.protocol("WM_DELETE_WINDOW", self.closeApp)
        self.label1.pack()
        
    def closeTopLv(self, bWriteStop=True):  # of lumiaGuiApp
        self.guiPg1TpLv.destroy()
        if(bWriteStop):
            cleanUp(bWriteStop)
            logger.info('lumiaGUI canceled by user.')
            self.closeApp(False)
        self.guiPg1TpLv=None
        self.root.deiconify()
        #self.runPage2()  # done in parent method

    def closeApp(self, bWriteStop=True):  # of lumiaGuiApp
        cleanUp(bWriteStop)
        logger.info("Closing the GUI...")
        self.root.destroy()
        if(bWriteStop):
            logger.info('lumiaGUI canceled by user.')
        else:
            # TODO: write the GO message to file
            logger.info(f'LumiaGUI completed successfully. The updated Lumia config file has been written to: {self.ymlFile}')
        sys.exit(0)

    # ====================================================================
    #  Event handler for widget events from first GUI page of lumiaGuiApp 
    # ====================================================================
    def EvHdPg1ExitWithSuccess(self):
        self.closeApp(bWriteStop=False)

    def EvHdPg1SetObsFileLocation(self):
        # "TracerRadioButton, iTracerRbVal"
        if (self.iObservationsFileLocation.get()==1):
           self.ymlContents['observations'][self.tracer]['file']['location'] = 'LOCAL'
           
        else:
            self.ymlContents['observations'][self.tracer]['file']['location'] = 'CARBONPORTAL'

    def EvHdPg1SetTracer(self):
        # "TracerRadioButton, iTracerRbVal"
        if (self.iTracerRbVal.get()==1):
           self.ymlContents['run']['tracers'] = 'co2'
        else:
            self.ymlContents['run']['tracers'] = 'ch4'
        # applyRules()
        
    def EvHdPg1GotoPage2(self):
        self.closeTopLv(bWriteStop=False)


    # ====================================================================
    # body & brain of first GUI page  -- part of lumiaGuiApp (toplevel window)
    # ====================================================================
        
    def guiPage1AsTopLv(self, iVerbosityLv='INFO'):  # of lumiaGuiApp
        if(self.guiPg1TpLv is None):
            self.guiPg1TpLv = tk.Toplevel(self.root,  bg="cadet blue")
        self.root.iconify()

        # Plan the layout of the GUI - get screen dimensions, choose a reasonable font size for it, xPadding, etc.
        nCols=5 # sum of labels and entry fields per row
        nRows=13 # number of rows in the GUI
        stakeOutSpacesAndFonts(self.root, nCols, nRows)
        xPadding=self.root.xPadding
        yPadding=self.root.yPadding
        #activeTextColor='gray10'
        #inactiveTextColor='gray50'
        # Dimensions of the window
        appWidth, appHeight = self.root.appWidth, self.root.appHeight
        # action if the gui window is closed by the user (==canceled)
        self.guiPg1TpLv.protocol("WM_DELETE_WINDOW", self.closeTopLv)

        # ====================================================================
        # Creation of all widgets of first GUI page  -- part of lumiaGuiApp (toplevel window)
        # ====================================================================
        # 
        # Prepare some data and initial values
        # Get the current values from the yamlFile or previous user entry:
        startDate=str(self.ymlContents['run']['time']['start'])[:10]  # e.g. '2018-01-01 00:00:00'
        endDate=str(self.ymlContents['run']['time']['end'])[:10]   # e.g. '2018-02-01 23:59:59'
        self.sStartDate=ge.guiStringVar(value=startDate)
        self.sEndDate=ge.guiStringVar(value=endDate)
        # Geographical region on which to operate (Lat/Lon box)
        # grid: ${Grid:{lon0:-15, lat0:33, lon1:35, lat1:73, dlon:0.25, dlat:0.25}}
        # sRegion="lon0=%.3f, lon1=%.3f, lat0=%.3f, lat1=%.3f, dlon=%.3f, dlat=%.3f, nlon=%d, nlat=%d"%(regionGrid.lon0, regionGrid.lon1,  regionGrid.lat0,  regionGrid.lat1,  regionGrid.dlon,  regionGrid.dlat,  regionGrid.nlon,  regionGrid.nlat)
        self.lonMin = ge.guiDoubleVar(value=-25.0)  # this is a property of Lumia and protects against use it was not designed or tested for
        self.lonMax = ge.guiDoubleVar(value=45.0)
        self.latMin = ge.guiDoubleVar(value=23.0)
        self.latMax = ge.guiDoubleVar(value=83.0)
        Lat0=self.ymlContents['run']['region']['lat0']  # 33.0
        Lat1=self.ymlContents['run']['region']['lat1']   #73.0
        self.sLat0=ge.guiStringVar(value=f'{Lat0:.3f}')
        self.sLat1=ge.guiStringVar(value=f'{Lat1:.3f}')
        Lon0=self.ymlContents['run']['region']['lon0']  # -15.0
        Lon1=self.ymlContents['run']['region']['lon1']   #35.0
        self.sLon0=ge.guiStringVar(value=f'{Lon0:.3f}')
        self.sLon1=ge.guiStringVar(value=f'{Lon1:.3f}')
        # Get the currently selected tracer from the yml config file
        tracers = self.ymlContents['run']['tracers']
        if (isinstance(tracers, list)):
            self.tracer=tracers[0]
        else:
            self.tracer=self.ymlContents['run']['tracers']
        # Set the Tracer radiobutton initial status in accordance with the (first) tracer extracted from the yml config file
        self.iTracerRbVal= ge.guiIntVar(value=1)
        if(('ch4' in self.tracer) or ('CH4' in self.tracer)):
            self.iTracerRbVal = ge.guiIntVar(value=2)
        else:
            self.iTracerRbVal = ge.guiIntVar(value=1)
        # Land/vegetation net exchange drop down combo box - initial values and drop-down lists
        self.LandNetExchangeModelCkbVar = ge.guiStringVar(value=self.ymlContents['emissions'][self.tracer]['categories']['biosphere']['origin'])
        # Fossil emissions combo box.  Latest version at time of writing: https://meta.icos-cp.eu/collections/GP-qXikmV7VWgG4G2WxsM1v3
        self.FossilEmisCkbVar = ge.guiStringVar(value=self.ymlContents['emissions'][self.tracer]['categories']['fossil']['origin']) #"EDGARv4_LATEST"
        # Ocean Net Exchange combo box (a prioris)
        self.OceanNetExchangeCkbVar = ge.guiStringVar(value=self.ymlContents['emissions'][self.tracer]['categories']['ocean']['origin'])  # "mikaloff01"
        global AVAIL_LAND_NETEX_DATA       # Land/Vegetation Net Exchange combo box
        global AVAIL_FOSSIL_EMISS_DATA      # FOSSIL Emissions data sets supported
        global AVAIL_OCEAN_NETEX_DATA     # Ocean Net Exchange combo box
        # Obs data location radiobutton variable (local vs carbon portal)
        self.iObservationsFileLocation= ge.guiIntVar(value=1)
        if('CARBONPORTAL' in self.ymlContents['observations'][self.tracer]['file']['location']):
            self.iObservationsFileLocation = ge.guiIntVar(value=2)
        #       Ignore ChkBx
        self.bIgnoreWarningsCkbVar = ge.guiBooleanVar(value=False) 
        # initial values --  chose what categories to adjust (checkboxes)
        self.LandVegCkbVar = ge.guiBooleanVar(value=True)
        self.FossilCkbVar = ge.guiBooleanVar(value=False)
        self.OceanCkbVar = ge.guiBooleanVar(value=False)

        
        # Row 0:  Title Label
        # ################################################################
        title="LUMIA  --  Configure your next LUMIA run"
        self.Pg1TitleLabel = ge.guiTxtLabel(self.guiPg1TpLv, title, fontName=self.root.myFontFamily, fontSize=self.root.fsGIGANTIC, style="bold")
        # Row 1:  Time interval Heading
        # ################################################################
        self.Pg1LatitudesLabel = ge.guiTxtLabel(self.guiPg1TpLv, text="Time interval",  fontName=self.root.myFontFamily,  fontSize=self.root.fsLARGE)
        # Row 2: Time interval Entry
        # ################################################################
        self.Pg1TimeStartLabel = ge.guiTxtLabel(self.guiPg1TpLv, anchor="w",
                        text="Start date (00:00h):", width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        self.Pg1TimeStartEntry = ge.guiDataEntry(self.guiPg1TpLv,textvariable=self.sStartDate,  
                          placeholder_text=str(self.ymlContents['run']['time']['start'])[:10], width=self.root.colWidth)
        self.Pg1TimeEndLabel = ge.guiTxtLabel(self.guiPg1TpLv,
                        text="End date: (23:59h)", width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        self.Pg1TimeEndEntry = ge.guiDataEntry(self.guiPg1TpLv,textvariable=self.sEndDate, 
                          placeholder_text=str(self.ymlContents['run']['time']['end'])[:10], width=self.root.colWidth)
        # Row 3:  Geographical Region Heading & Message Box
        # ################################################################
        self.Pg1LatitudesLabel = ge.guiTxtLabel(self.guiPg1TpLv, anchor="w", text="Geographical extent of the area modelled (in deg. North/East)",
                                                                    fontName=self.root.myFontFamily,  fontSize=self.root.fsLARGE)
        #    Text Box for messages, warnings, etc
        self.Pg1displayBox = ge.guiTextBox(self.guiPg1TpLv, width=200,  height=100,  fontName=self.root.myFontFamily,  fontSize=self.root.fsSMALL, text_color="red") 
        self.Pg1displayBox.configure(state=tk.DISABLED)  # configure textbox to be read-only
        # Row 4: Latitudes Entry Fields
        # ################################################################
        txt=f"Latitude (≥{self.latMin.get()}°N):" # "Latitude (≥33°N):"
        self.Pg1LatitudeMinLabel = ge.guiTxtLabel(self.guiPg1TpLv,anchor="w", text=txt, width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        self.Pg1LatitudeMaxLabel = ge.guiTxtLabel(self.guiPg1TpLv, text="max (≤73°N):", width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        self.Pg1Latitude0Entry = ge.guiDataEntry(self.guiPg1TpLv,textvariable=self.sLat0, placeholder_text=self.sLat0, width=self.root.colWidth)
        self.Pg1Latitude1Entry = ge.guiDataEntry(self.guiPg1TpLv,textvariable=self.sLat1, placeholder_text=self.sLat1, width=self.root.colWidth)
        # Row 5: Longitudes Entry Fields
        # ################################################################
        self.Pg1LongitudeMinLabel = ge.guiTxtLabel(self.guiPg1TpLv, text="Longitude (≥-15°E):", anchor="w", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, width=self.root.colWidth)
        self.Pg1LongitudeMaxLabel = ge.guiTxtLabel(self.guiPg1TpLv, text="max (≤35°E):",  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, width=self.root.colWidth)
        self.Pg1Longitude0Entry = ge.guiDataEntry(self.guiPg1TpLv, textvariable=self.sLon0, placeholder_text=self.sLon0, width=self.root.colWidth)
        self.Pg1Longitude1Entry = ge.guiDataEntry(self.guiPg1TpLv, textvariable=self.sLon1, placeholder_text=self.sLon1, width=self.root.colWidth)
        # Row 6:  Tracer radiobutton (CO2/CH4)
        # ################################################################
        self.Pg1TracerLabel = ge.guiTxtLabel(self.guiPg1TpLv,anchor="w", text="Tracer:", width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        self.Pg1TracerRadioButton = ge.guiRadioButton(self.guiPg1TpLv,
                                   text="CO2", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, 
                                   variable=self.iTracerRbVal,  value=1, command=self.EvHdPg1SetTracer)
        self.Pg1TracerRadioButton = ge.guiRadioButton(self.guiPg1TpLv,
                                   text="CH4", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                                   variable=self.iTracerRbVal,  value=2, command=self.EvHdPg1SetTracer)
        # Row 7: Emissions data (a prioris): Heading and Land/Vegetation choice
        # ################################################################
        self.Pg1EmissionsLabel = ge.guiTxtLabel(self.guiPg1TpLv, anchor="w", text="Emissions data (a priori)",  fontName=self.root.myFontFamily,  fontSize=self.root.fsLARGE)
        #       Emissions data (a prioris) : dyn.vegetation net exchange model
        self.Pg1NeeLabel = ge.guiTxtLabel(self.guiPg1TpLv, text="Land/Vegetation NEE:", width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        #       Land/Vegetation Net Exchange combo box
        self.Pg1LandNetExchangeOptionMenu = ge.guiOptionMenu(self.guiPg1TpLv,  values=AVAIL_LAND_NETEX_DATA, variable=self.LandNetExchangeModelCkbVar, 
                                                                                                        dropdown_fontName=self.root.myFontFamily, dropdown_fontSize=self.root.fsNORMAL)
        # Row 8: Emissions data (a prioris) continued: fossil+ocean
        # ################################################################
        self.Pg1FossilEmisLabel = ge.guiTxtLabel(self.guiPg1TpLv, text="Fossil emissions:", width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        self.Pg1FossilEmisOptionMenu = ge.guiOptionMenu(self.guiPg1TpLv, values=AVAIL_FOSSIL_EMISS_DATA, 
                                        variable=self.FossilEmisCkbVar, dropdown_fontName=self.root.myFontFamily, dropdown_fontSize=self.root.fsNORMAL)
        # Ocean Net Exchange combo box (a prioris)
        self.Pg1OceanNetExchangeLabel = ge.guiTxtLabel(self.guiPg1TpLv, text="Ocean net exchange:", width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        self.Pg1OceanNetExchangeOptionMenu = ge.guiOptionMenu(self.guiPg1TpLv, values=AVAIL_OCEAN_NETEX_DATA,
                                        variable=self.OceanNetExchangeCkbVar, dropdown_fontName=self.root.myFontFamily, dropdown_fontSize=self.root.fsNORMAL)
        # Row 9: Obs data location radiobutton
        # ################################################################
        self.Pg1ObsFileLocationCPortalRadioButton = ge.guiRadioButton(self.guiPg1TpLv,
                                   text="Ranking of Obsdata from CarbonPortal", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, 
                                   variable=self.iObservationsFileLocation,  value=2, command=self.EvHdPg1SetObsFileLocation)
        self.Pg1ObsFileLocationLocalRadioButton = ge.guiRadioButton(self.guiPg1TpLv,
                                   text="Observational CO2 data from local file", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                                   variable=self.iObservationsFileLocation,  value=1, command=self.EvHdPg1SetObsFileLocation)
        # Row 10: Obs data entries
        # ################################################################

        # Ranking of data records from CarbonPortal
        rankingList=self.ymlContents['observations'][self.tracer]['file']['ranking']
        self.Pg1ObsFileRankingTbxVar = ge.guiStringVar(value="ObsPack")
        self.Pg1ObsFileRankingBox = ge.guiTextBox(self.guiPg1TpLv, width=self.root.colWidth,  height=(2*self.root.rowHeight+self.root.vSpacer),  fontName=self.root.myFontFamily,  fontSize=self.root.fsSMALL)
        txt=""
        rank=4
        for sEntry in  rankingList:
            txt+=str(rank)+': '+sEntry+'\n'
            rank=rank-1
        self.Pg1ObsFileRankingBox.insert('0.0', txt)  # insert at line 0 character 0
        # Label for local  obs data path
        self.Pg1ObsFileLocationLocalPathLabel = ge.guiTxtLabel(self.guiPg1TpLv, text="Local.file.with.obs.data:", width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        # Row 11:  Obs data entries
        # ################################################################
        self.Pg1ObsFileLocationLocalEntry = ge.guiDataEntry(self.guiPg1TpLv, placeholder_text= self.ymlContents['observations'][self.tracer]['file']['path'], width=self.root.colWidth)
        #       Ignore ChkBx
        self.Pg1ignoreWarningsCkb = ge.guiCheckBox(self.guiPg1TpLv,state=tk.DISABLED, text="Ignore Warnings", fontName=self.root.myFontFamily,  
                            fontSize=self.root.fsNORMAL, variable=self.bIgnoreWarningsCkbVar, onvalue=True, offvalue=False) # text_color='gray5',  text_color_disabled='gray70', 
        # Row 12 Cancel Button
        # ################################################################
        self.Pg1CancelButton = ge.guiButton(self.guiPg1TpLv, text="Cancel",  command=self.closeTopLv,  fontName=self.root.myFontFamily,  fontSize=self.root.fsLARGE) 
        # Row 13  chose what categories to adjust (checkboxes) and 'Go to page 2' button
        # ################################################################
        self.Pg1TuningParamLabel = ge.guiTxtLabel(self.guiPg1TpLv, text="LUMIA may adjust:", width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        self.Pg1LandVegCkb = ge.guiCheckBox(self.guiPg1TpLv, text="Land/Vegetation", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, 
                             variable=self.LandVegCkbVar, onvalue=True, offvalue=False)
        self.Pg1FossilCkb = ge.guiCheckBox(self.guiPg1TpLv, text="Fossil (off)", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                            variable=self.FossilCkbVar, onvalue=True, offvalue=False)                             
        self.Pg1OceanCkb = ge.guiCheckBox(self.guiPg1TpLv, text="Ocean (off)", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                            variable=self.OceanCkbVar, onvalue=True, offvalue=False)                            
        self.Pg1GoButton = ge.guiButton(self.guiPg1TpLv, text="Go to page 2", command=self.EvHdPg1GotoPage2)
            
 
        # ====================================================================
        # Placement of all widgets of first GUI page  -- part of lumiaGuiApp (toplevel window)
        # ====================================================================
        # 
        # ################################################################
        if(USE_TKINTER):
            # Row 0:  Title Label
            self.Pg1TitleLabel.grid(row=0, column=0, columnspan=8,padx=xPadding, pady=yPadding, sticky="ew")
            # Row 12 : Cancel Button
            self.Pg1CancelButton.grid(row=12, column=4, columnspan=1, padx=xPadding, pady=yPadding, sticky="ew")
            # Row 13 : Go to page 2 Button
            self.Pg1GoButton.grid(row=13, column=4, columnspan=1, padx=xPadding, pady=yPadding, sticky="ew")
            
        else:
            display(self.Pg1TitleLabel)
            display(self.Pg1CancelButton)
            display(self.Pg1GoButton)
        
        # set the size of the gui window before showing it
        if(USE_TKINTER):
            #self.root.xoffset=int(0.5*1920)
            #self.root.update_idletasks()
            self.root.geometry(f"{appWidth}x{appHeight}+{self.root.xoffset}+0")   
            self.guiPg1TpLv.geometry(f"{appWidth}x{appHeight}")
        else:
            toolbar_widget = widgets.VBox()
            toolbar_widget.children = [
                self.guiPg1TpLv.Pg1LatitudesLabel, 
                self.guiPg1TpLv.Pg1CancelButton
            ]

    # ====================================================================
    #  general helper functions of the first GUI page -- part of lumiaGuiApp (toplevel window) 
    # ====================================================================
    # 
    # checkGuiValues gathers all the selected options and text from the available entry
    # fields and boxes and then generates a prompt using them
    def checkGuiValues(self):
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
            self.ymlContents['run']['time']['start'] = tStart.strftime('%Y-%m-%d %H:%M:%S')
            self.ymlContents['run']['time']['end'] = tEnd.strftime('%Y-%m-%d %H:%M:%S')
            
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
            self.ymlContents['run']['region']['lat0'] = Lat0
            self.ymlContents['run']['region']['lat1'] = Lat1
            self.ymlContents['run']['region']['lon0'] = Lon0
            self.ymlContents['run']['region']['lon1'] = Lon1
            dLat=self.ymlContents['run']['region']['dlat']   # 0.25
            dLon=self.ymlContents['run']['region']['dlon']  # 0.25
           # run:  grid: ${Grid:{lon0:-15, lat0:33, lon1:35, lat1:73, dlon:0.25, dlat:0.25}}
            self.ymlContents['run']['grid'] = '${Grid:{lon0:%.3f,lat0:%.3f,lon1:%.3f,lat1:%.3f,dlon:%.5f, dlat:%.5f}}' % (Lon0, Lat0,Lon1, Lat1, dLon, dLat)
            # dollar='$'
            # lBrac='{'
            # rBrac='}'
            # griddy2 = str('%s%sGrid:%slon0:%.3f,lat0:%.3f,lon1:%.3f,lat1:%.3f,dlon:%.5f, dlat:%.5f%s%s' % (dollar, lBrac,lBrac, Lon0, Lat0,Lon1, Lat1, dLon, dLat, rBrac, rBrac))
            # logger.debug(f'griddy2={griddy2}')
            # self.ymlContents['run']['griddy2']=griddy2
            # hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'run',  'griddy2'],   value=griddy2, bNewValue=True)

        # ObservationsFileLocation
        if (self.iObservationsFileLocation.get()==2):
            self.ymlContents['observations'][self.tracer]['file']['location'] = 'CARBONPORTAL'
        else:
            self.ymlContents['observations'][self.tracer]['file']['location'] = 'LOCAL'
            # TODO: get the file name
            
        # Emissions data (a prioris)
        # Land/Vegetation Net Exchange combo box
        sLandVegModel=self.LandNetExchangeOptionMenu.get()  # 'VPRM', 'LPJ-GUESS'
        self.ymlContents['emissions'][self.tracer]['categories']['biosphere']['origin']=sLandVegModel
        # Fossil emissions combo box
        sFossilEmisDataset = self.FossilEmisOptionMenu.get()  # "EDGARv4_LATEST")
        self.ymlContents['emissions'][self.tracer]['categories']['fossil']['origin']=sFossilEmisDataset
        # Ocean Net Exchange combo box
        sOceanNetExchangeDataset = self.OceanNetExchangeCkbVar.get()  # "mikaloff01"
        self.ymlContents['emissions'][self.tracer]['categories']['ocean']['origin']=sOceanNetExchangeDataset

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
            self.ymlContents['optimize']['emissions'][self.tracer]['biosphere']['adjust'] = self.LandVegCkb.get()
            self.ymlContents['optimize']['emissions'][self.tracer]['fossil']['adjust'] = self.FossilCkb.get()
            self.ymlContents['optimize']['emissions'][self.tracer]['ocean']['adjust'] = self.OceanCkb.get()                
            
        # Deal with any errors or warnings
        return(bErrors, sErrorMsg, bWarnings, sWarningsMsg)

            
            
    # ====================================================================
    # body & brain of second GUI page  -- part of lumiaGuiApp (root window)
    # ====================================================================
        
    def runPage2(self, iVerbosityLv='INFO'):  # of lumiaGuiApp
        # Plan the layout of the GUI - get screen dimensions, choose a reasonable font size for it, xPadding, etc.
        nCols=12 # sum of labels and entry fields per row
        nRows=32 #5+len(newDf) # number of rows in the GUI - not so important - window is scrollable
        stakeOutSpacesAndFonts(self.root, nCols, nRows)

        # ====================================================================
        # EventHandler of all widgets of second GUI page  -- part of lumiaGuiApp (root window)
        # ====================================================================

        # ====================================================================
        # Creation of all widgets of second GUI page  -- part of lumiaGuiApp (root window)
        # ====================================================================
        # Define all widgets needed. 
        # Row 0:  Title Label
        # ################################################################
        title="LUMIA  --  Refine your selections among the data discovered"
        self.Pg2TitleLabel = ge.guiTxtLabel(self.root, title, fontName=self.root.myFontFamily, fontSize=self.root.fsGIGANTIC, style="bold")
        # Row 7?:  Title Label
        # ################################################################
        self.button2 = tk.Button(self.root, text="Exit", command=self.EvHdPg1ExitWithSuccess) # Note: expressions after command= cannot have parameters or they will be executed at initialisation which is unwanted
        self.button2.pack()

        # ====================================================================
        # Placement of all widgets of the second GUI page  -- part of lumiaGuiApp (root window)
        # ====================================================================

   
   

# LumiaGui Class =============================================================
class LumiaTkGui(ctk.CTk):
    def __init__(self): #, root, *args, **kwargs):
        ctk.set_appearance_mode("System")  
        ctk.set_default_color_theme(scriptDirectory+"/doc/lumia-dark-theme.json") 
        ctk.CTk.__init__(self)
        # self.geometry(f"{maxW+1}x{maxH+1}")   is set later when we know about the screen dimensions
        self.title('LUMIA - the Lund University Modular Inversion Algorithm') 
        self.grid_rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.activeTextColor='gray10'
        self.inactiveTextColor='gray50'

class LumiaTkFrame(tk.Frame):
    def __init__(self, parent, *args, **kwargs):
        tk.Frame.__init__(self, parent, *args, **kwargs)
        self.parent = parent

# =======================================================================
def LumiaGuiPage1(root,  sLogCfgPath, ymlContents, ymlFile, bRefine=False, iVerbosityLv=1): 
    # =====================================================================
    
    def CancelAndQuit(): 
        CloseTheGUI(bAskUser=False,  bWriteStop=True)

    def show(guiPage1):
        if(USE_TKINTER):
            try:
                guiPage1.wm_protocol("WM_DELETE_WINDOW", CancelAndQuit)
            except:
                pass
            #guiPage1.wait_window(guiPage1)
        return 

    if(USE_TKINTER):
        #guiPage1=LumiaTkFrame(root) # .pack(side="top", fill="both", expand=True)
        #guiPage1=LumiaTkGui() # .pack(side="top", fill="both", expand=True)
        # guiPage1= tk.Frame(root, bg="cadet blue")
        #guiPage1= tk.Toplevel(root)
        #guiPage1.parent=root
        guiPage1 = root
    else:
        notify_output = widgets.Output()
        display(notify_output)
    
        
        
    xPadding=guiPage1.xPadding
    yPadding=guiPage1.yPadding

    # Dimensions of the window
    appWidth, appHeight = guiPage1.appWidth, guiPage1.appHeight

    #guiPage1.lonMin = ge.guiDoubleVar(value=-25.0)
    #guiPage1.lonMax = ge.guiDoubleVar(value=45.0)
    #guiPage1.latMin = ge.guiDoubleVar(value=23.0)
    #guiPage1.latMax = ge.guiDoubleVar(value=83.0)
    # Define all widgets needed. 
    # Row 0:  Title Label
    # ################################################################
    if(bRefine):
        title="LUMIA  --  Refine your selections among the data discovered"
    else:
        title="LUMIA  --  Configure your next LUMIA run"
    guiPage1.TitleLabel = ge.guiTxtLabel(guiPage1, title, fontName=guiPage1.myFontFamily, fontSize=guiPage1.fsGIGANTIC, style="bold")
    # Row 12 Cancel Button
    # ################################################################
    guiPage1.CancelButton = ge.guiButton(guiPage1, text="Cancel",  command=CancelAndQuit,  fontName="Georgia",  fontSize=guiPage1.fsLARGE) 
        
    if(not USE_TKINTER):
        #btn_clickme = widgets.Button(description='Cancel')
        guiPage1.CancelButton.on_click(CancelAndQuit)  # clickme)
        

    # Now place all the widgets on the frame or canvas
    # ################################################################
    if(USE_TKINTER):
        # Row 0:  Title Label
        guiPage1.TitleLabel .grid(row=0, column=0, columnspan=8,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 12 : Cancel Button
        guiPage1.CancelButton.grid(row=12, column=4, columnspan=1, padx=xPadding, pady=yPadding, sticky="ew")
    else:
        display(guiPage1.TitleLabel )
        display(guiPage1.CancelButton)
        #guiPage1.CancelButton.on_click(CancelAndQuit)
        #guiPage1.CancelButton
        # Row 12 : Cancel Button


    # sOutputPrfx=ymlContents[ 'run']['thisRun']['uniqueOutputPrefix']
    # sTmpPrfx=ymlContents[ 'run']['thisRun']['uniqueTmpPrefix'] 
    if(USE_TKINTER):
        guiPage1.geometry(f"{appWidth}x{appHeight}")   
        show(guiPage1)
    else:
        toolbar_widget = widgets.VBox()
        toolbar_widget.children = [
            guiPage1.LatitudesLabel, 
            guiPage1.CancelButton
        ]
        toolbar_widget   
        display(toolbar_widget) 
    while LOOP_ACTIVE:
        #guiPage1.mainloop()
        guiPage1.update()
    # if we got here, then this subroutine was successful - at least that is the idea...
    bSuccess=True
    return(bSuccess, ymlContents)
    
    
        

def displayGeometry(maxAspectRatio=1.778):  
    '''
    Query the current monitor or viewport about its dimensions in pixels.
    @maxAspectRatio 1920/1080.0=1.778 is here to prevant silly things if one uses multiple screens....
    @type float
    @return a tuple (screenWidth, screenHeight, xPadding, yPadding)
    @type integers
    '''
    # Get the screen resolution
    # m may return a string like
    # Monitor(x=0, y=0, width=1920, height=1080, width_mm=1210, height_mm=680, name='HDMI-1', is_primary=True)
    xoffset=0
    try:
        monitors=get_monitors()  # TODO: relies on  libxrandr2  which may not be available on the current system
        screenWidth=0
        for screen in monitors:
            try:
                useThisOne=screen.is_primary
            except:
                useThisOne=True # 'isprimary' entry may be absent on simple systems, then it is the only one
            if(useThisOne):
                try:
                    screenWidth = screen.width
                    screenHeight =screen.height
                except:
                    pass
        if (screenWidth < MIN_SCREEN_RES):
            screenWidth=MIN_SCREEN_RES
            screenHeight=MIN_SCREEN_RES
            xoffset=1
            
    except:
        logger.error('screeninfo.get_monitors() failed. Try installing the libxrandr2 library if missing. Setting display to 1080x960pxl')
        screenWidth= int(1080)
        screenHeight= int(960)
    # on Linux you can also run from commandline, but it may not be ideal if you encounter systems with multiple screens attached.
    # xrandr | grep '*'
    #    1920x1080     60.00*+  50.00    59.94    30.00    25.00    24.00    29.97    23.98  
    # Use the screen resolution to scale the GUI in the smartest way possible...
    # screenWidth = rootFrame.winfo_screenwidth()
    # screenHeight = rootFrame.winfo_screenheight()
    logger.debug(f'screeninfo.get_monitors.screenwidth()={screenWidth},  screenheight()={screenHeight} (primary screen)')
    if((screenWidth/screenHeight) > (1920/1080.0)):  # multiple screens?
        screenWidth=int(screenHeight*(1920/1080.0))
    logger.debug(f'guiPage1.winfo_screenwidth()={screenWidth},   guiPage1.winfo_screenheight()={screenHeight},  multiple screen correction')
    maxW = int(0.92*screenWidth)
    maxH = int(0.92*screenHeight)
    if(maxW > 1.2*maxH):
        maxW = int((1.2*maxH)+0.5)
        logger.debug(f'maxW={maxW},  maxH={maxH},  max aspect ratio fix.')
    if(xoffset==0):
        xoffset=0.5*(screenWidth-maxW) # helps us to place the gui horizontally in the center of the screen
    # Sets the dimensions of the window to something reasonable with respect to the user's screen properties
    xPadding=int(0.008*maxW)
    yPadding=int(0.008*maxH)
    return(maxW, maxH, xPadding, yPadding, xoffset)




def  readMyYamlFile(ymlFile):
    '''
    Function readMyYamlFile

    @param ymlFile : the LUMIA YAML configuration file in yaml (or rc) data format (formatted text)
    @type string (file name)
    @return contents of the ymlFile
    @type yamlObject
    '''
    ymlContents=None
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
    return(ymlContents)


# def main():    
p = argparse.ArgumentParser()
p.add_argument('--start', dest='start', default=None, help="Start of the simulation in date+time ISO notation as in \'2018-08-31 00:18:00\'. Overwrites the value in the rc-file")
p.add_argument('--end', dest='end', default=None, help="End of the simulation as in \'2018-12-31 23:59:59\'. Overwrites the value in the rc-file")
p.add_argument('--rcf', dest='rcf', default=None, help="Same as the --ymf option. Deprecated. For backward compatibility only.")   
p.add_argument('--ymf', dest='ymf', default=None,  help='yaml configuration file where the user plans his or her Lumia run: parameters, input files etc.')   
p.add_argument('--verbosity', '-v', dest='verbosity', default='INFO')
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

# Do the housekeeping like documenting the current git commit version of this code, date, time, user, platform etc.
thisScript='LumiaGUI'
ymlFile=hk.documentThisRun(ymlFile, thisScript,  args)  # from housekeepimg.py
# Now the config.yml file has all the details for this particular run

# no need to pass args.start or args.end because hk.documentThisRun() already took care of these.
scriptDirectory = os.path.dirname(os.path.abspath(sys.argv[0]))
# Call the main method
prepareCallToLumiaGUI(ymlFile,  scriptDirectory, args.verbosity)


