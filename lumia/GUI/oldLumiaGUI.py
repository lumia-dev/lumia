#!/usr/bin/env python3

# for JupyterLab notebooks it may be necessary to install these packages the first it is run:
# !pip install loguru
# !pip install git -- ERROR: Could not find a version that satisfies the requirement git
# !pip install tkinter -- ERROR: Could not find a version that satisfies the requirement git
# !pip install customtkinter
# !pip install icoscp_core  -- installed with conflicts and warnings: pangaeapy 1.0.6 requires requests~=2.26.0, but you have requests 2.31.0 which is incompatible
# !pip install ipython
# !pip install Pillow
# !pip install screeninfo

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

ISNOTEBOOK=False
scriptName=sys.argv[0]
if('.ipynb' in scriptName[-6:]):
    ISNOTEBOOK=True
if(ISNOTEBOOK):
    import guiElementsIpWdg.py as ge
else:
    import guiElementsTk as ge
    import tkinter as tk    
    import customtkinter as ctk

import boringStuff as bs
from queryCarbonPortal import discoverObservationsOnCarbonPortal
import warnings

warnings.simplefilter("ignore", FutureWarning)

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
class LumiaGui(ctk.CTkToplevel):  #ctk.CTk):
    # =====================================================================
    # The layout of the window will be written
    # in the init function itself
    def __init__(self, root,  sLogCfgPath, ymlContents, ymlFile):  # *args, **kwargs):
        ctk.CTkToplevel.__init__(self, root)
        self.parent = root
        # Get the screen resolution to scale the GUI in the smartest way possible...
        if(os.path.isfile("LumiaGui.stop")):
            sCmd="rm LumiaGui.stop"
            hk.runSysCmd(sCmd,  ignoreError=True)
        if(os.path.isfile("LumiaGui.go")):
            sCmd="rm LumiaGui.go"
            hk.runSysCmd(sCmd,  ignoreError=True)

    def run(self, root,  sLogCfgPath, ymlContents, ymlFile,tracer, pdTimeStart, pdTimeEnd, timeStep, sDataType=None,  iVerbosityLv=1): 
        dobjLst=[]
        selectedDobjLst=[]
        dfObsDataInfo=None
        # self.deiconify()
        screenWidth = self.winfo_screenwidth()
        screenHeight = self.winfo_screenheight()
        logger.debug(f'self.winfo_screenwidth()={screenWidth},   self.winfo_screenheight()={screenHeight}')
        if((screenWidth/screenHeight) > (1920/1080.0)):  # multiple screens?
            screenWidth=int(screenHeight*(1920/1080.0))
            logger.debug(f'self.winfo_screenwidth()={screenWidth},   self.winfo_screenheight()={screenHeight},  multiple screen correction')
        maxW = int(0.92*screenWidth)
        # maxW = 1650 # TODO: remove
        maxH = int(0.92*screenHeight)
        logger.debug(f'maxW={maxW},  maxH={maxH}')
        if(maxW > 1.2*maxH):
            maxW = int((1.2*maxH)+0.5)
            logger.debug(f'maxW={maxW},  maxH={maxH},  max aspect ratio fix.')
        nCols=5 # sum of labels and entry fields per row
        nRows=13 # number of rows in the GUI
        xPadding=int(0.008*maxW)
        wSpacer=int(2*0.008*maxW)
        yPadding=int(0.008*maxH)
        vSpacer=int(2*0.008*maxH)
        myFontFamily="Georgia"
        sLongestTxt="Start date (00:00h):"  # "Latitude (≥33°N):"
        (bFontFound, fsTINY,  fsSMALL,  fsNORMAL,  fsLARGE,  fsHUGE,  fsGIGANTIC, myFontHeight,  bWeMustStackColumns, bSuccess)= \
            bs.calculateEstheticFontSizes(myFontFamily,  maxW,  maxH, sLongestTxt, nCols, nRows, xPad=xPadding, 
                                                        yPad=yPadding, maxFontSize=20, USE_TKINTER=True,  bWeCanStackColumns=False)
        hDeadSpace=wSpacer+(nCols*xPadding*2)+wSpacer
        vDeadSpace=2*yPadding #vSpacer+(nRows*yPadding*2)+vSpacer
        colWidth=int((maxW - hDeadSpace)/(nCols*1.0))
        rowHeight=int((maxH - vDeadSpace)/(nRows*1.0))
        # Dimensions of the window
        appWidth, appHeight = maxW, maxH
        #activeTextColor='gray10'
        #inactiveTextColor='gray50'
        self.lonMin = ge.guiDoubleVar(value=-25.0)
        self.lonMax = ge.guiDoubleVar(value=45.0)
        self.latMin = ge.guiDoubleVar(value=23.0)
        self.latMax = ge.guiDoubleVar(value=83.0)

        # Actions
        # ################################################################
        # 
        def CloseTheGUI(bAskUser=True,  bWriteStop=True):
            if(bAskUser):  # only happens if the GUI window was closed brutally
                if ge.guiAskOkCancel(title="Quit?",  message="Is it OK to abort your Lumia run?"):  # tk.messagebox.askokcancel("Quit", "Is it OK to abort your Lumia run?"):
                    if(bWriteStop):
                        #self.iconify()
                        logger.info("LumiaGUI was canceled.")
                        sCmd="touch LumiaGui.stop"
                        hk.runSysCmd(sCmd)
            else:  # the user clicked Cancel or Go
                if(bWriteStop): # the user selected Cancel - else the LumiaGui.go message has already been written
                    #self.iconify()
                    logger.info("LumiaGUI was canceled.")
                    sCmd="touch LumiaGui.stop"
                    hk.runSysCmd(sCmd)
                global LOOP_ACTIVE
                LOOP_ACTIVE = False
                # self.iconify()
                logger.info("Closing the GUI...")
                try:
                    self.after(100, self.event_generate("<Destroy>"))
                except:
                    pass
            self.protocol("WM_DELETE_WINDOW", CloseTheGUI)
        
        def CancelAndQuit(): 
            #logger.info("LumiaGUI was canceled.")
            #sCmd="touch LumiaGui.stop"
            #hk.runSysCmd(sCmd)
            CloseTheGUI(bAskUser=False,  bWriteStop=True)
            #global LOOP_ACTIVE
            #LOOP_ACTIVE = False


        def GoButtonHit():
            # def generateResults(self):
            bGo=False
            (bErrors, sErrorMsg, bWarnings, sWarningsMsg) = self.checkGuiValues(ymlContents=ymlContents, tracer=tracer)
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
                try:
                    with open(ymlFile, 'w') as outFile:
                        yaml.dump(ymlContents, outFile)
                except:
                    sTxt=f"Fatal Error: Failed to write to text file {ymlFile} in local run directory. Please check your write permissions and possibly disk space etc."
                    logger.error(sTxt)
                    CancelAndQuit()

                sCmd="touch LumiaGui.go"
                hk.runSysCmd(sCmd)
                logger.info("Done. LumiaGui part-1 completed successfully. Config file updated.")
                # self.bPleaseCloseTheGui.set(True)
                CloseTheGUI(bAskUser=False,  bWriteStop=False)
                #global LOOP_ACTIVE
                #LOOP_ACTIVE = False

        def setTracer():
            # "TracerRadioButton, iTracerRbVal"
            if (self.iTracerRbVal.get()==1):
               ymlContents['run']['tracers'] = 'co2'
            else:
                ymlContents['run']['tracers'] = 'ch4'
            # applyRules()
            
        def setObsFileLocation():
            # "TracerRadioButton, iTracerRbVal"
            if (self.iObservationsFileLocation.get()==1):
               ymlContents['observations'][tracer]['file']['location'] = 'LOCAL'
            else:
                ymlContents['observations'][tracer]['file']['location'] = 'CARBONPORTAL'

        # GUI Layout
        # ################################################################
        #
        
        # Sets the title of the window to "LumiaGui"
        self.title("LUMIA run configuration")  
        # Sets the dimensions of the window to 800x900
        self.geometry(f"{appWidth}x{appHeight}")   

        # Row 0:  Title Label
        # ################################################################

        self.LatitudesLabel = ge.guiTxtLabel(self, "LUMIA  --  Configure your next LUMIA run",  
                                                                    fontName="Arial MT Extra Bold", fontSize=fsGIGANTIC)
        self.LatitudesLabel.grid(row=0, column=0,
                            columnspan=8,padx=xPadding, pady=yPadding,
                            sticky="ew")

        # Row 1:  Time interval
        # ################################################################

        self.LatitudesLabel = ge.guiTxtLabel(self, text="Time interval",  fontName="Georgia",  fontSize=fsLARGE)
        self.LatitudesLabel.grid(row=1, column=0,  
                            columnspan=1, padx=xPadding, pady=yPadding,
                            sticky="ew")
 
        # Row 2: Time interval
        # ################################################################

        self.TimeStartLabel = ge.guiTxtLabel(self, anchor="w",
                                text="Start date (00:00h):", width=colWidth,  fontName="Georgia",  fontSize=fsNORMAL)
        self.TimeStartLabel.grid(row=2, column=0, 
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")

        self.TimeEndLabel = ge.guiTxtLabel(self,
                                text="End date: (23:59h)", width=colWidth,  fontName="Georgia",  fontSize=fsNORMAL)
        self.TimeEndLabel.grid(row=2, column=2,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")
        # Time Entry Fields
        # Get the current values from the yamlFile or previous user entry:
        
        startDate=str(ymlContents['run']['time']['start'])[:10]  # e.g. '2018-01-01 00:00:00'
        endDate=str(ymlContents['run']['time']['end'])[:10]   # e.g. '2018-02-01 23:59:59'
        self.sStartDate=ge.guiStringVar(value=startDate)
        self.sEndDate=ge.guiStringVar(value=endDate)

        #s=self.sStartDate.get()
        # prep the StartDate field  
        self.TimeStartEntry = ge.guiDataEntry(self,textvariable=self.sStartDate,  
                          placeholder_text=str(ymlContents['run']['time']['start'])[:10], width=colWidth)
        #self.TimeStartEntry = ctk.CTkEntry(self,textvariable=self.sStartDate,  
        #                  placeholder_text=str(ymlContents['run']['time']['start'])[:10], width=colWidth)
        self.TimeStartEntry.grid(row=2, column=1,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="ew")
                            
        # prep the EndDate field
        self.TimeEndEntry = ge.guiDataEntry(self,textvariable=self.sEndDate, 
                          placeholder_text=str(ymlContents['run']['time']['end'])[:10], width=colWidth)
        self.TimeEndEntry.grid(row=2, column=3,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="ew")
                            
        # Row 3:  Geograph Region Title
        # ################################################################

        # Lat/Lon
        self.LatitudesLabel = ge.guiTxtLabel(self, anchor="w",
                                text="Geographical extent of the area modelled (in deg. North/East)",  fontName="Georgia",  fontSize=fsLARGE)
        self.LatitudesLabel.grid(row=3, column=0,
                            columnspan=4,padx=xPadding, pady=yPadding,
                            sticky="ew")


        # ################################################################
        # 
        # Text Box
        self.displayBox = ge.guiTextBox(self, width=200,  height=100,  fontName="Georgia",  fontSize=fsSMALL, text_color="red") 
                                # ctk.CTkTextbox(self, width=200,
                                #        text_color="red", font=("Georgia",  fsSMALL),  height=100)
        self.displayBox.grid(row=3, column=4, columnspan=1,rowspan=6, 
                             padx=20, pady=20, sticky="nsew")
                             
        #self.displayBox.delete("0.0", "end")  # delete all text
        self.displayBox.configure(state=tk.DISABLED)  # configure textbox to be read-only

        # Row 4: 
        # ################################################################
        # Latitudes Label
        txt=f"Latitude (≥{self.latMin.get()}°N):" # "Latitude (≥33°N):"
        self.LatitudeMinLabel = ge.guiTxtLabel(self,anchor="w",
                                text=txt, width=colWidth,  fontName="Georgia",  fontSize=fsNORMAL)
        self.LatitudeMinLabel.grid(row=4, column=0,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")
        self.LatitudeMaxLabel = ge.guiTxtLabel(self,
                                text="max (≤73°N):", width=colWidth,  fontName="Georgia",  fontSize=fsNORMAL)
        self.LatitudeMaxLabel.grid(row=4, column=2,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")
 
        # Latitudes Entry Fields
        # min
        # gridString=ymlContents['run']['grid']  # '${Grid:{lon0:-15, lat0:33, lon1:35, lat1:73, dlon:0.25, dlat:0.25}}'

        Lat0=ymlContents['run']['region']['lat0']  # 33.0
        Lat1=ymlContents['run']['region']['lat1']   #73.0
        self.sLat0=ge.guiStringVar(value=f'{Lat0:.3f}')
        self.sLat1=ge.guiStringVar(value=f'{Lat1:.3f}')
        # grid: ${Grid:{lon0:-15, lat0:33, lon1:35, lat1:73, dlon:0.25, dlat:0.25}}
        # sRegion="lon0=%.3f, lon1=%.3f, lat0=%.3f, lat1=%.3f, dlon=%.3f, dlat=%.3f, nlon=%d, nlat=%d"%(regionGrid.lon0, regionGrid.lon1,  regionGrid.lat0,  regionGrid.lat1,  regionGrid.dlon,  regionGrid.dlat,  regionGrid.nlon,  regionGrid.nlat)

        self.Latitude0Entry = ge.guiDataEntry(self,textvariable=self.sLat0,
                          placeholder_text=self.sLat0, width=colWidth)
        self.Latitude0Entry.grid(row=4, column=1,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="ew")
        # max
        self.Latitude1Entry = ge.guiDataEntry(self,textvariable=self.sLat1,
                          placeholder_text=self.sLat1, width=colWidth)
        self.Latitude1Entry.grid(row=4, column=3,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="ew")
                 

        # Row 5: 
        # Longitudes Entry Fields
        # ################################################################
        Lon0=ymlContents['run']['region']['lon0']  # -15.0
        Lon1=ymlContents['run']['region']['lon1']   #35.0
        self.sLon0=ge.guiStringVar(value=f'{Lon0:.3f}')
        self.sLon1=ge.guiStringVar(value=f'{Lon1:.3f}')

        # row 5 Longitudes  my_string = f'{my_float:.3f}'
        self.LongitudeMinLabel = ge.guiTxtLabel(self, text="Longitude (≥-15°E):", anchor="w", fontName="Georgia",  fontSize=fsNORMAL, width=colWidth)
        self.LongitudeMinLabel.grid(row=5, column=0,
                           padx=xPadding, pady=yPadding,
                           sticky="ew")
        self.LongitudeMaxLabel = ge.guiTxtLabel(self, text="max (≤35°E):",  fontName="Georgia",  fontSize=fsNORMAL, width=colWidth)
        self.LongitudeMaxLabel.grid(row=5, column=2,
                           padx=xPadding, pady=yPadding,
                           sticky="ew")
        # Longitude0 Entry Field
        # min
        self.Longitude0Entry = ge.guiDataEntry(self, textvariable=self.sLon0, 
                            placeholder_text=self.sLon0, width=colWidth)
        self.Longitude0Entry.grid(row=5, column=1,
                           columnspan=1, padx=xPadding,
                           pady=yPadding, sticky="ew")
        # max
        self.Longitude1Entry = ge.guiDataEntry(self,textvariable=self.sLon1,
                            placeholder_text=self.sLon1, width=colWidth)
        self.Longitude1Entry.grid(row=5, column=3,
                           columnspan=1, padx=xPadding,
                           pady=yPadding, sticky="ew")
        

        # Row 6:
        #  Tracer
        # ################################################################
         # Get the currently selected tracer from the yml config file
        tracers = ymlContents['run']['tracers']
        if (isinstance(tracers, list)):
            tracer=tracers[0]
        else:
            tracer=ymlContents['run']['tracers']
        self.LatitudeMinLabel = ge.guiTxtLabel(self,anchor="w",
                                text="Tracer:", width=colWidth,  fontName="Georgia",  fontSize=fsNORMAL)
        self.LatitudeMinLabel.grid(row=6, column=0,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")

        # Set the radiobutton initial status in accordance with the (first) tracer extracted from the yml config file
        self.iTracerRbVal= ge.guiIntVar(value=1)
        if(('ch4' in tracer) or ('CH4' in tracer)):
            self.iTracerRbVal = ge.guiIntVar(value=2)
        else:
            self.iTracerRbVal = ge.guiIntVar(value=1)
        self.TracerRadioButton = ge.guiRadioButton(self,
                                   text="CO2", fontName="Georgia",  fontSize=fsNORMAL, 
                                   variable=self.iTracerRbVal,  value=1, command=setTracer)
        self.TracerRadioButton.grid(row=6, column=1, columnspan=1, 
                                  padx=xPadding, pady=yPadding,sticky="ew")

        self.TracerRadioButton = ge.guiRadioButton(self,
                                   text="CH4", fontName="Georgia",  fontSize=fsNORMAL,
                                   variable=self.iTracerRbVal,  value=2, command=setTracer)
        self.TracerRadioButton.grid(row=6, column=2, columnspan=1, 
                                  padx=xPadding, pady=yPadding,sticky="ew")
            
        # Row 7: 
        # ################################################################
        # Emissions data (a prioris)
        self.LatitudesLabel = ge.guiTxtLabel(self, anchor="w",
                                text="Emissions data (a priori)",  fontName="Georgia",  fontSize=fsLARGE)
        self.LatitudesLabel.grid(row=7, column=0,  
                            columnspan=2, padx=xPadding, pady=yPadding,
                            sticky="ew")

        # Emissions data (a prioris) : dyn.vegetation net exchange model
        self.observationsFilePathLabel = ge.guiTxtLabel(self,
                                text="Land/Vegetation NEE:", width=colWidth,  fontName="Georgia",  fontSize=fsNORMAL)
        self.observationsFilePathLabel.grid(row=7, column=2,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew") 

        # Land/Vegetation Net Exchange combo box
        self.LandNetExchangeModelCkbVar = ge.guiStringVar(value=ymlContents['emissions'][tracer]['categories']['biosphere']['origin'])
        #self.LandNetExchangeOptionMenu = ctk.CTkOptionMenu(self,
        #                                values=["LPJ-GUESS","VPRM"],  dropdown_font=("Georgia",  fsNORMAL), 
        #                                variable=self.LandNetExchangeModelCkbVar)
        self.LandNetExchangeOptionMenu = ge.guiOptionMenu(self,  values=["LPJ-GUESS","VPRM"], variable=self.LandNetExchangeModelCkbVar, 
                                                                                                        dropdown_fontName="Georgia", dropdown_fontSize=fsNORMAL)
        self.LandNetExchangeOptionMenu.grid(row=7, column=3,
                                        padx=xPadding, pady=yPadding,
                                        columnspan=1, sticky="ew")

        # Row 8: 
        # Emissions data (a prioris) continued: fossil+ocean
        # ################################################################

        # Fossil Emissions data (a prioris)
        self.observationsFilePathLabel = ge.guiTxtLabel(self,
                                text="Fossil emissions:", width=colWidth,  fontName="Georgia",  fontSize=fsNORMAL)
        self.observationsFilePathLabel.grid(row=8, column=0,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")
        # Fossil emissions combo box
        # Latest is presently https://meta.icos-cp.eu/collections/GP-qXikmV7VWgG4G2WxsM1v3
        self.FossilEmisCkbVar = ge.guiStringVar(value=ymlContents['emissions'][tracer]['categories']['fossil']['origin']) #"EDGARv4_LATEST"
        self.FossilEmisOptionMenu = ge.guiOptionMenu(self, 
                                        values=["EDGARv4_LATEST","EDGARv4.3_BP2021_CO2_EU2_2020","EDGARv4.3_BP2021_CO2_EU2_2019", "EDGARv4.3_BP2021_CO2_EU2_2018"], 
                                        variable=self.FossilEmisCkbVar, dropdown_fontName="Georgia", dropdown_fontSize=fsNORMAL)

        #self.FossilEmisOptionMenu = ctk.CTkOptionMenu(self, dropdown_fontName="Georgia",  fontSize=fsNORMAL, 
        #                                values=["EDGARv4_LATEST","EDGARv4.3_BP2021_CO2_EU2_2020","EDGARv4.3_BP2021_CO2_EU2_2019", "EDGARv4.3_BP2021_CO2_EU2_2018"], 
        #                                variable=self.FossilEmisCkbVar)
                #  https://hdl.handle.net/11676/Ce5IHvebT9YED1KkzfIlRwDi (2022) https://doi.org/10.18160/RFJD-QV8J EDGARv4.3 and BP statistics 2023
                # https://hdl.handle.net/11676/6i-nHIO0ynQARO3AIbnxa-83  EDGARv4.3_BP2021_CO2_EU2_2020.nc  
                # https://hdl.handle.net/11676/ZU0G9vak8AOz-GprC0uY-HPM  EDGARv4.3_BP2021_CO2_EU2_2019.nc
                # https://hdl.handle.net/11676/De0ogQ4l6hAsrgUwgjAoGDoy EDGARv4.3_BP2021_CO2_EU2_2018.nc
        self.FossilEmisOptionMenu.grid(row=8, column=1,
                                        padx=xPadding, pady=yPadding,
                                        columnspan=1, sticky="ew")
        

        # Ocean Emissions data (a prioris)
        self.observationsFilePathLabel = ge.guiTxtLabel(self,
                                text="Ocean net exchange:", width=colWidth,  fontName="Georgia",  fontSize=fsNORMAL)
        self.observationsFilePathLabel.grid(row=8, column=2,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")
        # Ocean Net Exchange combo box
        self.OceanNetExchangeCkbVar = ge.guiStringVar(value=ymlContents['emissions'][tracer]['categories']['ocean']['origin'])  # "mikaloff01"
        self.OceanNetExchangeOptionMenu = ge.guiOptionMenu(self,
                                        values=["mikaloff01"],
                                        variable=self.OceanNetExchangeCkbVar, dropdown_fontName="Georgia", dropdown_fontSize=fsNORMAL)

#        self.OceanNetExchangeOptionMenu = ctk.CTkOptionMenu(self,
#                                        values=["mikaloff01"], dropdown_fontName="Georgia",  fontSize=fsNORMAL, 
#                                        variable=self.OceanNetExchangeCkbVar)
        self.OceanNetExchangeOptionMenu.grid(row=8, column=3,
                                        padx=xPadding, pady=yPadding,
                                        columnspan=1, sticky="ew")
 

        # Row 9: 
        # Obs data labels
        # ################################################################
        # ObservationsFileLocation
        self.iObservationsFileLocation= ge.guiIntVar(value=1)
        if('CARBONPORTAL' in ymlContents['observations'][tracer]['file']['location']):
            self.iObservationsFileLocation = ge.guiIntVar(value=2)
        else:
            self.iObservationsFileLocation = ge.guiIntVar(value=1)
        self.ObsFileLocationCPortalRadioButton = ge.guiRadioButton(self,
                                   text="Ranking of Obsdata from CarbonPortal", fontName="Georgia",  fontSize=fsNORMAL, 
                                   variable=self.iObservationsFileLocation,  value=2, command=setObsFileLocation)
        self.ObsFileLocationCPortalRadioButton.grid(row=9, column=0,
                                  columnspan=2, padx=xPadding, pady=yPadding,
                                  sticky="ew")

        #iObservationsFileLocation.set(1) # Read observations from local file
        if ('CARBONPORTAL' in ymlContents['observations'][tracer]['file']['location']):
            self.iObservationsFileLocation.set(2)
        self.ObsFileLocationLocalRadioButton = ge.guiRadioButton(self,
                                   text="Observational CO2 data from local file", fontName="Georgia",  fontSize=fsNORMAL,
                                   variable=self.iObservationsFileLocation,  value=1, command=setObsFileLocation)
        self.ObsFileLocationLocalRadioButton.grid(row=9, column=2,
                                  columnspan=2, padx=xPadding, pady=yPadding,
                                  sticky="ew")


        # Row 10: 
        # Obs data entries
        # ################################################################

        # Ranking of data records from CarbonPortal
        rankingList=ymlContents['observations'][tracer]['file']['ranking']
        self.ObsFileRankingTbxVar = ge.guiStringVar(value="ObsPack")
        self.ObsFileRankingBox = ge.guiTextBox(self, width=colWidth,  height=(2*rowHeight+vSpacer),  fontName="Georgia",  fontSize=fsSMALL)
                                        # ctk.CTkTextbox(self,
                                        # width=colWidth,
                                        # height=(2*rowHeight+vSpacer))
        self.ObsFileRankingBox.grid(row=10, column=0,
                             columnspan=1, rowspan=3, padx=xPadding,
                             pady=yPadding, sticky="nsew")
        txt=""
        rank=4
        for sEntry in  rankingList:
            txt+=str(rank)+': '+sEntry+'\n'
            rank=rank-1
        self.ObsFileRankingBox.insert('0.0', txt)  # insert at line 0 character 0


        # Row 11: 
        # Obs data entries
        # ################################################################
        #  Entry LocalFile
        self.observationsFilePathLabel = ge.guiTxtLabel(self,
                                text="Local.file.with.obs.data:", width=colWidth,  fontName="Georgia",  fontSize=fsNORMAL)
        self.observationsFilePathLabel.grid(row=10, column=2,
                            columnspan=2,padx=xPadding, pady=yPadding,
                            sticky="ew")
        self.ObsFileLocationLocalEntry = ge.guiDataEntry(self,
                          placeholder_text= ymlContents['observations'][tracer]['file']['path'], width=colWidth)
        self.ObsFileLocationLocalEntry.grid(row=11, column=2,
                            columnspan=2, padx=xPadding,
                            pady=yPadding, sticky="ew")

        # Ignore ChkBx
        self.bIgnoreWarningsCkbVar = ge.guiBooleanVar(value=False) # tk.NORMAL
        self.ignoreWarningsCkb = ge.guiCheckBox(self,state=tk.DISABLED, 
                            text="Ignore Warnings", fontName="Georgia",  fontSize=fsNORMAL,
                            variable=self.bIgnoreWarningsCkbVar, text_color='gray5',  text_color_disabled='gray70', 
                            onvalue=True, offvalue=False)                            
        self.ignoreWarningsCkb.grid(row=11,  column=4, 
                          padx=xPadding, pady=yPadding,
                          sticky="nw")

        # Row 12
        # ################################################################
        # Cancel Button
        self.CancelButton = ge.guiButton(self, text="Cancel",  command=CancelAndQuit,  fontName="Georgia",  fontSize=fsLARGE) 
        self.CancelButton.grid(row=12, column=4,
                                        columnspan=1, padx=xPadding,
                                        pady=yPadding, sticky="ew")



        # Row 13
        # ################################################################
        # 
        self.TuningParamLabel = ge.guiTxtLabel(self,
                                text="LUMIA may adjust:", width=colWidth,  fontName="Georgia",  fontSize=fsNORMAL)
        self.TuningParamLabel.grid(row=13, column=0,
                            columnspan=1,padx=xPadding, pady=yPadding,
                            sticky="ew")
        #self.LandVegCkbVar = ge.guiStringVar(value="Vegetation")
        #self.LandVegCkb = ge.guiCheckBox(self,
        #                     text="Land/Vegetation",
        #                     variable=self.LandVegCkbVar,
        #                     onvalue="Vegetation", offvalue="c1")
        self.LandVegCkbVar = ge.guiBooleanVar(value=True)
        self.LandVegCkb = ge.guiCheckBox(self,
                             text="Land/Vegetation", fontName="Georgia",  fontSize=fsNORMAL, 
                             variable=self.LandVegCkbVar,
                             onvalue=True, offvalue=False)
        self.LandVegCkb.grid(row=13, column=1,
                          padx=xPadding, pady=yPadding,
                          sticky="ew")

        self.FossilCkbVar = ge.guiBooleanVar(value=False)
        self.FossilCkb = ge.guiCheckBox(self,
                            text="Fossil (off)", fontName="Georgia",  fontSize=fsNORMAL,
                            variable=self.FossilCkbVar,
                             onvalue=True, offvalue=False)                             
        self.FossilCkb.grid(row=13, column=2,
                          padx=xPadding, pady=yPadding,
                          sticky="ew")

        self.OceanCkbVar = ge.guiBooleanVar(value=False)
        self.OceanCkb = ge.guiCheckBox(self,
                            text="Ocean (off)", fontName="Georgia",  fontSize=fsNORMAL,
                            variable=self.OceanCkbVar,
                             onvalue=True, offvalue=False)                            
        self.OceanCkb.grid(row=13, column=3,
                          padx=xPadding, pady=yPadding,
                          sticky="ew")

        self.RunButton = ctk.CTkButton(self, font=("Georgia", fsNORMAL), 
                                         text="RUN",
                                         command=GoButtonHit)
        self.RunButton.grid(row=13, column=4,
                                        columnspan=1, padx=xPadding,
                                        pady=yPadding, sticky="ew")

                
                                        
        def loop_function():
            global LOOP_ACTIVE
            LOOP_ACTIVE = True
            while LOOP_ACTIVE:
                time.sleep(3)
            logger.info("Closing the GUI...")
            try:
                self.after(100, self.event_generate("<Destroy>"))
            except:
                pass
                #logger.error('Failed to destroy the first page of the GUI for whatever reason...')


        _thread.start_new_thread(loop_function, ())
        #root.mainloop()     
        return(dobjLst, selectedDobjLst, dfObsDataInfo)
        
    def show(self):
        # self.deiconify()
        self.wm_protocol("WM_DELETE_WINDOW", self.destroy)
        self.wait_window(self)
        return 

    def AskUserAboutWarnings(self):
        GoBackAndFixIt=True
        return(GoBackAndFixIt)
    
 
    # This function is used to get the selected
    # options and text from the available entry
    # fields and boxes and then generates
    # a prompt using them
    def checkGuiValues(self, ymlContents, tracer):
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
            ymlContents['run']['time']['start'] = tStart.strftime('%Y-%m-%d %H:%M:%S')
            ymlContents['run']['time']['end'] = tEnd.strftime('%Y-%m-%d %H:%M:%S')
            
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
            dLat=ymlContents['run']['region']['dlat']   # 0.25
            dLon=ymlContents['run']['region']['dlon']  # 0.25
           # run:  grid: ${Grid:{lon0:-15, lat0:33, lon1:35, lat1:73, dlon:0.25, dlat:0.25}}
            ymlContents['run']['grid'] = '${Grid:{lon0:%.3f,lat0:%.3f,lon1:%.3f,lat1:%.3f,dlon:%.5f, dlat:%.5f}}' % (Lon0, Lat0,Lon1, Lat1, dLon, dLat)
            # dollar='$'
            # lBrac='{'
            # rBrac='}'
            # griddy2 = str('%s%sGrid:%slon0:%.3f,lat0:%.3f,lon1:%.3f,lat1:%.3f,dlon:%.5f, dlat:%.5f%s%s' % (dollar, lBrac,lBrac, Lon0, Lat0,Lon1, Lat1, dLon, dLat, rBrac, rBrac))
            # logger.debug(f'griddy2={griddy2}')
            # ymlContents['run']['griddy2']=griddy2
            # hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'griddy2'],   value=griddy2, bNewValue=True)

        # ObservationsFileLocation
        if (self.iObservationsFileLocation.get()==2):
            ymlContents['observations'][tracer]['file']['location'] = 'CARBONPORTAL'
        else:
            ymlContents['observations'][tracer]['file']['location'] = 'LOCAL'
            # TODO: get the file name
            
        # Emissions data (a prioris)
        # Land/Vegetation Net Exchange combo box
        sLandVegModel=self.LandNetExchangeOptionMenu.get()  # 'VPRM', 'LPJ-GUESS'
        ymlContents['emissions'][tracer]['categories']['biosphere']['origin']=sLandVegModel
        # Fossil emissions combo box
        sFossilEmisDataset = self.FossilEmisOptionMenu.get()  # "EDGARv4_LATEST")
        ymlContents['emissions'][tracer]['categories']['fossil']['origin']=sFossilEmisDataset
        # Ocean Net Exchange combo box
        sOceanNetExchangeDataset = self.OceanNetExchangeCkbVar.get()  # "mikaloff01"
        ymlContents['emissions'][tracer]['categories']['ocean']['origin']=sOceanNetExchangeDataset

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
            ymlContents['optimize']['emissions'][tracer]['biosphere']['adjust'] = self.LandVegCkb.get()
            ymlContents['optimize']['emissions'][tracer]['fossil']['adjust'] = self.FossilCkb.get()
            ymlContents['optimize']['emissions'][tracer]['ocean']['adjust'] = self.OceanCkb.get()                
            
        # Deal with any errors or warnings
        return(bErrors, sErrorMsg, bWarnings, sWarningsMsg)
        




# LumiaGui Class =============================================================
class RefineObsSelectionGUI(ctk.CTk):
    # =====================================================================
    # The layout of the window is now written
    # in the init function itself
    def __init__(self, root,  sLogCfgPath, ymlContents, ymlFile, widgetsLst):  # *args, **kwargs):
        # Get the screen resolution to scale the GUI in the smartest way possible...
        ctk.set_appearance_mode("System")  
        ctk.set_default_color_theme(scriptDirectory+"/doc/lumia-dark-theme.json") 
        screenWidth = root.winfo_screenwidth()
        screenHeight = root.winfo_screenheight()
        # print(f'screenWidth={screenWidth},  screenHeight={screenHeight}', flush=True)
        if(screenWidth<100):
            print('Oooops')
        if((screenWidth/screenHeight) > (1920/1080.0)):  # multiple screens?
            screenWidth=int(screenHeight*(1920/1080.0))
        self.maxW = int(0.92*screenWidth)
        self.maxH = int(0.92*screenHeight)
        # Dimensions of the window
        self.appWidth, self.appHeight = self.maxW, self.maxH
        root.title("LUMIA: Refine the selection of observations to be used")  
        # Sets the dimensions of the window to something reasonable with respect to the user's screren properties
        root.geometry(f"{self.appWidth}x{self.appHeight}")   
        root.grid_rowconfigure(0, weight=1)
        root.columnconfigure(0, weight=1)


    def run(self, root,  sLogCfgPath, ymlContents, ymlFile,  widgetsLst, pdTimeStart,  pdTimeEnd, timeStep, tracer):
        nWidgetsPerRow=5
        if(os.path.isfile("LumiaGui.stop")):
            sCmd="rm LumiaGui.stop"
            hk.runSysCmd(sCmd,  ignoreError=True)
        if(os.path.isfile("LumiaGui.go")):
            sCmd="rm LumiaGui.go"
            hk.runSysCmd(sCmd,  ignoreError=True)

        # run through the first page of the GUI so we select the right time interval, region, emissions files etc
        guiPage1=LumiaGui(root,  sLogCfgPath, ymlContents, ymlFile) 
        # query the CarbonPortal for suitable data sets that match the requests defined in the config.yml file and the latest GUI choices
        #root.withdraw()
        guiPage1.attributes('-topmost', 'true')
        (dobjLst, selectedDobjLst, dfObsDataInfo)=guiPage1.run(root, sLogCfgPath,  
                    ymlContents, ymlFile,tracer, pdTimeStart, pdTimeEnd, timeStep, sDataType=None,  iVerbosityLv=1) 
        # root.wait_window(guiPage1)
        guiPage1.show()
        #guiPage1.iconify()
        #cpDir=ymlContents['observations'][tracer]['file']['cpDir']
        # sOutputPrfx=ymlContents[ 'run']['thisRun']['uniqueOutputPrefix']
        sTmpPrfx=ymlContents[ 'run']['thisRun']['uniqueTmpPrefix'] 
        #bThreading=True
        #if(bThreading):  # Not worth it....
        #    thread1 = threading.Thread(target=discoverObservationsOnCarbonPortal, args=[tracer,   
        #                        pdTimeStart, pdTimeEnd, timeStep,  ymlContents])
        #    thread1.start()
        #else:
        if(os.path.isfile("LumiaGui.stop")):
            sys.exit(-1)
            #global LOOP2_ACTIVE
            #LOOP2_ACTIVE = False
            #return
            
        # discoverObservationsOnCarbonPortal()
        # TODO: uncomment the next line to stop using a canned list DiscoveredObservations-short.csv for testing (saves a lot fo time in the debugger)
        (dobjLst, selectedDobjLst, dfObsDataInfo, fDiscoveredObservations, badPidsLst)=discoverObservationsOnCarbonPortal(tracer,   
                            pdTimeStart, pdTimeEnd, timeStep,  ymlContents,  sDataType=None, printProgress=True,    iVerbosityLv=1)
        #fDiscoveredObservations='DiscoveredObservations.csv' # 'DiscoveredObservations-short.csv'
        #badPidsLst=[]
        

        if (len(badPidsLst) > 0):
            sFOut=ymlContents['observations'][tracer]['file']['selectedPIDs']
            sFOut=sFOut[:-21]+'bad-PIDs.csv'
            with open( sFOut, 'w') as fp:
                for item in badPidsLst:
                    fp.write("%s\n" % item)
        #self.deiconify()
        nCols=12 # sum of labels and entry fields per row
        nRows=32 #5+len(newDf) # number of rows in the GUI - not so important - window is scrollable
        xPadding=int(0.008*self.maxW)
        wSpacer=int(2*0.008*self.maxW)
        yPadding=int(0.008*self.maxH)
        vSpacer=int(2*0.008*self.maxH)
        myFontFamily="Georgia"
        sLongestTxt="Latitude NN :"
        (bFontFound, fsTINY,  fsSMALL,  fsNORMAL,  fsLARGE,  fsHUGE,  fsGIGANTIC, myFontHeight,  bWeMustStackColumns, bSuccess)= \
            bs.calculateEstheticFontSizes(myFontFamily,  self.maxW,  self.maxH, sLongestTxt, nCols, nRows, xPad=xPadding, 
                                                        yPad=yPadding, maxFontSize=20, USE_TKINTER=True, bWeCanStackColumns=False)
        hDeadSpace=wSpacer+(nCols*xPadding*2)+wSpacer
        vDeadSpace=2*yPadding 
        colWidth=int((self.maxW - hDeadSpace)/(nCols*1.0))
        rowHeight=int((self.maxH - vDeadSpace)/(nRows*1.0))
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
        dfAllObs = pd.read_csv (fDiscoveredObservations)
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
        newDf.to_csv(sTmpPrfx+'_dbg_newDfObs.csv', mode='w', sep=',')  
        nObs=len(newDf)
        #filtered = ((newDf['selected'] == True))
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
        newDf.to_csv(sTmpPrfx+'_dbg_selectedObs.csv', mode='w', sep=',')  

        
        # Now we venture to make the root scrollable....
        #main_frame = tk.Frame(root)
        rootFrame = tk.Frame(root)
        rootFrame.configure(background='sienna1')
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
        self.iObservationsFileLocation= ge.guiIntVar(value=1)
        #iObservationsFileLocation.set(1) # Read observations from local file
        if ('CARBONPORTAL' in ymlContents['observations'][tracer]['file']['location']):
            self.iObservationsFileLocation.set(2)
        self.RankingLabel = ctk.CTkLabel(rootFrame,
                                   text="Obsdata Ranking", font=("Georgia",  fsNORMAL))
        self.RankingLabel.grid(row=1, column=0,
                                  columnspan=1, padx=xPadding, pady=yPadding,
                                  sticky="nw")

        # Ranking for Observation Files
        rankingList=ymlContents['observations'][tracer]['file']['ranking']
        self.ObsFileRankingTbxVar = ge.guiStringVar(value="ObsPack")
        self.ObsFileRankingBox = ge.guiTextBox(rootFrame,  width=colWidth,  height=(2.4*rowHeight+vSpacer),  fontName="Georgia",  fontSize=fsSMALL)
                                        # ctk.CTkTextbox(rootFrame,
                                        # width=colWidth,
                                        # height=int(2.4*rowHeight+vSpacer))
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
        self.ObsLv1CkbVar = ge.guiBooleanVar(value=True)
        self.ObsLv1Ckb = ge.guiCheckBox(rootFrame,
                            text="Level1", fontName="Georgia",  fontSize=fsNORMAL,
                            variable=self.ObsLv1CkbVar,
                             onvalue=True, offvalue=False)                             
        self.ObsLv1Ckb.grid(row=1, column=1,
                          padx=xPadding, pady=yPadding,
                          sticky="nw")

        self.ObsNRTCkbVar = ge.guiBooleanVar(value=True)
        self.ObsNRTCkb = ge.guiCheckBox(rootFrame,
                            text="NRT", fontName="Georgia",  fontSize=fsNORMAL,
                            variable=self.ObsNRTCkbVar,
                             onvalue=True, offvalue=False)                             
        self.ObsNRTCkb.grid(row=2, column=1,
                          padx=xPadding, pady=yPadding,
                          sticky="nw")

        self.ObsOtherCkbVar = ge.guiBooleanVar(value=True)
        self.ObsOtherCkb = ge.guiCheckBox(rootFrame,
                            text="Other", fontName="Georgia",  fontSize=fsNORMAL,
                            variable=self.ObsOtherCkbVar,
                             onvalue=True, offvalue=False)                             
        self.ObsOtherCkb.grid(row=3, column=1,
                          padx=xPadding, pady=yPadding,
                          sticky="nw")
        
        
        # Col 3    Filtering of station altitudes
        #  ##################################################

        self.FilterStationAltitudesCkbVar = ge.guiBooleanVar(value=ymlContents['observations']['filters']['bStationAltitude'])
        self.FilterStationAltitudesCkb = ge.guiCheckBox(rootFrame,
                            text="Filter station altitudes", fontName="Georgia",  fontSize=fsNORMAL,
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
        self.stationMinAlt=ge.guiStringVar(value=f'{stationMinAlt}')
        self.stationMaxAlt=ge.guiStringVar(value=f'{stationMaxAlt}')
        # min Altitude Entry
        self.stationMinAltEntry = ge.guiDataEntry(rootFrame,textvariable=self.stationMinAlt,
                          placeholder_text=self.stationMinAlt, width=colWidth)
        self.stationMinAltEntry.grid(row=2, column=4,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="nw")
        # max Altitude Entry
        self.stationMaxAltEntry = ge.guiDataEntry(rootFrame,textvariable=self.stationMaxAlt,
                          placeholder_text=self.stationMaxAlt, width=colWidth)
        self.stationMaxAltEntry.grid(row=3, column=4,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="nw")
                             

        # Col 5    -  sampling height filter
        # ################################################################
        # 

        self.FilterSamplingHghtCkbVar = ge.guiBooleanVar(value=ymlContents['observations']['filters']['bSamplingHeight'])
        self.FilterSamplingHghtCkb = ge.guiCheckBox(rootFrame,
                            text="Filter sampling heights", fontName="Georgia",  fontSize=fsNORMAL,
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
        self.inletMinHght=ge.guiStringVar(value=f'{inletMinHght}')
        self.inletMaxHght=ge.guiStringVar(value=f'{inletMaxHght}')
        # min inlet height
        self.inletMinHghtEntry = ge.guiDataEntry(rootFrame,textvariable=self.inletMinHght,
                          placeholder_text=self.inletMinHght, width=colWidth)
        self.inletMinHghtEntry.grid(row=2, column=6,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="nw")
        # max inlet height
        self.inletMaxHghtEntry = ge.guiDataEntry(rootFrame,textvariable=self.inletMaxHght,
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
        self.isICOSRadioButtonVar = ge.guiIntVar(value=1)
        if (ymlContents['observations']['filters']['ICOSonly']==True):
            self.isICOSRadioButtonVar.set(2)
        self.isICOSplusRadioButton = ge.guiRadioButton(rootFrame,
                                   text="Any station", fontName="Georgia",  fontSize=fsNORMAL,
                                   variable=self.isICOSRadioButtonVar,  value=1,  command=isICOSrbAction)
        self.isICOSplusRadioButton.grid(row=2, column=7,
                            padx=xPadding, pady=yPadding,
                            sticky="nw")
         
        self.ICOSradioButton = ge.guiRadioButton(rootFrame,
                                   text="ICOS only", fontName="Georgia",  fontSize=fsNORMAL, 
                                   variable=self.isICOSRadioButtonVar,  value=2,  command=isICOSrbAction)
        self.ICOSradioButton.grid(row=3, column=7,
                            padx=xPadding, pady=yPadding,
                            sticky="nw")


        # Col_11
        # ################################################################
        # 

        def CloseTheGUI():
            if tk.messagebox.askokcancel("Quit", "Is it OK to abort your Lumia run?"):
                logger.info("LumiaGUI was canceled.")
                sCmd="touch LumiaGui.stop"
                hk.runSysCmd(sCmd)
                global LOOP2_ACTIVE
                LOOP2_ACTIVE = False
            root.protocol("WM_DELETE_WINDOW", CloseTheGUI)
        
        def CancelAndQuit(): 
            logger.info("LumiaGUI was canceled.")
            sCmd="touch LumiaGui.stop"
            hk.runSysCmd(sCmd)
            global LOOP2_ACTIVE
            LOOP2_ACTIVE = False

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
        def GoBtnHit(ymlFile,  ymlContents):
            sOutputPrfx=ymlContents[ 'run']['thisRun']['uniqueOutputPrefix']
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
                newDf.to_csv(sTmpPrfx+'_dbg_allObsInTimeSpaceSlab.csv', mode='w', sep=',')  
            except:
                sTxt=f"Fatal Error: Failed to write to file {sTmpPrfx}_dbg_allObsInTimeSpaceSlab.csv. Please check your write permissions and possibly disk space etc."
                CancelAndQuit(sTxt)
            try:
                dfq['pid2'] = dfq['pid'].apply(bs.grabFirstEntryFromList)
                dfq['samplingHeight2'] = dfq['samplingHeight'].apply(bs.grabFirstEntryFromList)
                #,selected,country,stationID,altOk,altitude,HghtOk,samplingHeight[Lst],isICOS,latitude,longitude,dClass,dataSetLabel,includeCountry,includeStation,pid[Lst],pid2,samplingHeight2
                dfq.drop(columns='pid',inplace=True) # drop columns with lists. These are replaced with single values from the first list entry
                dfq.drop(columns='samplingHeight',inplace=True) # drop columns with lists. These are replaced with single values from the first list entry
                dfq.drop(columns='selected',inplace=True)
                dfq.drop(columns='altOk',inplace=True)
                dfq.drop(columns='HghtOk',inplace=True)
                dfq.drop(columns='includeCountry',inplace=True)
                dfq.drop(columns='includeStation',inplace=True)
                dfq.rename(columns={'pid2': 'pid', 'samplingHeight2': 'samplingHeight'},inplace=True)
                ymlContents['observations'][tracer]['file']['selectedObsData']=sOutputPrfx+"selected-ObsData-"+tracer+".csv"
                dfq.to_csv(ymlContents['observations'][tracer]['file']['selectedObsData'], mode='w', sep=',')
                dfPids=dfq['pid']
                ymlContents['observations'][tracer]['file']['selectedPIDs']=sOutputPrfx+"selected-PIDs-"+tracer+".csv"
                selectedPidLst = dfPids.iloc[1:].tolist()
                sFOut=ymlContents['observations'][tracer]['file']['selectedPIDs']
                # dfPids.to_csv(ymlContents['observations'][tracer]['file']['selectedPIDs'], mode='w', sep=',')
                with open( sFOut, 'w') as fp:
                    for item in selectedPidLst:
                        fp.write("%s\n" % item)
                
            except:
                sTxt=f"Fatal Error: Failed to write to file {sOutputPrfx}-selected-ObsData-{tracer}.csv. Please check your write permissions and possibly disk space etc."
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
                hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'observations',  'filters',  'StationsExcluded'],   
                                                                            value=excludedStationsList, bNewValue=True)
                if(nC==0):
                    logger.info("No countries were rejected")
                else:
                    s=""
                    for element in excludedCountriesList:
                        s=s+element+', '
                    logger.info(f"{nC} countries ({s[:-2]}) were rejected")
                hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'observations',  'filters',  'CountriesExcluded'],   
                                                                            value=excludedCountriesList, bNewValue=True)
            except:
                pass
            # Save  all details of the configuration and the version of the software used:
            hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'observations',  'filters',  'ICOSonly'],   
                                                                        value=ymlContents['observations']['filters']['ICOSonly'], bNewValue=True)
            
            # sOutputPrfx=ymlContents[ 'run']['thisRun']['uniqueOutputPrefix']
            ymlContents['observations'][tracer]['file']['discoverData']=False # lumiaGUI has already hunted down and documented all user obsData selections
            try:
                with open(ymlFile, 'w') as outFile:
                    yaml.dump(ymlContents, outFile)
            except:
                sTxt=f"Fatal Error: Failed to write to text file {ymlFile} in local run directory. Please check your write permissions and possibly disk space etc."
                CancelAndQuit(sTxt)
                return
            
            sCmd="touch LumiaGui.go"
            hk.runSysCmd(sCmd)
            logger.info("Done. LumiaGui completed successfully. Config and Log file written.")
            # self.bPleaseCloseTheGui.set(True)
            global LOOP2_ACTIVE
            LOOP2_ACTIVE = False

        # ######################################################            
        self.GoButton = ctk.CTkButton(rootFrame, font=("Georgia", fsNORMAL), 
                                         text_color='gray5',  text_color_disabled='gray70',  text="RUN", fg_color='green2')  
        self.GoButton.configure(command= lambda: GoBtnHit(ymlFile,  ymlContents)) 
        self.GoButton.grid(row=3, column=11,
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
        
        #rootFrame.update() # above widgets are drawn and we can get column width measures
        #x, y, width, height = rootFrame.grid_bbox()

        rootFrameCanvas = tk.Frame(rootFrame)
        rootFrameCanvas.configure(background='OliveDrab1')
        rootFrameCanvas.grid(row=5, column=0,  columnspan=12,  rowspan=20, pady=(5, 0), sticky='nw') #, columnspan=11,  rowspan=10
        rootFrameCanvas.grid_rowconfigure(0, weight=0)
        rootFrameCanvas.grid_columnconfigure(0, weight=0)
        # Set grid_propagate to False to allow 5-by-5 buttons resizing later
        #rootFrameCanvas.grid_propagate(False)
        cWidth = self.appWidth - xPadding
        cHeight = self.appHeight - (7*rowHeight) - (3*yPadding)
        cHeight = self.appHeight - (7*rowHeight) - (3*yPadding)
        logger.debug(f'requested dimensions for scrollableCanvas: w={cWidth} h={cHeight}.')
        # Add a scrollableCanvas in that frame
        scrollableCanvas = tk.Canvas(rootFrameCanvas, width=cWidth, height=cHeight, borderwidth=0, highlightthickness=0)
        scrollableCanvas.configure(background='cadet blue')
        scrollableCanvas.grid(row=0, column=0,  columnspan=12,  rowspan=10, sticky="news")
        
        # Link a scrollbar to the scrollableCanvas
        vsb = tk.Scrollbar(rootFrameCanvas, orient="vertical", command=scrollableCanvas.yview)
        vsb.grid(row=0, column=1, sticky='ns')
        scrollableCanvas.configure(yscrollcommand=vsb.set)
        
        # Create a frame to contain the widgets for all obs data sets found following initial user criteria
        scrollableFrame4Widgets = tk.Frame(scrollableCanvas) #, bg="#82d0d2") #  slightly lighter than "cadet blue"
        scrollableFrame4Widgets.configure(background='orchid1')
        scrollableCanvas.create_window((0, 0), window=scrollableFrame4Widgets, anchor='nw')
        # scrollableFrame4Widgets.grid_rowconfigure(0, weight=1,uniform = 999)
        
        # 
        #startRow=5
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
            global LOOP2_ACTIVE
            LOOP2_ACTIVE = True
            while LOOP2_ACTIVE:
                time.sleep(1)
            logger.info("Closing the GUI...")
            try:
                root.after(1000, root.event_generate("<Destroy>"))  # Wait 1 sec so the the main thread can meanwhile be exited before the GUI is destroyed
            except:
                pass
        _thread.start_new_thread(loop_function, ())
        #root.mainloop()
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
        myWidgetVar= ge.guiBooleanVar(value=row['selected'])
        myWidgetSelect  = GridCTkCheckBox(scrollableFrame4Widgets, gridID,  text="", font=("Georgia", fsNORMAL),
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
            myWidgetVar= ge.guiBooleanVar(value=row['includeCountry'])
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
        myWidgetVar= ge.guiBooleanVar(value=row['includeStation'])
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
        myWidgetVar= ge.guiStringVar(value=str(row['samplingHeight'][0])) 
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
        myWidgetVar= bs.formatMyString(str(row['altitude']), 9, 'm')\
                                +bs.formatMyString(affiliationICOS, 12, '')\
                                +bs.formatMyString((sLat), 11, '°N')\
                                +bs.formatMyString((sLon), 10, '°E')\
                                +bs.formatMyString(str(row['dClass']), 8, '')\
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
                            bs.swapListElements(sSamplingHeights, 0, nPos)
                            f=[]
                            for s in sSamplingHeights:
                                f1=float(s)
                                f.append(f1)
                            obsDf.at[(ri) ,  ('samplingHeight')] =f
                            # TODO: keep PIDs in sync!
                            pids=obsDf.at[(ri) ,  ('pid')]
                            bs.swapListElements(pids, 0, nPos)
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
    def checkGuiValues(self, ymlContents):
        bErrors=False
        sErrorMsg=""
        bWarnings=False
        sWarningsMsg=""

        # ObservationsFileLocation
        #if (self.iObservationsFileLocation.get()==2):
        #    ymlContents['observations'][tracer]['file']['location'] = 'CARBONPORTAL'
        #else:
        #    ymlContents['observations'][tracer]['file']['location'] = 'LOCAL'
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
        
    

def callLumiaGUI(ymlFile,  scriptDirectory,  bStartup=True): 
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
    @type string
    '''
    #bPage2Done=False
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
        
    current_date = datetime.now()
    sNow=current_date.isoformat("T","seconds") # sNow is the time stamp for all log files of a particular run
    # colons from the time are not without problems in directory and file names. Better to play it safe and replace them with uynderscores
    sNow=re.sub(':', '_', sNow)
    sNow=sNow[:-3] # minutes is good enough....don't need seconds if a run takes hours...
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, ['run',  'thisRun', 'uniqueIdentifierDateTime'],  value=sNow, bNewValue=True)

    # All output is written into  subdirectories named after the run.thisRun.uniqueIdentifierDateTime key
    # Create these subdirectories. This also ensures early on that we can write to the intended locations
    try:
        sOutpDir=ymlContents['run']['paths']['output']
    except:
        sOutpDir="./output"
        hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'paths',  'output' ],   value=sOutpDir, bNewValue=True)
    if(len(sOutpDir)>0):
        sCmd=("mkdir -p "+sOutpDir)
    try:
        os.system(sCmd)
    except:
        sys.exit(f'Abort. Failed to create user-requested output directory {sOutpDir}. Please check the key run.paths.output in your {ymlFile} file as well as your write permissions.')
    sOutputPrfx='LumiaGUI-'+sNow+os.path.sep+'LumiaGUI-'+sNow+'-'
    sTmpPrfx=sOutputPrfx # same structure below the Temp and Output directories
    if ((len(sOutpDir)>0) and (sOutpDir[-1]!=os.path.sep)):
        sOutpDir=sOutpDir+os.path.sep
    sCmd=("mkdir -p "+sOutpDir+'LumiaGUI-'+sNow)
    try:
        os.system(sCmd)
    except:
        sys.exit(f'Abort. Failed to create user-requested output sub-directory {sOutpDir}LumiaGUI-{sNow}. Please check the key run.paths.output in your {ymlFile} file as well as your write permissions.')
    sOutputPrfx=sOutpDir+sOutputPrfx
    try:
        sTmpDir=ymlContents['run']['paths']['temp']
    except:
        sTmpDir="./tmp"
        hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'paths',  'temp' ],   value=sTmpDir, bNewValue=True)
    if(len(sOutpDir)>0):
        sCmd=("mkdir -p "+sTmpDir)
    try:
        os.system(sCmd)
    except:
        sys.exit(f'Abort. Failed to create user-requested temp directory {sTmpDir}. Please check the key run.paths.output in your {ymlFile} file as well as your write permissions.')
    if ((len(sTmpDir)>0) and (sTmpDir[-1]!=os.path.sep)):
        sTmpDir=sTmpDir+os.path.sep
    sCmd=("mkdir -p "+sTmpDir+'LumiaGUI-'+sNow)
    try:
        os.system(sCmd)
    except:
        sys.exit(f'Abort. Failed to create user-requested temp sub-directory {sTmpDir}LumiaGUI-{sNow}. Please check the key run.paths.output in your {ymlFile} file as well as your write permissions.')
    sTmpPrfx=sTmpDir+sTmpPrfx

    # hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'uniqueIdentifierDateTime'],   value=sNow, bNewValue=True)
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'uniqueOutputPrefix'],   value=sOutputPrfx, bNewValue=True)
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'thisRun',  'uniqueTmpPrefix'],   value=sTmpPrfx, bNewValue=True)
    # Read simulation time
    tracers = ymlContents['run']['tracers']
    if (isinstance(tracers, list)):
        tracer=tracers[0]
    else:
        tracer=ymlContents['run']['tracers']
    
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [  'path',  'data'],   value='./data', bNewValue=False) # for runflex ./data/meteo/ea.eurocom025x025/
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [  'run',  'paths',  'temp'],   value='/temp', bNewValue=False)
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'paths',  'footprints'],   value='/footprints', bNewValue=False)
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, ['correlation',  'inputdir'],   value='/data/corr', bNewValue=False )
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'tag'],  value=args.tag, bNewValue=True)
    
    # Run-dependent paths
    #if(not bs.nestedKeyExists(ymlContents, 'run',  'paths',  'output')):
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'run',  'paths',  'output'],   value=os.path.join('/output', args.tag), bNewValue=False)
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'var4d',  'communication',  'file'],   value='congrad.nc', bNewValue=False)
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'emissions',  '*',  'archive'],   value='rclone:lumia:fluxes/nc/', bNewValue=False)
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'emissions',  '*',  'path'],   value= '/data/fluxes/nc', bNewValue=False)
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'model',  'transport',  'exec'],   value='/lumia/transport/multitracer.py', bNewValue=False)
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'transport',  'output'],   value= 'T', bNewValue=False)
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'transport',  'steps'],   value='forward', bNewValue=False)
    
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'branch'],   value='gitkraken://repolink/778bf0763fae9fad55be85dde4b42613835a3528/branch/LumiaDA?url=git%40github.com%3Alumia-dev%2Flumia.git',  bNewValue=True)
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'lumia',  'commit'],   value='gitkraken://repolink/778bf0763fae9fad55be85dde4b42613835a3528/commit/5e5e9777a227631d6ceeba4fd8cff9b241c55de1?url=git%40github.com%3Alumia-dev%2Flumia.git',  bNewValue=True)
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [  'softwareUsed',  'runflex',  'branch'],   value='gitkraken://repolink/b9411fbf7aeeb54d7bb34331a98e2cc0b6db9d5f/branch/v2?url=https%3A%2F%2Fgithub.com%2Flumia-dev%2Frunflex.git',  bNewValue=True)
    hk.setKeyVal_Nested_CreateIfNecessary(ymlContents, [ 'softwareUsed',  'runflex',  'commit'],   value='gitkraken://repolink/b9411fbf7aeeb54d7bb34331a98e2cc0b6db9d5f/commit/aad612b36a247046120bda30c8837acb5dec4f26?url=https%3A%2F%2Fgithub.com%2Flumia-dev%2Frunflex.git',  bNewValue=True)
    
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
    tracer='co2'
    try:
        if (isinstance(ymlContents['run']['tracers'], str)):
            tracer=ymlContents['run']['tracers']
        else:
            trac=ymlContents['run']['tracers']
            tracer=trac[0]
    except:
        tracer='co2'
        
    #tracer=tracer.upper()    
    sStart=ymlContents['run']['time']['start']    # should be a string like start: '2018-01-01 00:00:00'
    sEnd=ymlContents['run']['time']['end']
    pdTimeStart = to_datetime(sStart[:19], format="%Y-%m-%d %H:%M:%S")
    pdTimeStart=pdTimeStart.tz_localize('UTC')
    pdTimeEnd = to_datetime(sEnd[:19], format="%Y-%m-%d %H:%M:%S")
    pdTimeEnd=pdTimeEnd.tz_localize('UTC')
    timeStep=ymlContents['run']['time']['timestep']
    if(bStartup):
        bStartup=False
        if(os.path.isfile("LumiaGui.stop")):
            sCmd="rm LumiaGui.stop"
            hk.runSysCmd(sCmd,  ignoreError=True)
        if(os.path.isfile("LumiaGui.go")):
            sCmd="rm LumiaGui.go"
            hk.runSysCmd(sCmd,  ignoreError=True)
        
    if os.environ.get('DISPLAY','') == '':
        print('no display found. Using :0.0')
        os.environ.__setitem__('DISPLAY', ':0.0')
    root=ctk.CTk()
    widgetsLst = []
    # root.iconify()
    guiPage2=RefineObsSelectionGUI(root,  sLogCfgPath=sLogCfgPath, ymlContents=ymlContents, ymlFile=ymlFile, 
                        widgetsLst=widgetsLst) 
    guiPage2.iconify
    #print('gui2object created')
    guiPage2.run(root,  sLogCfgPath, ymlContents, ymlFile, widgetsLst, pdTimeStart, pdTimeEnd, timeStep, tracer) 
    logger.debug('left guiPage2')
    root.mainloop()
    return

    
     
p = argparse.ArgumentParser()
p.add_argument('--start', dest='start', default=None, help="Start of the simulation in date+time ISO notation as in \'2018-08-31 00:18:00\'. Overwrites the value in the rc-file")
p.add_argument('--end', dest='end', default=None, help="End of the simulation as in \'2018-12-31 23:59:59\'. Overwrites the value in the rc-file")
p.add_argument('--rcf', dest='rcf', default=None, help="Same as the --ymf option. Deprecated. For backward compatibility only.")   
p.add_argument('--ymf', dest='ymf', default=None,  help='yaml configuration file where the user plans his or her Lumia run: parameters, input files etc.')   
p.add_argument('--serial', '-s', action='store_true', default=False, help="Run on a single CPU")
p.add_argument('--noTkinter', '-n', action='store_true', default=False, help="Do not use tkinter (=> use ipywidgets)")
#p.add_argument('--fDiscoveredObs', dest='fDiscoveredObservations', default=None,  help="If step2 is set you must specify the .csv file that lists the observations discovered by LUMIA, typically named DiscoveredObservations.csv")   # yaml configuration file where the user plans his or her Lumia run: parameters, input files etc.
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
fDiscoveredObservations=None


bError=False

#if(os.path.exists(fDiscoveredObservations)==False):
#    logger.error(f"LumiaGUI called for refinement of the observations presumably found, but the file name provided for the list of observations ({fDiscoveredObservations}) cannot be found or read. If you are not using the default file, you can provide your own with the --fDiscoveredObs switch.")
#    bError=True
if(bError):    
    sCmd="touch LumiaGui.stop"
    try:
        os.system(sCmd)
    except:
        sTxt=f"Fatal Error: Failed to execute system command >>{sCmd}<<. Please check your write permissions and possibly disk space etc."
        logger.warning(sTxt)
    sys.exit(-4)

# Do the housekeeping like documenting the current git commit version of this code, date, time, user, platform etc.
thisScript='LumiaGUI'
ymlFile=hk.documentThisRun(ymlFile, thisScript,  args)  # from housekeepimg.py
# Now the config.yml file has all the details for this particular run
    
    
# Shall we call the GUI to tweak some parameters before we start the ball rolling?
# no need to pass args.start or args.end because hk.documentThisRun() already took care of these.
scriptDirectory = os.path.dirname(os.path.abspath(sys.argv[0]))
callLumiaGUI(ymlFile,  scriptDirectory)

logger.info("LumiaGUI window closed")
logger.info('LumiaGUI completed successfully. The updayed Lumia config file has been written to:')
logger.info(ymlFile)

print('LumiaGUI completed successfully. The updayed Lumia config file has been written to:')
print(ymlFile)

