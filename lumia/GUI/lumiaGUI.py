#!/usr/bin/env python3


import os
import sys
import housekeeping as hk
import pandas as pd
import argparse
from datetime import datetime,  timedelta
import yaml
#import time
import re
from pandas import to_datetime
from loguru import logger
import _thread
from queryCarbonPortal import discoverObservationsOnCarbonPortal

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



# =============================================================================
# Core functions with interesting tasks
# =============================================================================

def prepareCallToLumiaGUI(ymlFile,  scriptDirectory, iVerbosityLv='INFO'): 
    '''
    Function 
    LumiaGUI exposes selected paramters of the LUMIA config file (in yaml data format) to a user
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
    # All output is written into  subdirectories (defined by run.paths.output and run.paths.temp) followed by a directory level named 
    # after the run.thisRun.uniqueIdentifierDateTime key which is also what all subsequent output filenames are starting with.
    # see housekeeping.py for details
    # sOutputPrfx=ymlContents[ 'run']['thisRun']['uniqueOutputPrefix']
    # self.sTmpPrfx=ymlContents[ 'run']['thisRun']['uniqueTmpPrefix'] 

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
        lumiaGuiAppInst=lumiaGuiApp(root)  # the main GUI application (class)
        lumiaGuiAppInst.sLogCfgPath = sLogCfgPath  # TODO: check if this is obsolete now
        lumiaGuiAppInst.ymlFile = ymlFile  # the initial Lumia configuration file from which we start
        lumiaGuiAppInst.ymlContents = ymlContents # the contents of the Lumia configuration file
        lumiaGuiAppInst.sOutputPrfx=ymlContents[ 'run']['thisRun']['uniqueOutputPrefix'] # where to write output
        lumiaGuiAppInst.sTmpPrfx=ymlContents[ 'run']['thisRun']['uniqueTmpPrefix'] 
        lumiaGuiAppInst.iVerbosityLv=iVerbosityLv
        guiPg1TpLv=lumiaGuiAppInst.guiPage1AsTopLv(iVerbosityLv)  # the first of 2 pages of the GUI implemented as a toplevel window (init)
        #lumiaGuiAppInst.runPage2  # execute the central method of lumiaGuiApp
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




# =============================================================================
# Tkinter solution for GUI
# =============================================================================
class lumiaGuiApp:
    def __init__(self, root):
        self.root = root
        self.guiPg1TpLv=None
        self.label1 = tk.Label(self.root, text="App main window - hosting the second GUI page.")
        self.root.protocol("WM_DELETE_WINDOW", self.closeApp)
        #self.label1.pack()
        
    def closeTopLv(self, bWriteStop=True):  # of lumiaGuiApp
        self.guiPg1TpLv.destroy()
        if(bWriteStop):
            bs.cleanUp(bWriteStop)
            logger.info('lumiaGUI canceled by user.')
            self.closeApp(False)
        self.guiPg1TpLv=None
        self.root.deiconify()
        self.runPage2()  

    def closeApp(self, bWriteStop=True):  # of lumiaGuiApp
        bs.cleanUp(bWriteStop)
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
                
    def EvHdPg1GotoPage2(self):
        bGo=False
        (bErrors, sErrorMsg, bWarnings, sWarningsMsg) = self.checkGuiValues()
        ge.guiWipeTextBox(self.Pg1displayBox, protect=True) # delete all text
        if((bErrors) and (bWarnings)):
            # self.Pg1displayBox.insert("0.0", "Please fix the following errors:\n"+sErrorMsg+sWarningsMsg)
            ge.guiWriteIntoTextBox(self.Pg1displayBox, "Please fix the following errors:\n"+sErrorMsg+sWarningsMsg, protect=True)
        elif(bErrors):
            ge.guiWriteIntoTextBox(self.Pg1displayBox, "Please fix the following errors:\n"+sErrorMsg, protect=True)
        elif(bWarnings):
            ge.guiWriteIntoTextBox(self.Pg1displayBox, "Please consider carefully the following warnings:\n"+sWarningsMsg, protect=True)
            if(self.bIgnoreWarningsCkbVar.get()):
                bGo=True
            if(USE_TKINTER):
                self.Pg1ignoreWarningsCkb.configure(state=tk.NORMAL)
        else:
            bGo=True
        if(USE_TKINTER):
            self.Pg1displayBox.configure(state=tk.DISABLED)  # configure textbox to be read-only

        if(bGo):
            self.guiPg1TpLv.iconify()
            # Save  all details of the configuration and the version of the software used:
            try:
                with open(self.ymlFile, 'w') as outFile:
                    yaml.dump(self.ymlContents, outFile)
            except:
                sTxt=f"Fatal Error: Failed to write to text file {self.ymlFile} in local run directory. Please check your write permissions and possibly disk space etc."
                logger.error(sTxt)
                self.closeTopLv(bWriteStop=True)  # Abort. Do not proceed to page 2
            logger.info("Done. LumiaGui part-1 completed successfully. Config file updated.")
            
            # At this moment the commandline is visible. Before closing the toplevel and proceeding, we need to discover any requested data.
            # Once collected, we have the relevant info to create the 2nd gui page and populate it with dynamical widgets.
            self.huntAndGatherObsData()
            self.closeTopLv(bWriteStop=False)
            
    def EvHdPg1selectFile(self):
        filename = ctk.filedialog.askopenfilename()
        if ((filename is None) or (len(filename)<5)):
            return   # Canceled or no valid filename returned: Keep previous data and continue
        # update the file entry widget
        self.ObsFileLocationEntryVar = ge.guiStringVar(value=filename)
        ge.updateWidget(self.Pg1ObsFileLocationLocalEntry,  value=self.ObsFileLocationEntryVar, bTextvar=True)
        # if textvariable is longer than its entry box, i.e. the path spills over, it will be right-aligned, showing the end with the file name
        if(USE_TKINTER):
            self.Pg1ObsFileLocationLocalEntry.xview_moveto(1)  

    def EvHdPg1SetObsFileLocation(self):
        if (self.iObservationsFileLocation.get()==1):
           self.ymlContents['observations'][self.tracer]['file']['location'] = 'LOCAL'
        else:
            self.ymlContents['observations'][self.tracer]['file']['location'] = 'CARBONPORTAL'

    def EvHdPg1SetTracer(self):
        if (self.iTracerRbVal.get()==1):
           self.ymlContents['run']['tracers'] = 'co2'
        else:
            self.ymlContents['run']['tracers'] = 'ch4'

    def getFilters(self):
        self.bUseStationAltitudeFilter=self.ymlContents['observations']['filters']['bStationAltitude']
        self.bUseSamplingHeightFilter=self.ymlContents['observations']['filters']['bSamplingHeight']
        self.stationMinAlt = self.ymlContents['observations']['filters']['stationMinAlt']     # in meters amsl
        self.stationMaxAlt = self.ymlContents['observations']['filters']['stationMaxAlt']  # in meters amsl
        self.inletMinHght = self.ymlContents['observations']['filters']['inletMinHeight']     # in meters amsl
        self.inletMaxHght = self.ymlContents['observations']['filters']['inletMaxHeight']  # in meters amsl

    def huntAndGatherObsData(self):
        # prowl through the carbon portal for any matching data sets in accordance with the choices from the first gui page.
        sStart=self.ymlContents['run']['time']['start']    # should be a string like start: '2018-01-01 00:00:00'
        sEnd=self.ymlContents['run']['time']['end']
        pdTimeStart = to_datetime(sStart[:19], format="%Y-%m-%d %H:%M:%S")
        pdTimeStart=pdTimeStart.tz_localize('UTC')
        pdTimeEnd = to_datetime(sEnd[:19], format="%Y-%m-%d %H:%M:%S")
        pdTimeEnd=pdTimeEnd.tz_localize('UTC')
        #timeStep=self.ymlContents['run']['time']['timestep']
        
        # discoverObservationsOnCarbonPortal()
        # TODO: uncomment the next line to stop using a canned list DiscoveredObservations-short.csv for testing (saves a lot fo time in the debugger)
        #(dobjLst, selectedDobjLst, dfObsDataInfo, self.fDiscoveredObservations, self.badPidsLst)=discoverObservationsOnCarbonPortal(self.tracer,   
        #                    pdTimeStart, pdTimeEnd, timeStep,  self.ymlContents,  sDataType=None, printProgress=True,    self.iVerbosityLv='INFO')
        self.fDiscoveredObservations='DiscoveredObservations.csv' # 'DiscoveredObservations-short.csv'
        self.badPidsLst=[]
        
        if (len(self.badPidsLst) > 0):
            sFOut=self.ymlContents['observations'][self.tracer]['file']['selectedPIDs']
            sFOut=sFOut[:-21]+'bad-PIDs.csv'
            with open( sFOut, 'w') as fp:
                for item in self.badPidsLst:
                    fp.write("%s\n" % item)
        # re-organise the self.fDiscoveredObservations dataframe,
        # It is presently sorted by country, station, dataRanking (dClass), productionTime and samplingHeight -- in that order
        #   columnNames=['pid', 'selected','stationID', 'country', 'isICOS','latitude','longitude','altitude','samplingHeight','size', 
        #                 'nRows','dataLevel','obsStart','obsStop','productionTime','accessUrl','fileName','dClass','dataSetLabel'] 
        # 
        # 1) Set the first dataset for each station, highest dClass and heighest samplingHeight to selected and all other ones to not-selected 
        # 2) if there are multiple samplingHeights for otherwise the same characterestics, then stick the lower heights into a list 
        #    of samplingHeight + PID pairs for an otherwise single entry.
        #
        #        #data=[[pid, pidMetadata['specificInfo']['acquisition']['station']['id'], pidMetadata['coverageGeo']['geometry']['coordinates'][0], pidMetadata['coverageGeo']['geometry']['coordinates'][1], pidMetadata['coverageGeo']['geometry']['coordinates'][2], pidMetadata['specificInfo']['acquisition']['samplingHeight'], pidMetadata['size'], pidMetadata['specification']['dataLevel'], pidMetadata['references']['temporalCoverageDisplay'], pidMetadata['specificInfo']['productionInfo']['dateTime'], pidMetadata['accessUrl'], pidMetadata['fileName'], int(0), pidMetadata['specification']['self']['label']]]
        self.nRows=int(0)
        bCreateDf=True
        bTrue=True
        isDifferent=True
        #AllObsColumnNames=['pid', 'selected','stationID', 'country', 'isICOS','latitude','longitude','altitude','samplingHeight','size', 
        #            'nRows','dataLevel','obsStart','obsStop','productionTime','accessUrl','fileName','dClass','dataSetLabel'] 
        self.getFilters()
        newColumnNames=['selected','country', 'stationID', 'altOk', 'altitude', 'HghtOk', 'samplingHeight', 'isICOS', 'latitude', 'longitude', 'dClass', 'dataSetLabel', 'pid', 'includeCountry', 'includeStation']
        dfAllObs = pd.read_csv (self.fDiscoveredObservations)
        for index, row in dfAllObs.iterrows():
            hLst=[row['samplingHeight'] ]
            pidLst=[ row['pid']]
            bStationAltOk = (((row['altitude'] >= self.stationMinAlt) &
                                (row['altitude'] <= self.stationMaxAlt) ) | (self.bUseStationAltitudeFilter==False)) 
            bSamplHghtOk = (((row['samplingHeight'] >= self.inletMinHght) &
                                (row['samplingHeight'] <= self.inletMaxHght) ) | (self.bUseSamplingHeightFilter==False))
            newRow=[bTrue,row['country'], row['stationID'], bStationAltOk, row['altitude'],  
                            bSamplHghtOk, hLst, row['isICOS'], row['latitude'], row['longitude'], row['dClass'], row['dataSetLabel'],  pidLst, True,  True]
            
            if(bCreateDf):
                newDf=pd.DataFrame(data=[newRow], columns=newColumnNames)     
                bCreateDf=False
                self.nRows+=1
            else:
                #data=[pid, pidMetadata['specificInfo']['acquisition']['station']['id'], pidMetadata['coverageGeo']['geometry']['coordinates'][0], pidMetadata['coverageGeo']['geometry']['coordinates'][1], pidMetadata['coverageGeo']['geometry']['coordinates'][2], pidMetadata['specificInfo']['acquisition']['samplingHeight'], pidMetadata['size'], pidMetadata['specification']['dataLevel'], pidMetadata['references']['temporalCoverageDisplay'], pidMetadata['specificInfo']['productionInfo']['dateTime'], pidMetadata['accessUrl'], pidMetadata['fileName'], int(0), pidMetadata['specification']['self']['label']]
                isDifferent = ((row['stationID'] not in newDf['stationID'][self.nRows-1]) |
                                        (int(row['dClass']) != int(newDf['dClass'][self.nRows-1]) )
                )
                if(isDifferent):  # keep the new row as an entry that differs from the previous row in more than samplingHeight
                    newDf.loc[self.nRows] = newRow
                    self.nRows+=1
                else:
                    #newDf['samplingHeight'][self.nRows-1]+=[row['samplingHeight'] ]  # ! works on a copy of the column, not the original object
                    # Don't do as in the previous line. See here: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
                    newDf.at[(self.nRows-1) ,  ('samplingHeight')]+=[row['samplingHeight'] ]
                    newDf.at[(self.nRows-1) ,  ('pid')]+=[row['pid'] ]
                    #TODO: dClass
                
        if(not isDifferent):
            newDf.drop(newDf.tail(1).index,inplace=True) # drop the last row 
        newDf.to_csv(self.sTmpPrfx+'_dbg_newDfObs.csv', mode='w', sep=',')  
        nObs=len(newDf)
        #filtered = ((newDf['selected'] == True))
        #dfq= newDf[filtered]
        #nSelected=len(dfq)
        logger.info(f"There are {nObs} valid data sets in the selected geographical region ingoring multiple sampling heights.")
        isDifferent=True
        self.nRows=0
        isSameStation = False
        for index, row in newDf.iterrows():
            if((row['altOk']==False) or (row['HghtOk']==False)):
                newDf.at[(self.nRows) ,  ('selected')] = False
            if(self.nRows>0):
                isSameStation = ((row['stationID'] in newDf['stationID'][self.nRows-1]))
            if(isSameStation):
                newDf.at[(self.nRows) ,  ('selected')] = False
            self.nRows+=1
        newDf.to_csv(self.sTmpPrfx+'_dbg_selectedObs.csv', mode='w', sep=',')  

        self.excludedCountriesList = []
        self.excludedStationsList = []
        try:
            self.excludedCountriesList = self.ymlContents['observations']['filters']['CountriesExcluded']
        except:
            pass
        try:
            self.excludedStationsList = self.ymlContents['observations']['filters']['StationsExcluded']
        except:
            pass
        self.newDf = newDf    


    # ====================================================================
    # EventHandler and helper functions for widgets of the second GUI page  -- part of lumiaGuiApp (root window)
    # ====================================================================
    def guiPg2createRowOfObsWidgets(self, scrollableFrame4Widgets, widgetsLst, num, rowidx, row, guiRow, 
                                                sSamplingHeights, activeTextColor, 
                                                inactiveTextColor, fsNORMAL, xPadding, yPadding, fsSMALL, obsDf):
        ''' draw all the widgets belonging to one observational data set corresponding to a single line on the GUI '''
        #nWidgetsPerRow=5
        gridRow=[]
        
        bSelected=row['selected']
        if(bSelected):
            sTextColor=activeTextColor
        else:
            sTextColor=inactiveTextColor
        countryInactive=row['country'] in self.excludedCountriesList
        stationInactive=row['stationID'] in self.excludedStationsList
        
        colidx=int(0)  # row['selected']
        # ###################################################
        gridID=int((100*rowidx)+colidx)  # encode row and column in the button's variable
        myWidgetVar= ge.guiBooleanVar(value=row['selected'])
        myWidgetSelect  = ge.GridCTkCheckBox(scrollableFrame4Widgets, gridID,  text="",font=("Georgia", fsNORMAL),
                                                            text_color=sTextColor, text_color_disabled=sTextColor, 
                                                            variable=myWidgetVar, onvalue=True, offvalue=False) 
        myWidgetSelect.configure(command=lambda widgetID=myWidgetSelect.widgetGridID : self.EvHdPg2myCheckboxEvent(myWidgetSelect.widgetGridID, 
                                                    widgetsLst, obsDf, self.nWidgetsPerRow, activeTextColor, inactiveTextColor)) 
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
            myWidgetCountry  = ge.GridCTkCheckBox(scrollableFrame4Widgets, gridID, text=row['country'],text_color=sTextColor, text_color_disabled=sTextColor, 
                                                                font=("Georgia", fsNORMAL), variable=myWidgetVar, onvalue=True, offvalue=False)  
            myWidgetCountry.configure(command=lambda widgetID=myWidgetCountry.widgetGridID : self.EvHdPg2myCheckboxEvent(myWidgetCountry.widgetGridID, 
                                                        widgetsLst, obsDf, self.nWidgetsPerRow, activeTextColor, inactiveTextColor)) 
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
        myWidgetStationid  = ge.GridCTkCheckBox(scrollableFrame4Widgets, gridID, text=row['stationID'],text_color=sTextColor, text_color_disabled=sTextColor, 
                                                            font=("Georgia", fsNORMAL), variable=myWidgetVar, onvalue=True, offvalue=False) 
        myWidgetStationid.configure(command=lambda widgetID=myWidgetStationid.widgetGridID : self.EvHdPg2myCheckboxEvent(myWidgetStationid.widgetGridID, 
                                                        widgetsLst, obsDf, self.nWidgetsPerRow, activeTextColor, inactiveTextColor)) 
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
        myWidgetSamplingHeight  = ge.GridCTkOptionMenu(scrollableFrame4Widgets, gridID, values=sSamplingHeights,
                                                            variable=myWidgetVar, text_color=sTextColor, text_color_disabled=sTextColor,
                                                            font=("Georgia", fsNORMAL), dropdown_font=("Georgia",  fsSMALL)) 
        myWidgetSamplingHeight.configure(command=lambda widget=myWidgetSamplingHeight.widgetGridID : self.EvHdPg2myOptionMenuEvent(myWidgetSamplingHeight.widgetGridID, widgetsLst, obsDf, sSamplingHeights, self.nWidgetsPerRow, activeTextColor, inactiveTextColor))  
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
        myWidgetOtherLabels  = ge.GridCTkLabel(scrollableFrame4Widgets, gridID, text=myWidgetVar,text_color=sTextColor, text_color_disabled=sTextColor, 
                                                            font=("Georgia", fsNORMAL), textvariable=myWidgetVar, justify="right", anchor="e") 
        myWidgetOtherLabels.grid(row=guiRow, column=colidx, columnspan=6, padx=xPadding, pady=yPadding, sticky='nw')
        widgetsLst.append(myWidgetOtherLabels)
        gridRow.append(myWidgetVar)
        # ###################################################
        # guiPg2createRowOfObsWidgets() completed

    def EvHdPg2isICOSfilter(self):
        isICOSrbValue=self.isICOSplusRadioButton.cget("variable")
        if(isICOSrbValue==2):
            self.ymlContents['observations']['filters']['ICOSonly']=True
        else:
            self.ymlContents['observations']['filters']['ICOSonly']=False
        self.applyFilterRules()                       
    

    def EvHdPg2myCheckboxEvent(self, gridID, widgetsLst, obsDf, nWidgetsPerRow, activeTextColor, inactiveTextColor):
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
                self.EvHdPg2updateRowOfObsWidgets(widgetsLst, ri, row, nWidgetsPerRow, activeTextColor, inactiveTextColor)
            elif(ci==1):  # Country
                bSameCountry=True  # multiple rows may be affected
                self.nRows=len(obsDf)
                thisCountry=obsDf.at[(ri) ,  ('country')]
                while((bSameCountry) and (ri<self.nRows)):
                    if(bChkBxIsSelected):
                        # Set 'selected' to True only if the station, AltOk & HghtOk are presently selected AND dClass is the highest available, else not
                        try:
                            self.excludedCountriesList.remove(row['country'])
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
                        if(row['country'] not in self.excludedCountriesList):
                            self.excludedCountriesList.append(row['country'])
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
                    self.EvHdPg2updateRowOfObsWidgets(widgetsLst, ri, row, nWidgetsPerRow, activeTextColor, inactiveTextColor)
                    ri+=1
                    widgetID+=nWidgetsPerRow
                    if(ri>=self.nRows):
                        break
                    row=obsDf.iloc[ri]
                    if (thisCountry not in row['country']) :
                        bSameCountry=False
            elif(ci==2):  # stationID
                bSameStation=True  # multiple rows may be affected
                self.nRows=len(obsDf)
                thisStation=obsDf.at[(ri) ,  ('stationID')]
                #includeStationIdx=widgetID+2
                while((bSameStation) and (ri<self.nRows)):
                    if(bChkBxIsSelected):
                        try:
                            self.excludedStationsList.remove(row['stationID'])
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
                        if(row['stationID'] not in self.excludedStationsList):
                            self.excludedStationsList.append(row['stationID'])
                        obsDf.at[(ri) ,  ('selected')] =False
                        row.iloc[0]=False
                        obsDf.at[(ri) ,  ('includeStation')] =False
                        row.iloc[14]=False # 'includeStation'
                        widgetsLst[widgetID].configure(text_color=inactiveTextColor)
                    self.EvHdPg2updateRowOfObsWidgets(widgetsLst, ri, row, nWidgetsPerRow, activeTextColor, inactiveTextColor)
                    ri+=1
                    widgetID+=nWidgetsPerRow
                    #includeStationIdx+=nWidgetsPerRow
                    if(ri>=self.nRows):
                        break
                    row=obsDf.iloc[ri]
                    if (thisStation not in row['stationID']) :
                        bSameStation=False
 

    def EvHdPg2myOptionMenuEvent(self, gridID, widgetsLst,  obsDf, sSamplingHeights, nWidgetsPerRow, activeTextColor, inactiveTextColor):
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


    def EvHdPg2updateRowOfObsWidgets(self, widgetsLst, rowidx, row, nWidgetsPerRow, activeTextColor, inactiveTextColor):
        ''' toggle active/inactiveTextColor and verify the ChkBox states all the widgets belonging to one observational data 
            set corresponding to a single line on the GUI.
            We only modify the state parameters of existing widgets.
        '''
        ri=rowidx
        #nWidgetsPerRow=5
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
        bs.stakeOutSpacesAndFonts(self.root, nCols, nRows, USE_TKINTER,  sLongestTxt="Start date (00:00h):")
        # this gives us self.root.colWidth self.root.rowHeight, self.myFontFamily, self.fontHeight, self.fsNORMAL & friends
        xPadding=self.root.xPadding
        yPadding=self.root.yPadding
        # Dimensions of the window
        appWidth, appHeight = self.root.appWidth, self.root.appHeight
        # action if the gui window is closed by the user (==canceled)
        self.guiPg1TpLv.protocol("WM_DELETE_WINDOW", self.closeTopLv)
        # set the size of the gui window before showing it
        if(USE_TKINTER):
            #self.root.xoffset=int(0.5*1920)
            self.root.geometry(f"{appWidth}x{appHeight}+{self.root.xoffset}+0")   
            sufficentHeight=int(((nRows+1)*self.root.fontHeight*1.88)+0.5)
            if(sufficentHeight < appHeight):
                appHeight=sufficentHeight
            self.guiPg1TpLv.geometry(f"{appWidth}x{appHeight}")

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
        # Ranking of data records from CarbonPortal : populate the Pg1ObsFileRankingBoxTxt
        rankingList=self.ymlContents['observations'][self.tracer]['file']['ranking']
        self.Pg1ObsFileRankingBoxTxt=""
        rank=4
        for sEntry in  rankingList:
            self.Pg1ObsFileRankingBoxTxt+=str(rank)+': '+sEntry+'\n'
            rank=rank-1
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
        # Filename of local obs data file
        self.ObsFileLocationEntryVar = ge.guiStringVar(value=self.ymlContents['observations'][self.tracer]['file']['path'])
        # initial values --  chose what categories to adjust (checkboxes)
        self.LandVegCkbVar = ge.guiBooleanVar(value=True)
        self.FossilCkbVar = ge.guiBooleanVar(value=False)
        self.OceanCkbVar = ge.guiBooleanVar(value=False)

        # ====================================================================
        # Create all widgets of the first GUI page  -- part of lumiaGuiApp (toplevel window)
        # ====================================================================
        self.createAllPg1Widgets()
            
        # ====================================================================
        # Place all widgets onto the first GUI page  -- part of lumiaGuiApp (toplevel window)
        # ====================================================================
        self.placeAllPg1WidgetsOnCanvas(nCols,  nRows,  xPadding,  yPadding)
        
        if(not USE_TKINTER):
            toolbar_widget = widgets.VBox()
            toolbar_widget.children = [
                self.guiPg1TpLv.Pg1TitleLabel, 
                self.guiPg1TpLv.Pg1CancelButton
            ]

    def createAllPg1Widgets(self):
        # ====================================================================
        # Create all widgets of the first GUI page  -- part of lumiaGuiApp (toplevel window)
        # ====================================================================
        # Row 0:  Title Label
        # ################################################################
        title="LUMIA  --  Configure your next LUMIA run"
        self.Pg1TitleLabel = ge.guiTxtLabel(self.guiPg1TpLv, title, fontName=self.root.myFontFamily, fontSize=self.root.fsGIGANTIC, style="bold")
        # Row 1:  Time interval Heading
        # ################################################################
        self.Pg1TimeHeaderLabel = ge.guiTxtLabel(self.guiPg1TpLv, text="Time interval",  fontName=self.root.myFontFamily,  fontSize=self.root.fsLARGE, anchor="w")
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
        self.Pg1displayBox = ge.guiTextBox(self.guiPg1TpLv, width=self.root.colWidth,  height=(4*self.root.rowHeight),  fontName=self.root.myFontFamily,  fontSize=self.root.fsSMALL, text_color="red") 
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
        self.Pg1TracerRadioButton2 = ge.guiRadioButton(self.guiPg1TpLv,
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
        self.Pg1ObsFileRankingTbxVar = ge.guiStringVar(value="ObsPack")
        self.Pg1ObsFileRankingBox = ge.guiTextBox(self.guiPg1TpLv, width=self.root.colWidth,  height=(2*self.root.rowHeight),  fontName=self.root.myFontFamily,  fontSize=self.root.fsSMALL)
        self.Pg1ObsFileRankingBox.insert('0.0', self.Pg1ObsFileRankingBoxTxt)  # insert at line 0 character 0
        # Label for local  obs data path
        self.Pg1ObsFileLocationLocalPathLabel = ge.guiTxtLabel(self.guiPg1TpLv, text="Local.file.with.obs.data:", width=self.root.colWidth,  
                        fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, anchor="w")
        # Row 11:  Obs data entries
        # ################################################################
        self.Pg1FileSelectButton = ge.guiButton(self.guiPg1TpLv, text="Select File",  command=self.EvHdPg1selectFile,  fontName=self.root.myFontFamily,  fontSize=self.root.fsLARGE) 
        ge.updateWidget(self.Pg1FileSelectButton,  value='gray1', bText_color=True)
        ge.updateWidget(self.Pg1FileSelectButton,  value='light goldenrod', bFg_color=True) # in CTk this is the main button color (not the text color)
        #       Ignore ChkBx
        self.Pg1ignoreWarningsCkb = ge.guiCheckBox(self.guiPg1TpLv,state=tk.DISABLED, text="Ignore Warnings", fontName=self.root.myFontFamily,  
                            fontSize=self.root.fsNORMAL, variable=self.bIgnoreWarningsCkbVar, onvalue=True, offvalue=False) # text_color='gray5',  text_color_disabled='gray70', 
        # Row 12 Local Obs data filename entry and Cancel Button
        # ################################################################
        self.Pg1ObsFileLocationLocalEntry = ge.guiDataEntry(self.guiPg1TpLv, textvariable=self.ObsFileLocationEntryVar, placeholder_text=self.ObsFileLocationEntryVar, width=self.root.colWidth)
        ge.updateWidget(self.Pg1ObsFileLocationLocalEntry,  value='lemon chiffon', bFg_color=True) # in CTk this is the main button color (not the text color)
        if(USE_TKINTER):
            # if textvariable is longer than entry box, i.e. the path spills over, it will be right-aligned, showing the end with the file name
            self.Pg1ObsFileLocationLocalEntry.xview_moveto(1)  
        self.Pg1CancelButton = ge.guiButton(self.guiPg1TpLv, text="Cancel",  command=self.closeTopLv,  fontName=self.root.myFontFamily,  fontSize=self.root.fsLARGE) 
        ge.updateWidget(self.Pg1CancelButton,  value='gray1', bText_color=True)
        ge.updateWidget(self.Pg1CancelButton,  value='DarkOrange1', bFg_color=True) # in CTk this is the main button color (not the text color)
        # Row 13  chose what categories to adjust (checkboxes) and 'Go to page 2' button
        # ################################################################
        self.Pg1TuningParamLabel = ge.guiTxtLabel(self.guiPg1TpLv, text="LUMIA may adjust:", width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        self.Pg1LandVegCkb = ge.guiCheckBox(self.guiPg1TpLv, text="Land/Vegetation", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, 
                             variable=self.LandVegCkbVar, onvalue=True, offvalue=False)
        self.Pg1FossilCkb = ge.guiCheckBox(self.guiPg1TpLv, text="Fossil (off)", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                            variable=self.FossilCkbVar, onvalue=True, offvalue=False)                             
        self.Pg1OceanCkb = ge.guiCheckBox(self.guiPg1TpLv, text="Ocean (off)", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                            variable=self.OceanCkbVar, onvalue=True, offvalue=False)                            
        self.Pg1GoButton = ge.guiButton(self.guiPg1TpLv, text="PROCEED", command=self.EvHdPg1GotoPage2, fontName=self.root.myFontFamily,  fontSize=self.root.fsLARGE)
        ge.updateWidget(self.Pg1GoButton,  value='gray1', bText_color=True)
        ge.updateWidget(self.Pg1GoButton,  value='green3', bFg_color=True) # in CTk this is the main button color (not the text color)


    def  placeAllPg1WidgetsOnCanvas(self, nCols,  nRows,  xPadding,  yPadding):
        # ====================================================================
        # Place all widgets onto the first GUI page  -- part of lumiaGuiApp (toplevel window)
        # ====================================================================
        # ################################################################
        # Row 0:  Title Label
        ge.guiPlaceWidget(self.Pg1TitleLabel, row=0, column=0, columnspan=nCols,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 1:  Time interval Heading
        # ################################################################
        ge.guiPlaceWidget(self.Pg1TimeHeaderLabel, row=1, column=0, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 2: Time interval Entry
        # ################################################################
        ge.guiPlaceWidget(self.Pg1TimeStartLabel, row=2, column=0, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1TimeStartEntry, row=2, column=1, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1TimeEndLabel, row=2, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1TimeEndEntry, row=2, column=3, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 3:  Geographical Region Heading & Message Box
        # ################################################################
        ge.guiPlaceWidget(self.Pg1LatitudesLabel, row=3, column=0, columnspan=3,padx=xPadding, pady=yPadding, sticky="ew")
        #    Text Box for messages, warnings, etc
        ge.guiPlaceWidget(self.Pg1displayBox, row=3, column=4, columnspan=1, rowspan=7, padx=xPadding, pady=yPadding, sticky="ew")
        # Row 4: Latitudes Entry Fields
        # ################################################################
        ge.guiPlaceWidget(self.Pg1LatitudeMinLabel, row=4, column=0, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1LatitudeMaxLabel, row=4, column=1, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1Latitude0Entry, row=4, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1Latitude1Entry, row=4, column=3, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 5: Longitudes Entry Fields
        # ################################################################
        ge.guiPlaceWidget(self.Pg1LongitudeMinLabel, row=5, column=0, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1LongitudeMaxLabel, row=5, column=1, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1Longitude0Entry, row=5, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1Longitude1Entry, row=5, column=3, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 6:  Tracer radiobutton (CO2/CH4)
        # ################################################################
        ge.guiPlaceWidget(self.Pg1TracerLabel, row=6, column=0, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1TracerRadioButton, row=6, column=1, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1TracerRadioButton2, row=6, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 7: Emissions data (a prioris): Heading and Land/Vegetation choice
        # ################################################################
        ge.guiPlaceWidget(self.Pg1EmissionsLabel, row=7, column=0, columnspan=2,padx=xPadding, pady=yPadding, sticky="ew")
        #       Emissions data (a prioris) : dyn.vegetation net exchange model
        ge.guiPlaceWidget(self.Pg1NeeLabel , row=7, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        #       Land/Vegetation Net Exchange combo box
        ge.guiPlaceWidget(self.Pg1LandNetExchangeOptionMenu, row=7, column=3, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 8: Emissions data (a prioris) continued: fossil+ocean
        # ################################################################
        ge.guiPlaceWidget(self.Pg1FossilEmisLabel, row=8, column=0, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1FossilEmisOptionMenu, row=8, column=1, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Ocean Net Exchange combo box (a prioris)
        ge.guiPlaceWidget(self.Pg1OceanNetExchangeLabel, row=8, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1OceanNetExchangeOptionMenu, row=8, column=3, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 9: Obs data location radiobutton
        # ################################################################
        ge.guiPlaceWidget(self.Pg1ObsFileLocationCPortalRadioButton, row=9, column=0, columnspan=2,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1ObsFileLocationLocalRadioButton, row=9, column=2, columnspan=2,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 10: Obs data entries
        # ################################################################
        # Ranking of data records from CarbonPortal
        ge.guiPlaceWidget(self.Pg1ObsFileRankingBox, row=10, column=0, columnspan=1, rowspan=3, padx=xPadding, pady=yPadding, sticky="ew")
        # Label for local  obs data path
        ge.guiPlaceWidget(self.Pg1ObsFileLocationLocalPathLabel, row=10, column=2, columnspan=2, padx=xPadding, pady=yPadding, sticky="ew")
        # Row 11:  Obs data entries
        # ################################################################
        ge.guiPlaceWidget(self.Pg1FileSelectButton, row=11, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        #       Ignore ChkBx
        ge.guiPlaceWidget(self.Pg1ignoreWarningsCkb, row=11, column=4, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 12 Cancel Button
        # ################################################################
        ge.guiPlaceWidget(self.Pg1ObsFileLocationLocalEntry, row=12, column=2, columnspan=2,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1CancelButton, row=12, column=4, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 13  chose what categories to adjust (checkboxes) and 'Go to page 2' button
        # ################################################################
        ge.guiPlaceWidget(self.Pg1TuningParamLabel, row=13, column=0, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1LandVegCkb, row=13, column=1, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1FossilCkb, row=13, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.Pg1OceanCkb, row=13, column=3, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 13 : Go to page 2 Button
        ge.guiPlaceWidget(self.Pg1GoButton, row=13, column=4, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")


    # ====================================================================
    #  general helper functions of the first GUI page -- part of lumiaGuiApp (toplevel window) 
    # ====================================================================
    # 
    # EvHdPg2checkGuiValues gathers all the selected options and text from the available entry
    # fields and boxes and then generates a prompt using them
    def checkGuiValues(self):
        bErrors=False
        sErrorMsg=""
        bWarnings=False
        sWarningsMsg=""

        # Get the Start- & End Dates/Times and do some sanity checks 
        # start format: '2018-01-01 00:00:00'    
        bTimeError=False
        strStartTime=self.Pg1TimeStartEntry.get()
        strEndTime=self.Pg1TimeEndEntry.get()
        if (len(strStartTime)<10):
            bTimeError=True
            sErrorMsg+='Invalid Start Date entered.\n'
        if (len(strEndTime)<10):
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
                sErrorMsg+='Invalid or corrupted Start Date entered. Please use the ISO format YYYY-MM-DD when entering dates.\n'
            try:
                tEnd=pd.Timestamp(strEndTime)
            except:
                bTimeError=True
                sErrorMsg+='Invalid or corrupted End Date entered. Please use the ISO format YYYY-MM-DD when entering dates.\n'
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
            try:
                self.ymlContents['run']['time']['start'] = tStart.strftime('%Y-%m-%d %H:%M:%S')
                self.ymlContents['run']['time']['end'] = tEnd.strftime('%Y-%m-%d %H:%M:%S')
            except:
                sErrorMsg+=f'Invalid or corrupted Start/End Date entered. cannot understand {tStart} - {tEnd}. Please use the ISO format YYYY-MM-DD when entering dates.\n'
                bErrors=True
        # Get the latitudes & langitudes of the selected region and do some sanity checks 
        bLatLonError=False
        Lat0=float(self.Pg1Latitude0Entry.get())
        Lat1=float(self.Pg1Latitude1Entry.get())
        Lon0=float(self.Pg1Longitude0Entry.get())
        Lon1=float(self.Pg1Longitude1Entry.get())

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
        # Get the name of the local obs data file. This is ignored, if (self.ymlContents['observations'][self.tracer]['file']['location'] == 'CARBONPORTAL')
        fname=self.Pg1ObsFileLocationLocalEntry.get()
        self.ymlContents['observations'][self.tracer]['file']['path'] = fname
            
        # Emissions data (a prioris)
        # Land/Vegetation Net Exchange combo box
        sLandVegModel=self.Pg1LandNetExchangeOptionMenu.get()  # 'VPRM', 'LPJ-GUESS'
        self.ymlContents['emissions'][self.tracer]['categories']['biosphere']['origin']=sLandVegModel
        # Fossil emissions combo box
        sFossilEmisDataset = self.Pg1FossilEmisOptionMenu.get()  # "EDGARv4_LATEST")
        self.ymlContents['emissions'][self.tracer]['categories']['fossil']['origin']=sFossilEmisDataset
        # Ocean Net Exchange combo box
        sOceanNetExchangeDataset = self.Pg1OceanNetExchangeOptionMenu.get()  # "mikaloff01"
        self.ymlContents['emissions'][self.tracer]['categories']['ocean']['origin']=sOceanNetExchangeDataset

        # Get the adjust Land/Fossil/Ocean checkBoxes and do some sanity checks 
        if((not self.Pg1LandVegCkb.get()) and  (not self.Pg1FossilCkb.get()) and (not self.Pg1OceanCkb.get())):
            bErrors=True
            sErrorMsg+="Error: At least one of Land, Fossil or Ocean needs to be adjustable, preferably Land.\n"
        elif(not self.Pg1LandVegCkb.get()):
            bWarnings=True
            sWarningsMsg+="Warning: It is usually a bad idea NOT to adjust the land/vegetation net exchange. Are you sure?!\n"
        if(self.Pg1FossilCkb.get()):
            bWarnings=True
            sWarningsMsg+="Warning: It is unusual wanting to adjust the fossil emissions in LUMIA. Are you sure?!\n"
        if(self.Pg1OceanCkb.get()):
            bWarnings=True
            sWarningsMsg+="Warning: It is unusual wanting to adjust the ocean net exchange in LUMIA. Are you sure?!\n"
        if(bErrors==False):
            self.ymlContents['optimize']['emissions'][self.tracer]['biosphere']['adjust'] = self.Pg1LandVegCkb.get()
            self.ymlContents['optimize']['emissions'][self.tracer]['fossil']['adjust'] = self.Pg1FossilCkb.get()
            self.ymlContents['optimize']['emissions'][self.tracer]['ocean']['adjust'] = self.Pg1OceanCkb.get()                
            
        # Deal with any errors or warnings
        return(bErrors, sErrorMsg, bWarnings, sWarningsMsg)

            
            
    # ====================================================================
    # body & brain of second GUI page  -- part of lumiaGuiApp (root window)
    # ====================================================================
        
    def runPage2(self):  # of lumiaGuiApp
        # ====================================================================
        # EventHandler for widgets of second GUI page  -- part of lumiaGuiApp (root window)
        # ====================================================================

        
        def applyFilterRules():
            bICOSonly=self.ymlContents['observations']['filters']['ICOSonly']
            self.getFilters()
            for ri, row in self.newDf.iterrows():
                #if ((row['stationID'] in 'ZSF') or (row['stationID'] in 'JAR') or (row['stationID'] in 'DEC')):
                    #id=row['stationID']
                    #print(f'stationID={id},  self.newDf-rowidx={ri}')
                row['altOk'] = (((float(row['altitude']) >= self.stationMinAlt) &
                                    (float(row['altitude']) <= self.stationMaxAlt) ) | (self.bUseStationAltitudeFilter==False)) 
                if(isinstance(row['samplingHeight'], list)):
                    sH=float(row['samplingHeight'][0])
                else:
                    sH=float(row['samplingHeight'])
                row['HghtOk'] = (((sH >= self.inletMinHght) &
                                                (sH <= self.inletMaxHght) ) | (self.bUseSamplingHeightFilter==False))
                bIcosOk=((bICOSonly==False)or(row['isICOS']==True))
                bSel=False
                countryInactive=row['country'] in self.excludedCountriesList
                stationInactive=row['stationID'] in self.excludedStationsList
                # The row['includeCountry'] flag tells us whether we draw it as we draw country names only once for all its data sets
                if((row['includeCountry']) and (row['includeStation']) and 
                    (not countryInactive) and (not stationInactive) and 
                    (row['altOk']) and (row['HghtOk']) and 
                    (int(row['dClass'])==4) and (bIcosOk)):
                    # if station and country are selected only then may we toggle the 'selected' entry to True
                    bSel=True
                bS=row['selected']
                row.iloc[0]=bSel  # row['selected'] is the same as row.iloc[0]
                self.newDf.at[(ri) ,  ('selected')] = bSel
                self.newDf.at[(ri) ,  ('altOk')] = row['altOk']
                self.newDf.at[(ri) ,  ('HghtOk')] = row['HghtOk']
                if((bSel != bS) and (int(row['dClass'])==4)): 
                    self.EvHdPg2updateRowOfObsWidgets(widgetsLst, ri, row, self.nWidgetsPerRow, activeTextColor, inactiveTextColor)
                    
                    
        def EvHdPg2stationAltitudeFilterAction():
            stationMinAltCommonSense= -100 #m Dead Sea
            stationMaxAltCommonSense= 9000 #m Himalaya
            bStationFilterActive=self.FilterStationAltitudesCkb.get() 
            sErrorMsg=""
            bStationFilterError=False        
            if(bStationFilterActive):
                self.ymlContents['observations']['filters']['bStationAltitude']=True
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
                self.ymlContents['observations']['filters']['bStationAltitude']=False
            if(bStationFilterError):
                self.FilterStationAltitudesCkb.set(False)
                self.ymlContents['observations']['filters']['bStationAltitude']=False
            elif(self.ymlContents['observations']['filters']['bStationAltitude']):
                self.ymlContents['observations']['filters']['stationMaxAlt']=mxh
                self.ymlContents['observations']['filters']['stationMinAlt']=mnh
            self.applyFilterRules()                       
            
        def EvHdPg2stationSamplingHghtAction():
            inletMinHeightCommonSense = 0    # in meters
            inletMaxHeightCommonSense = 850 # in meters World's heighest buildings
            sErrorMsg=""
            bStationFilterError=False
            bSamplingHghtFilterActive=self.FilterSamplingHghtCkb.get()
            if(bSamplingHghtFilterActive):
                self.ymlContents['observations']['filters']['bSamplingHeight']=True
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
                self.ymlContents['observations']['filters']['bSamplingHeight']=False
            if(bStationFilterError):
                self.FilterSamplingHghtCkb.set(False)
                self.ymlContents['observations']['filters']['bSamplingHeight']=False
            elif(self.ymlContents['observations']['filters']['bSamplingHeight']):
                self.ymlContents['observations']['filters']['inletMaxHeight']=mxh
                self.ymlContents['observations']['filters']['inletMinHeight']=mnh
            self.applyFilterRules()
    
            
        def EvHdPg2GoBtnHit():
            sOutputPrfx=self.ymlContents[ 'run']['thisRun']['uniqueOutputPrefix']
            try:
                nObs=len(self.newDf)
                filtered = ((self.newDf['selected'] == True))
                dfq= self.newDf[filtered]
                nSelected=len(dfq)
                logger.info(f"There are {nObs} valid data sets in the selected geographical region ingoring multiple sampling heights.")
                logger.info(f"Thereof {nSelected} are presently selected.")
            except:
                pass
            try:
                self.newDf.to_csv(self.sTmpPrfx+'_dbg_allObsInTimeSpaceSlab.csv', mode='w', sep=',')  
            except:
                logger.error(f"Fatal Error: Failed to write to file {self.sTmpPrfx}_dbg_allObsInTimeSpaceSlab.csv. Please check your write permissions and possibly disk space etc.")
                self.closeApp
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
                self.ymlContents['observations'][self.tracer]['file']['selectedObsData']=sOutputPrfx+"selected-ObsData-"+self.tracer+".csv"
                dfq.to_csv(self.ymlContents['observations'][self.tracer]['file']['selectedObsData'], mode='w', sep=',')
                dfPids=dfq['pid']
                self.ymlContents['observations'][self.tracer]['file']['selectedPIDs']=sOutputPrfx+"selected-PIDs-"+self.tracer+".csv"
                selectedPidLst = dfPids.iloc[1:].tolist()
                sFOut=self.ymlContents['observations'][self.tracer]['file']['selectedPIDs']
                # dfPids.to_csv(self.ymlContents['observations'][self.tracer]['file']['selectedPIDs'], mode='w', sep=',')
                with open( sFOut, 'w') as fp:
                    for item in selectedPidLst:
                        fp.write("%s\n" % item)
                
            except:
                logger.error(f"Fatal Error: Failed to write to file {sOutputPrfx}-selected-ObsData-{self.tracer}.csv. Please check your write permissions and possibly disk space etc.")
                self.closeApp
            try:
                nC=len(self.excludedCountriesList)
                nS=len(self.excludedStationsList)
                if(nS==0):
                    logger.info("No observation stations were rejected")
                else:
                    s=""
                    for element in self.excludedStationsList:
                        s=s+element+', '
                    logger.info(f"{nS} observation stations ({s[:-2]}) were rejected")
                hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'observations',  'filters',  'StationsExcluded'],   
                                                                            value=self.excludedStationsList, bNewValue=True)
                if(nC==0):
                    logger.info("No countries were rejected")
                else:
                    s=""
                    for element in self.excludedCountriesList:
                        s=s+element+', '
                    logger.info(f"{nC} countries ({s[:-2]}) were rejected")
                hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'observations',  'filters',  'CountriesExcluded'],   
                                                                            value=self.excludedCountriesList, bNewValue=True)
            except:
                pass
            # Save  all details of the configuration and the version of the software used:
            hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'observations',  'filters',  'ICOSonly'],   
                                                                        value=self.ymlContents['observations']['filters']['ICOSonly'], bNewValue=True)
            
            # sOutputPrfx=self.ymlContents[ 'run']['thisRun']['uniqueOutputPrefix']
            self.ymlContents['observations'][self.tracer]['file']['discoverData']=False # lumiaGUI has already hunted down and documented all user obsData selections
            try:
                with open(self.ymlFile, 'w') as outFile:
                    yaml.dump(self.ymlContents, outFile)
            except:
                logger.error(f"Fatal Error: Failed to write to text file {self.ymlFile} in local run directory. Please check your write permissions and possibly disk space etc.")
                self.closeApp
                return
            self.closeApp(bWriteStop=False)
            sCmd="touch LumiaGui.go"
            hk.runSysCmd(sCmd)
            logger.info("Done. LumiaGui completed successfully. Config and Log file written.")
            # self.bPleaseCloseTheGui.set(True)
            global LOOP2_ACTIVE
            LOOP2_ACTIVE = False


        # ====================================================================
        # body & brain of second GUI page  -- part of lumiaGuiApp (root window)
        # ====================================================================
        if(os.path.isfile("LumiaGui.stop")):
            sys.exit(-1)
        # The obsData from the Carbon Portal is already known at this stage. This was done before the toplevel window was closed

        # Plan the layout of the GUI - get screen dimensions, choose a reasonable font size for it, xPadding, etc.
        nCols=11 # sum of labels and entry fields per row
        nRows=32 #5+len(self.newDf) # number of rows in the GUI - not so important - window is scrollable
        bs.stakeOutSpacesAndFonts(self.root, nCols, nRows, USE_TKINTER,  sLongestTxt="Obsdata Rankin",  maxWidth=True)
        # this gives us self.root.colWidth self.root.rowHeight, self.myFontFamily, self.fontHeight, self.fsNORMAL & friends
        xPadding=self.root.xPadding
        yPadding=self.root.yPadding
        activeTextColor='gray10'
        inactiveTextColor='gray50'
        # Dimensions of the window
        appWidth, appHeight = self.root.appWidth, self.root.appHeight
        # action if the gui window is closed by the user (==canceled)
        # self.guiPg1TpLv.protocol("WM_DELETE_WINDOW", self.closeTopLv)
        # set the size of the gui window before showing it
        widgetsLst = [] # list to hold dynamically created widgets that are created for each dataset found.
        if(USE_TKINTER):
            #self.root.xoffset=int(0.5*1920)
            self.root.geometry(f"{appWidth}x{appHeight}+{self.root.xoffset}+0")   
            sufficentHeight=int(((nRows+1)*self.root.fontHeight*1.88)+0.5)
            if(sufficentHeight < appHeight):
                appHeight=sufficentHeight
            self.root.geometry(f"{appWidth}x{appHeight}")
            # Now we venture to make the root scrollable....
            #main_frame = tk.Frame(root)
            rootFrame = tk.Frame(self.root, bg="cadet blue")
            rootFrame.grid(sticky='news')
            
            
        #self.deiconify()
            

        # ====================================================================
        # variables needed for the widgets of the second GUI page  -- part of lumiaGuiApp (root window)
        # ====================================================================
        rankingList=self.ymlContents['observations'][self.tracer]['file']['ranking']
        self.ObsFileRankingTbxVar = ge.guiStringVar(value="ObsPack")
        # ObservationsFileLocation
        self.iObservationsFileLocation= ge.guiIntVar(value=1) # Read observations from local file
        if ('CARBONPORTAL' in self.ymlContents['observations'][self.tracer]['file']['location']):
            self.iObservationsFileLocation.set(2) # Read observations from CarbonPortal
        self.ObsLv1CkbVar = ge.guiBooleanVar(value=True)
        self.ObsNRTCkbVar = ge.guiBooleanVar(value=True)
        self.ObsOtherCkbVar = ge.guiBooleanVar(value=True)
        self.getFilters()
        self.FilterStationAltitudesCkbVar = ge.guiBooleanVar(value=self.ymlContents['observations']['filters']['bStationAltitude'])
        self.sStationMinAlt=ge.guiStringVar(value=f'{self.stationMinAlt}')
        self.sStationMaxAlt=ge.guiStringVar(value=f'{self.stationMaxAlt}')
        self.FilterSamplingHghtCkbVar = ge.guiBooleanVar(value=self.ymlContents['observations']['filters']['bSamplingHeight'])
        self.sInletMinHght=ge.guiStringVar(value=f'{self.inletMinHght}')
        self.sInletMaxHght=ge.guiStringVar(value=f'{self.inletMaxHght}')
        self.isICOSRadioButtonVar = ge.guiIntVar(value=1)
        if (self.ymlContents['observations']['filters']['ICOSonly']==True):
            self.isICOSRadioButtonVar.set(2)
        
        # ====================================================================
        # Creation of all widgets of second GUI page  -- part of lumiaGuiApp (root window)
        # ====================================================================
        # Define all widgets needed. 
        # Row 0:  Title Label
        # ################################################################
        title="LUMIA  --  Refine your selections among the data discovered"
        self.Pg2TitleLabel = ge.guiTxtLabel(rootFrame, title, fontName=self.root.myFontFamily, fontSize=self.root.fsGIGANTIC, style="bold")
        # Row 1-4:  Header part with pre-selctions
        # ################################################################
        self.RankingLabel = ge.guiTxtLabel(rootFrame, text="Obsdata Ranking", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        # Ranking for Observation Files
        self.ObsFileRankingBox = ge.guiTextBox(rootFrame,  width=self.root.colWidth,  height=(2.4*self.root.rowHeight+self.root.vSpacer),  fontName=self.root.myFontFamily,  fontSize=self.root.fsSMALL)
        txt=""
        rank=4
        for sEntry in  rankingList:
            txt+=str(rank)+': '+sEntry+'\n'
            rank=rank-1
        self.ObsFileRankingBox.insert('0.0', txt)  # insert at line 0 character 0
        # Col2
        #  ###################################################################
        self.ObsLv1Ckb = ge.guiCheckBox(rootFrame,
                            text="Level1", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                            variable=self.ObsLv1CkbVar,
                             onvalue=True, offvalue=False)                             
        self.ObsNRTCkb = ge.guiCheckBox(rootFrame,
                            text="NRT", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                            variable=self.ObsNRTCkbVar,
                             onvalue=True, offvalue=False)                             
        self.ObsOtherCkb = ge.guiCheckBox(rootFrame,
                            text="Other", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                            variable=self.ObsOtherCkbVar,
                             onvalue=True, offvalue=False)                             
        # Col 3    Filtering of station altitudes
        #  ##################################################
        self.FilterStationAltitudesCkb = ge.guiCheckBox(rootFrame,
                            text="Filter station altitudes", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                            variable=self.FilterStationAltitudesCkbVar,
                            onvalue=True, offvalue=False, command=EvHdPg2stationAltitudeFilterAction)
        self.minAltLabel = ge.guiTxtLabel(rootFrame,
                                   text="min alt:", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        self.maxAltLabel = ge.guiTxtLabel(rootFrame,
                                   text="max alt:", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        # min Altitude Entry
        self.stationMinAltEntry = ge.guiDataEntry(rootFrame,textvariable=self.sStationMinAlt,
                          placeholder_text=str(self.stationMinAlt), width=self.root.colWidth)
        # max Altitude Entry
        self.stationMaxAltEntry = ge.guiDataEntry(rootFrame,textvariable=self.sStationMaxAlt,
                          placeholder_text=str(self.stationMaxAlt), width=self.root.colWidth)
        # Col 5    -  sampling height filter
        # ################################################################
        # 
        self.FilterSamplingHghtCkb = ge.guiCheckBox(rootFrame,
                            text="Filter sampling heights", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                            variable=self.FilterSamplingHghtCkbVar,
                            onvalue=True, offvalue=False, command=EvHdPg2stationSamplingHghtAction)                             
        self.minHghtLabel = ge.guiTxtLabel(rootFrame,
                                   text="min alt:", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        self.maxHghtLabel = ge.guiTxtLabel(rootFrame,
                                   text="max alt:", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        # min inlet height
        self.inletMinHghtEntry = ge.guiDataEntry(rootFrame,textvariable=self.sInletMinHght,
                          placeholder_text=str(self.inletMinHght), width=self.root.colWidth)
        # max inlet height
        self.inletMaxHghtEntry = ge.guiDataEntry(rootFrame,textvariable=self.sInletMaxHght,
                          placeholder_text=str(self.inletMaxHght), width=self.root.colWidth)
        # Col7
        # ################################################################
        # 
        self.ICOSstationsLabel = ge.guiTxtLabel(rootFrame,
                                   text="ICOS stations", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        self.isICOSplusRadioButton = ge.guiRadioButton(rootFrame,
                                   text="Any station", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                                   variable=self.isICOSRadioButtonVar,  value=1,  command=self.EvHdPg2isICOSfilter)
        self.ICOSradioButton = ge.guiRadioButton(rootFrame,
                                   text="ICOS only", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, 
                                   variable=self.isICOSRadioButtonVar,  value=2,  command=self.EvHdPg2isICOSfilter)
        # Col_11
        # ################################################################
        # 
        # Cancel Button
        self.CancelButton = ctk.CTkButton(master=rootFrame, font=(self.root.myFontFamily, self.root.fsNORMAL), text="Cancel",
            fg_color='orange red', command=self.closeApp)
        # Col_12
        # ################################################################
        # GO! Button
        self.GoButton = ctk.CTkButton(rootFrame, font=(self.root.myFontFamily, self.root.fsNORMAL), 
                                        command=EvHdPg2GoBtnHit,  text="RUN")  # Note: expressions after command= cannot have parameters or they will be executed at initialisation which is unwanted
        #self.GoButton.configure(command= lambda: self.EvHdPg2GoBtnHit) 
        ge.updateWidget(self.GoButton,  value='gray1', bText_color=True)
        ge.updateWidget(self.GoButton,  value='green3', bFg_color=True) # in CTk this is the main button color (not the text color)
        # Row 4 title for individual entries
        # ################################################################
        # newColumnNames=['selected','country', 'stationID', 'altOk', 'altitude', 'HghtOk', 'samplingHeight', 'isICOS', 'latitude', 'longitude', 'dClass', 'dataSetLabel', 'pid', 'includeCountry', 'includeStation']
        myLabels=". Selected        Country         StationID       SamplingHeight    Stat.altitude    Network   Latitude Longitude  DataRanking DataDescription"
        self.ColLabels = ge.guiTxtLabel(rootFrame, anchor="w",
                                   text=myLabels, fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)

        # ====================================================================
        # Placement of all widgets of the second GUI page  -- part of lumiaGuiApp (root window)
        # ====================================================================
        ge.guiPlaceWidget(self.Pg2TitleLabel, row=0, column=0, columnspan=nCols,padx=xPadding, pady=yPadding, sticky="ew")
        self.RankingLabel.grid(row=1, column=0, columnspan=1, padx=xPadding, pady=yPadding, sticky="nw")
        self.ObsFileRankingBox.grid(row=2, column=0, columnspan=1, rowspan=2, padx=xPadding, pady=yPadding, sticky="nsew")
        # Col2
        #  ###################################################################
        self.ObsLv1Ckb.grid(row=1, column=1,
                          padx=xPadding, pady=yPadding,
                          sticky="nw")
        self.ObsNRTCkb.grid(row=2, column=1,
                          padx=xPadding, pady=yPadding,
                          sticky="nw")
        self.ObsOtherCkb.grid(row=3, column=1,
                          padx=xPadding, pady=yPadding,
                          sticky="nw")
        # Col 3    Filtering of station altitudes
        #  ##################################################
        self.FilterStationAltitudesCkb.grid(row=1, column=3,columnspan=2, 
                          padx=xPadding, pady=yPadding,
                          sticky="nw")
        self.minAltLabel.grid(row=2, column=3,
                                  columnspan=1, padx=xPadding, pady=yPadding,
                                  sticky="nw")
        self.maxAltLabel.grid(row=3, column=3,
                                  columnspan=1, padx=xPadding, pady=yPadding,
                                  sticky="nw")
        # min Altitude Entry
        self.stationMinAltEntry.grid(row=2, column=4,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="nw")
        # max Altitude Entry
        self.stationMaxAltEntry.grid(row=3, column=4,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="nw")
        # Col 5    -  sampling height filter
        # ################################################################
        # 
        self.FilterSamplingHghtCkb.grid(row=1, column=5,columnspan=2, 
                          padx=xPadding, pady=yPadding,
                          sticky="nw")
        self.minHghtLabel.grid(row=2, column=5,
                                  columnspan=1, padx=xPadding, pady=yPadding,
                                  sticky="nw")
        self.maxHghtLabel.grid(row=3, column=5,
                                  columnspan=1, padx=xPadding, pady=yPadding,
                                  sticky="nw")
        # min inlet height
        self.inletMinHghtEntry.grid(row=2, column=6,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="nw")
        # max inlet height
        self.inletMaxHghtEntry.grid(row=3, column=6,
                            columnspan=1, padx=xPadding,
                            pady=yPadding, sticky="nw")
        # Col7
        # ################################################################
        # 
        self.ICOSstationsLabel.grid(row=1, column=7,
                                  columnspan=1, padx=xPadding, pady=yPadding,
                                  sticky="nw")
        self.isICOSplusRadioButton.grid(row=2, column=7,
                            padx=xPadding, pady=yPadding,
                            sticky="nw")
        self.ICOSradioButton.grid(row=3, column=7,
                            padx=xPadding, pady=yPadding,
                            sticky="nw")
        # Col_11
        # ################################################################
        # 
        self.CancelButton.grid(row=2, column=10,
                                        columnspan=1, padx=xPadding,
                                        pady=yPadding, sticky="nw")
        # Col_12
        # ################################################################
        # GO! Button
        self.GoButton.grid(row=3, column=10,
                                        columnspan=1, padx=xPadding,
                                        pady=yPadding, sticky="nw")
        # Row 4 title for individual entries
        # ################################################################
        # newColumnNames=['selected','country', 'stationID', 'altOk', 'altitude', 'HghtOk', 'samplingHeight', 'isICOS', 'latitude', 'longitude', 'dClass', 'dataSetLabel', 'pid', 'includeCountry', 'includeStation']
        self.ColLabels.grid(row=4, column=0, columnspan=10, padx=2, pady=yPadding, sticky="nw")





        # ====================================================================
        # Create a scrollable Frame within the rootFrame of the second GUI page to receive the dynamically created 
        #             widgets from the obsDataSets  -- part of lumiaGuiApp (root window)
        # ====================================================================
        if(USE_TKINTER):
            # Create a scrollable frame onto which to place the many widgets that represent all valid observations found
            #  ##################################################################
            # Create a frame for the canvas with non-zero row&column weights
            rootFrameCanvas = tk.Frame(rootFrame)
            rootFrameCanvas.grid(row=5, column=0,  columnspan=11,  rowspan=20, pady=(5, 0), sticky='nw') #, columnspan=11,  rowspan=10
            rootFrameCanvas.grid_rowconfigure(0, weight=8)
            rootFrameCanvas.grid_columnconfigure(0, weight=8)
            cWidth = appWidth - xPadding
            cHeight = appHeight - (7*self.root.rowHeight) - (3*yPadding)
            cHeight = appHeight - (7*self.root.rowHeight) - (3*yPadding)
            # Add a scrollableCanvas in that frame
            scrollableCanvas = tk.Canvas(rootFrameCanvas, bg="cadet blue", width=cWidth, height=cHeight, borderwidth=0, highlightthickness=0)
            scrollableCanvas.grid(row=0, column=0,  columnspan=11,  rowspan=10, sticky="news")
            # Link a scrollbar to the scrollableCanvas
            vsb = tk.Scrollbar(rootFrameCanvas, orient="vertical", command=scrollableCanvas.yview)
            vsb.grid(row=0, column=1, sticky='ns')
            scrollableCanvas.configure(yscrollcommand=vsb.set)
            # Create a frame to contain the widgets for all obs data sets found following initial user criteria
            scrollableFrame4Widgets = tk.Frame(scrollableCanvas, bg="#82d0d2") #  slightly lighter than "cadet blue"
            scrollableCanvas.create_window((0, 0), window=scrollableFrame4Widgets, anchor='nw')
            # Update buttons frames idle tasks to let tkinter calculate buttons sizes
            scrollableFrame4Widgets.update_idletasks()
            # Set the scrollableCanvas scrolling region
            scrollableCanvas.config(scrollregion=scrollableCanvas.bbox("all"))


        # ====================================================================
        # Placement of all widgets of the second GUI page  -- part of lumiaGuiApp (root window)
        # ====================================================================
        # newColumnNames=['selected','country', 'stationID', 'altOk', 'altitude', 'HghtOk', 'samplingHeight', 'isICOS', 'latitude', 'longitude', 'dClass', 'dataSetLabel', 'pid', 'includeCountry', 'includeStation']
        sLastCountry=''
        sLastStation=''
        #def guiPg2createRowOfObsWidgets(gridList, rowindex, row):
            
        num = 0  # index for 
        for rowidx, row in self.newDf.iterrows(): 
            guiRow=rowidx # startRow+rowidx
            
            if((rowidx==0) or (row['country'] not in sLastCountry)):
                # when the widgets are created,  row['includeCountry'] is used to indicate whether a CheckBox
                # is painted for that row - only the first row of each country will have this
                # Once the CheckBox was created, row['includeCountry'] is used to track wether the Country has been
                # selected or deselected by the user. At start (in this subroutine) all Countries are selected.
                row['includeCountry'] = True
                sLastCountry=row['country'] 
            else:
                #self.newDf.at[(rowidx) ,  ('includeCountry')] = False
                row['includeCountry']=False

            if((rowidx==0) or (row['stationID'] not in sLastStation)):
                row['includeStation']=True
                sLastStation=row['stationID']
            else:
                #self.newDf.at[(rowidx) ,  ('includeStation')] = False
                row['includeStation']=False

            sSamplingHeights=[str(row['samplingHeight'][0])] 
            #if(rowidx==0):
            #    print(row)
            for element in row['samplingHeight']:
                sElement=str(element)
                if(not sElement in sSamplingHeights):
                    sSamplingHeights.append(sElement)
                    
            #guiPg2createRowOfObsWidgets(gridList, rowidx, row)                
            self.guiPg2createRowOfObsWidgets(scrollableFrame4Widgets, widgetsLst, num,  rowidx, row,guiRow, 
                                                    sSamplingHeights, activeTextColor, 
                                                    inactiveTextColor, self.root.fsNORMAL, xPadding, yPadding, self.root.fsSMALL, obsDf=self.newDf)
            # After drawing the initial widgets, all entries of station and country are set to true unless they are on an exclusion list
            countryInactive=row['country'] in self.excludedCountriesList
            stationInactive=row['stationID'] in self.excludedStationsList
            self.newDf.at[(rowidx) ,  ('includeCountry')] = (not countryInactive)
            self.newDf.at[(rowidx) ,  ('includeStation')] =(not stationInactive)
   
   

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


