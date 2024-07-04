#!/usr/bin/env python3

import os
import sys
import housekeeping as hk
import pandas as pd
import argparse
from datetime import datetime,  timedelta
#import time
import yaml
#import re
from pandas import to_datetime
from loguru import logger
#import _thread
from queryCarbonPortal import discoverObservationsOnCarbonPortal

global AVAIL_LAND_NETEX_DATA  # Land/vegetation net exchange model emissions that are supported
AVAIL_LAND_NETEX_DATA=["LPJ-GUESS","VPRM"]
global AVAIL_FOSSIL_EMISS_DATA      # FOSSIL Emissions data sets supported
AVAIL_FOSSIL_EMISS_DATA=["EDGARv4_LATEST","EDGARv4.3_BP2021_CO2_EU2_2023","EDGARv4.3_BP2021_CO2_EU2_2020","EDGARv4.3_BP2021_CO2_EU2_2019", "EDGARv4.3_BP2021_CO2_EU2_2018"]
global AVAIL_OCEAN_NETEX_DATA      # Ocean Net Exchange data sets supported
AVAIL_OCEAN_NETEX_DATA=["mikaloff01"]
        # Fossil emissions combo box.  Latest version at time of writing: https://meta.icos-cp.eu/collections/KUbfPUVytev3qR_guM_-ir5a  (2023)
        # https://meta.icos-cp.eu/collections/GP-qXikmV7VWgG4G2WxsM1v3 (2023) https://commons.datacite.org/doi.org/10.18160/RFJD-QV8J
        #  https://hdl.handle.net/11676/Ce5IHvebT9YED1KkzfIlRwDi (2022) https://doi.org/10.18160/RFJD-QV8J EDGARv4.3 and BP statistics 2023
        # https://hdl.handle.net/11676/6i-nHIO0ynQARO3AIbnxa-83  EDGARv4.3_BP2021_CO2_EU2_2020.nc  
        # https://hdl.handle.net/11676/ZU0G9vak8AOz-GprC0uY-HPM  EDGARv4.3_BP2021_CO2_EU2_2019.nc
        # https://hdl.handle.net/11676/De0ogQ4l6hAsrgUwgjAoGDoy EDGARv4.3_BP2021_CO2_EU2_2018.nc

APPISACTIVE=True
USE_TKINTER=True
scriptName=sys.argv[0]
if('.ipynb' in scriptName[-6:]):
    USE_TKINTER=False
# For testing of ipywidgets uncomment the next line (even if not a notebook)

import boringStuff as bs



# =============================================================================
# Core functions with interesting tasks
# =============================================================================

def verifyYmlFile(ymlFile):
    '''
    Function verifyYmlFile

    @param ymlFile the name of the ymlFile provided as an (optional) commandline argument, else it is None
    @type TYPE string
    if ymlFile is given as a commandline argument, this function checks if that file exists.
    If no  ymlFile was specified, it either directly opens a file dialog box (tkinter) or paints a fileUpload button that the user needs to click 
    (Jupyter notebook or using ipywidgets), then the user needs to select an appropriate file before proceeding. This file is crucial to
    have before lumiaGUI can do anything meaningful.
    '''
    title='Open existing LUMIA configuration file:'
    filename=ymlFile
    if(ymlFile is None):
        filetypes='*.yml'
        if(USE_TKINTER):
            ymlFile = ge.guiFileDialog(filetypes=filetypes, description="Select yml file") 
        else:
            ymlFileSelectButton = ge.guiFileDialog(filetypes=filetypes, description="Select yml file") 
            ymlFileSelectButton
            display(ymlFileSelectButton)
            ContinueButton = ge.guiButton('None', text="Continue after file selection",  command='',  width=240) 
            ContinueButton
            display(ContinueButton)
            ge.guiWidgetsThatWait4UserInput(watchedWidget=ContinueButton,watchedWidget2=None, title='',  myDescription="Cancel",  myDescription2="Cancel", width=240)
            rval=ymlFileSelectButton.value[0]
            # filename = ge.guiFileDialogDoAll(filetypes=filetypes, title=title)
            ymlFile=rval['name']
       #print(f'Name of initial Lumia config file={ymlFile}')
    if (ymlFile is None):
        logger.error("You need to provide an initial Lumia configuration .yml file. Try again when you have done your homework.")
        sys.exit(-3)
    return(ymlFile)
    ymlFile=filename
    logger.info(f'User selected Lumia configuration file is the ymlFile {ymlFile}')
    if (not os.path.isfile(ymlFile)):
        logger.error(f"Fatal error in LumiaGUI: User specified configuration file {ymlFile} does not exist. Abort.")
        sys.exit(-3)
    return(ymlFile)

def prepareCallToLumiaGUI(ymlFile, args): 
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
    '''

    # Do the housekeeping like documenting the current git commit version of this code, date, time, user, platform etc.
    thisScript='LumiaGUI'
    # scriptDirectory = os.path.dirname(os.path.abspath(sys.argv[0]))
    iVerbosityLv=args.verbosity
    ymlFile=verifyYmlFile(ymlFile)
    initialYmlFile=ymlFile
    (ymlFile, oldDiscoveredObservations)=hk.documentThisRun(initialYmlFile, thisScript,  args)  # from housekeepimg.py
    # Now the config.yml file has all the details for this particular run

    # remove old message files - these are only relevant if LumiaGUI is used in an automated workflow as they signal
    # success or failure of this step in the workflow
    if(os.path.isfile("LumiaGui.stop")):
        os.remove('LumiaGui.stop') # sCmd="rm LumiaGui.stop" #hk.runSysCmd(sCmd,  ignoreError=True)
    if(os.path.isfile("LumiaGui.go")):
        os.remove('LumiaGui.go') # sCmd="rm LumiaGui.go" #hk.runSysCmd(sCmd,  ignoreError=True)

    # ensure we have a display connected
    myDsp=os.environ.get('DISPLAY','')
    if (myDsp == ''):
        logger.warning('DISPLAY not listed in os.environ. On simple systems DISPLAY is usually :0.0  ...so I will give that one a shot. Proceeding with fingers crossed...')
        os.environ.__setitem__('DISPLAY', ':0.0')
    else:
        logger.debug(f'found Display {myDsp}')

    if not(USE_TKINTER):
        notify_output = wdg.Output()
        display(notify_output)
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
    
    root=None
    root = ge.LumiaGui() # is the ctk.CTk() root window
    lumiaGuiAppInst=lumiaGuiApp(root)  # the main GUI application (class)
    lumiaGuiAppInst.sLogCfgPath = sLogCfgPath  # TODO: check if this is obsolete now
    lumiaGuiAppInst.initialYmlFile=initialYmlFile # the initial Lumia configuration file from which we started in case it needs to be restored
    lumiaGuiAppInst.ymlFile = ymlFile  # the current Lumia configuration file after pre-processing with housekeeping.py
    lumiaGuiAppInst.fDiscoveredObservations=''
    lumiaGuiAppInst.ymlContents = ymlContents # the contents of the Lumia configuration file
    lumiaGuiAppInst.sOutputPrfx=ymlContents[ 'run']['thisRun']['uniqueOutputPrefix'] # where to write output
    lumiaGuiAppInst.sTmpPrfx=ymlContents[ 'run']['thisRun']['uniqueTmpPrefix'] 
    lumiaGuiAppInst.oldDiscoveredObservations=oldDiscoveredObservations
    lumiaGuiAppInst.iVerbosityLv=iVerbosityLv
    #lumiaGuiAppInst.dbgHelper()
    guiPg1TpLv=lumiaGuiAppInst.guiPage1AsTopLv( iVerbosityLv=iVerbosityLv)  # the first of 2 pages of the GUI implemented as a toplevel window (init)
        #lumiaGuiAppInst.runPage2  # execute the central method of lumiaGuiApp
    if(USE_TKINTER):
        root.mainloop()
        sys.exit(0)
    '''
    (bFirstGuiPageSuccessful, ymlContents)=LumiaGuiPage1(root, sLogCfgPath, ymlContents, ymlFile, bRefine=False, iVerbosityLv=1) 
    
    # Go hunt for the data
    
    # Present to the user the data sets found
    '''
    #while(APPISACTIVE):
    #    time.sleep(1)
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
        if(USE_TKINTER):
            self.label1 = tk.Label(self.root, text="App main window - hosting the second GUI page.")
            self.root.protocol("WM_DELETE_WINDOW", self.closeApp)
        #self.label1.pack()
    
 
    def closeTopLv(self, bWriteStop=True):  # of lumiaGuiApp
        if(USE_TKINTER):
            self.guiPg1TpLv.destroy()
        if(bWriteStop):
            # do this in closeApp(): bs.cleanUp(bWriteStop=bWriteStop,  ymlFile=self.ymlFile)
            # logger.info('lumiaGUI canceled by user.')
            self.closeApp(True)
        self.guiPg1TpLv=None
        if(USE_TKINTER):
            self.root.deiconify()
        self.runPage2()  
        return True

    def closeApp(self, bWriteStop=True):  # of lumiaGuiApp
        bs.cleanUp(self,  bWriteStop=bWriteStop)
        logger.info("Closing the GUI...")
        if(USE_TKINTER):
            self.root.destroy()
        if(bWriteStop):
            logger.info('LumiaGUI canceled by user. Closing LumiaGUI app.')
        else:
            # TODO: write the GO message to file
            sCmd=f'cp {self.ymlFile} {self.initialYmlFile}'
            hk.runSysCmd(sCmd)
            logger.info(f'LumiaGUI completed successfully. The updated Lumia config file has been written to: {self.ymlFile} and {self.initialYmlFile}')
        global APPISACTIVE
        APPISACTIVE=False
        sys.exit(0)
        

    # ====================================================================
    #  Event handler for widget events from first GUI page of lumiaGuiApp 
    # ====================================================================
    def EvHdPg1ExitWithSuccess(self):
        self.closeApp(bWriteStop=False)
        return True
                
    def EvHdPg1GotoPage2(self):
        bGo=False
        repeat=False
       #print('Entering EvHdPg1GotoPage2')
        (bErrors, sErrorMsg, bWarnings, sWarningsMsg) = self.checkGuiValues()
        ge.guiWipeTextBox(self.Pg1displayBox, protect=True) # delete all text
        if((bErrors) and (bWarnings)):
            ge.guiWriteIntoTextBox(self.Pg1displayBox, "Please fix the following errors:\n"+sErrorMsg+sWarningsMsg, protect=True)
        elif(bErrors):
            ge.guiWriteIntoTextBox(self.Pg1displayBox, "Please fix the following errors:\n"+sErrorMsg, protect=True)
        elif(bWarnings):
            ge.guiWriteIntoTextBox(self.Pg1displayBox, "Please consider carefully the following warnings:\n"+sWarningsMsg, protect=True)
            #val=ge.getWidgetValue(self.Pg1ignoreWarningsCkb)
            #print(f'ge.getWidgetValue(self.Pg1ignoreWarningsCkb) val={val}')
            if((ge.getVarValue(self.bIgnoreWarningsCkbVar)) or (True==ge.getWidgetValue(self.Pg1ignoreWarningsCkb))):
                bGo=True
               #print('Proceed in spite of warnings')
            #if(USE_TKINTER):
            #    self.Pg1ignoreWarningsCkb.configure(state=tk.NORMAL)
            ge.guiConfigureWdg(self, widget=self.Pg1ignoreWarningsCkb,  disabled=False)

        else:
            bGo=True
        #if(USE_TKINTER):
        #    self.Pg1displayBox.configure(state=tk.DISABLED)  # configure textbox to be read-only
        ge.guiConfigureWdg(self, widget=self.Pg1displayBox,  disabled=False)
        if(bGo):
            self.bUseCachedList=False
            if(self.bSuggestOldDiscoveredObservations):
                if(USE_TKINTER):
                    self.bUseCachedList = ge.guiAskyesno(title='Use previous list of obs data?',
                                message=self.ageOfExistingDiscoveredObservations)
                else:
                    #print('..suggest cached obs data...')
                    self.wdgGrid4 = wdg.GridspecLayout(n_rows=2, n_columns=3,  grid_gap="5px")
                    self.Pg1cachedObsDataLabel = ge.guiTxtLabel(self.guiPg1TpLv, text=self.ageOfExistingDiscoveredObservations,  width=600)
                    self.Pg1UseCachedButton = ge.guiButton(self.guiPg1TpLv, text="Use cached Discovered-obs-data",  width=240)
                    self.Pg1DontUseCachedButton = ge.guiButton(self.guiPg1TpLv, text="Hunt for latest data from the carbon portal",  width=300)
                    ge.guiPlaceWidget(self.wdgGrid4, self.Pg1cachedObsDataLabel, row=0,  column=0, columnspan=2, rowspan=1,  padx=10, pady=10, sticky='news')
                    ge.guiPlaceWidget(self.wdgGrid4, self.Pg1UseCachedButton, row=1, column=0, columnspan=1, rowspan=1,  padx=10, pady=10, sticky='news')
                    ge.guiPlaceWidget(self.wdgGrid4, self.Pg1DontUseCachedButton, row=1, column=1, columnspan=1, rowspan=1,  padx=10, pady=10, sticky='news')
                    self.wdgGrid4
                    display(self.wdgGrid4)
                    askUserUseCachedBtn=ge.guiWidgetsThatWait4UserInput(watchedWidget=self.Pg1UseCachedButton,watchedWidget2=self.Pg1DontUseCachedButton, 
                            title='',  myDescription="Use cached Discovered-obs-data",  myDescription2="Hunt for latest data from the carbon portal", width=300)
                    whichButton=askUserUseCachedBtn.selectedBtn
                    if(whichButton==1): 
                        self.bUseCachedList = True
                    else:
                        self.bUseCachedList = False
            else:
                logger.debug('There is no cached obs data at hand...')
            if(USE_TKINTER):
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
            self.obsLocation=self.ymlContents['observations'][self.tracer]['file']['location']
            if('CARBONPORTAL' in self.obsLocation):
                self.huntAndGatherObsData()
            self.closeTopLv(bWriteStop=False)
        else: #(bGo==False):
            repeat=True
            try:
                whichButton=ge.guiWidgetsThatWait4UserInput(watchedWidget=self.Pg1GoButton,watchedWidget2=self.Pg1CancelButton, title='',  myDescription="PROCEED",  myDescription2="Cancel", width=240)
            except:
                pass
        return(repeat)
            
    def EvHdPg1selectFile(self):
        filename = ge.guiFileDialog(filetypes=[("All", "*")]) 
        if ((filename is None) or (len(filename)<5)):
            return   # Canceled or no valid filename returned: Keep previous data and continue
        # update the file entry widget
        self.ObsFileLocationEntryVar = ge.guiStringVar(value=filename)
        if(USE_TKINTER): # TODO fix
            ge.updateWidget(self.Pg1ObsFileLocationLocalEntry,  value=self.ObsFileLocationEntryVar, bTextvar=True)
        # if textvariable is longer than its entry box, i.e. the path spills over, it will be right-aligned, showing the end with the file name
        if(USE_TKINTER):
            self.Pg1ObsFileLocationLocalEntry.xview_moveto(1)  
        return True

    def EvHdPg1SetObsFileLocation(self):
        if(USE_TKINTER): # tkinter returns the index (int) of the selected radiobutton
            if (ge.getVarValue(self.iObservationsFileLocation)==0):
               self.ymlContents['observations'][self.tracer]['file']['location'] = 'LOCAL'
            else:
                self.ymlContents['observations'][self.tracer]['file']['location'] = 'CARBONPORTAL'
        else: # ipywidgets returns the value (text string) of the selected radiobutton
            ObsFileLocation=self.Pg1ObsFileLocationRadioButtons.value
            #print(f'..ObsFileLocation={ObsFileLocation}')
            self.ymlContents['observations'][self.tracer]['file']['location'] =ObsFileLocation
        #s=self.ymlContents['observations'][self.tracer]['file']['location'] 
        #print(f'EvHdPg1SetObsFileLocation(): changed to {s}')
        return True

    def EvHdPg1SetTracer(self):
        self.obsLocation=self.ymlContents['observations'][self.tracer]['file']['location']
        myPath2FluxData1=self.ymlContents['emissions'][self.tracer]['path']
        sLandVegModel=self.ymlContents['emissions'][self.tracer]['categories']['biosphere']['origin']
        #sEmBiosphereLocation=self.ymlContents['emissions'][self.tracer]['location']['biosphere']
        #sEmFossilLocation=self.ymlContents['emissions'][self.tracer]['location']['fossil']
        #sEmOceanLocation=self.ymlContents['emissions'][self.tracer]['location']['ocean']
        #sAnthropEm=self.ymlContents['emissions'][self.tracer]['categories']['biosphere']['fossil']
        #sOceanNEE=self.ymlContents['emissions'][self.tracer]['categories']['biosphere']['ocean']
        if(USE_TKINTER): # tkinter returns the index (int) of the selected radiobutton
            if (ge.getVarValue(self.iTracerRbVal)==0):
               self.ymlContents['run']['tracers'] = 'co2'
               self.tracer='co2'
            else:
                self.ymlContents['run']['tracers'] = 'ch4'
                self.tracer='ch4'
        else: # ipywidgets returns the value (text string) of the selected radiobutton
            TracerRbVal=self.Pg1TracerRadioButton.value
            #print(f'..TracerRbVal={TracerRbVal}')
            self.ymlContents['run']['tracers'] = TracerRbVal
            self.tracer=TracerRbVal
        # create potentially missing keys that are expected to exist for the new tracer
        hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'observations', self.tracer, 'path'],   value=self.obsLocation, bNewValue=False)
        hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'emissions', self.tracer, 'path'],   value=myPath2FluxData1, bNewValue=False)
        hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'observations', self.tracer, 'file', 'location'],   value=self.obsLocation, bNewValue=False)
        hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'emissions', self.tracer, 'categories', 'biosphere', 'origin'],   value=sLandVegModel, bNewValue=False)
        hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'emissions', self.tracer, 'categories', 'fossil', 'origin'],   value='UNKOWN', bNewValue=False)
        hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'emissions', self.tracer, 'categories', 'ocean', 'origin'],   value='UNKOWN', bNewValue=False)
        hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'emissions', self.tracer, 'location', 'biosphere'],   value='UNKOWN', bNewValue=False)
        hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'emissions', self.tracer, 'location', 'fossil'],   value='UNKOWN', bNewValue=False)
        hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'emissions', self.tracer, 'location', 'ocean'],   value='UNKOWN', bNewValue=False)
        hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'optimize','emissions', self.tracer, 'biosphere', 'adjust'],   value=True, bNewValue=False)
        hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'optimize','emissions', self.tracer, 'fossil', 'adjust'],   value=False, bNewValue=False)
        hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'optimize','emissions', self.tracer, 'ocean', 'adjust'],   value=False, bNewValue=False)
        hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'observations', self.tracer, 'file', 'location'],   value='UNKOWN', bNewValue=False)
        self.obsLocation=self.ymlContents['observations'][self.tracer]['file']['location']
        return True

    def getFilters(self):
        self.bUseStationAltitudeFilter=self.ymlContents['observations']['filters']['bStationAltitude']
        self.bUseSamplingHeightFilter=self.ymlContents['observations']['filters']['bSamplingHeight']
        self.bICOSonly=self.ymlContents['observations']['filters']['ICOSonly']
        self.stationMinAlt = self.ymlContents['observations']['filters']['stationMinAlt']     # in meters amsl
        self.stationMaxAlt = self.ymlContents['observations']['filters']['stationMaxAlt']  # in meters amsl
        self.samplingMinHght = self.ymlContents['observations']['filters']['inletMinHeight']     # in meters amsl
        self.samplingMaxHght = self.ymlContents['observations']['filters']['inletMaxHeight']  # in meters amsl
        self.stationMinAltVar=ge.guiStringVar(value=str(self.stationMinAlt))
        self.stationMaxAltVar=ge.guiStringVar(value=str(self.stationMaxAlt))
        self.samplingMinHghtVar=ge.guiStringVar(value=str(self.samplingMinHght))
        self.samplingMaxHghtVar=ge.guiStringVar(value=str(self.samplingMaxHght))
        return True

    def check4recentDiscoveredObservations(self, ymlContents):
        # Do we have a DiscoveredObservations.csv file from a previous run that a user could use?
        # This saves a fair bit of time if say you only want to do minor changes or check the settings.
        # However, because hk.documentThisRun() has already be run, so the value of sOutputPrfx
        # may already have changed. This means, we need to construct the likely name and location from the
        #         selectedObsData=ymlContents['observations'][tracer]['file']['selectedObsData']
        # key. For this we need the value of [tracer]. And then from something like 
        # ./output/LumiaGUI-2024-02-22T02_16/LumiaGUI-2024-02-22T02_16-selected-ObsData-co2.csv
        # we need to construct
        # ./output/LumiaGUI-2024-02-22T02_16/LumiaGUI-2024-02-22T02_16-DiscoveredObservations.csv
        self.haveDiscoveredObs=False
        self.obsLocation=self.ymlContents['observations'][self.tracer]['file']['location']
        if (('CARBONPORTAL' in self.obsLocation) and (os.path.isfile(self.oldDiscoveredObservations))):
            self.oldLat0=ymlContents['run']['region']['lat0']  # 33.0
            self.oldLat1=ymlContents['run']['region']['lat1']   #73.0
            self.oldLon0=ymlContents['run']['region']['lon0']  # -15.0
            self.oldLon1=ymlContents['run']['region']['lon1']   #35.0
            sStart=self.ymlContents['run']['time']['start']    # should be a string like start: '2018-01-01 00:00:00'
            sEnd=self.ymlContents['run']['time']['end']
            sStart=bs.removeQuotesFromString(sStart) # only in case a user puts quotes around any dates in the yml config file
            sEnd=bs.removeQuotesFromString(sEnd)
            self.oldpdTimeStart = to_datetime(sStart[:19], format="%Y-%m-%d %H:%M:%S")
            self.oldpdTimeStart=self.oldpdTimeStart.tz_localize('UTC')
            self.oldpdTimeEnd = to_datetime(sEnd[:19], format="%Y-%m-%d %H:%M:%S")
            self.oldpdTimeEnd=self.oldpdTimeEnd.tz_localize('UTC')
            self.ageOfExistingDiscoveredObservations='Do you want to use the cached discovered observations from UNKNOWN time ago?'
            try:
                # Extract the creation time of the existing oldDiscoveredObservations from its filename:
                baseName=os.path.basename(self.oldDiscoveredObservations)
                tracer=hk.getTracer(self.ymlContents['run']['tracers'])
                if(tracer not in baseName[-8:-4]):
                    return(False)
                # has format LumiaGUI-2024-02-26T12_36-DiscoveredObservations-co2.csv
                tidx=baseName.index('20')
                tStamp=baseName[tidx:tidx+16]
                tStampDatetime = to_datetime(tStamp, format="%Y-%m-%dT%H_%M")
                currentTime = datetime.now()
                dT=currentTime - tStampDatetime
                sdT=f'{dT}'
                sdT=sdT.replace(':','h',1)
                sdTcleaned = sdT.split(':', 1)[0] # # 0 days 7h2m
                self.ageOfExistingDiscoveredObservations=f'Do you want to use the cached discovered observations from \n{sdTcleaned}m ago?'
                self.haveDiscoveredObs=True
            except:
                self.haveDiscoveredObs=False
        return True
            

    def huntAndGatherObsData(self):
        # prowl through the carbon portal for any matching data sets in accordance with the choices from the first gui page.
        sStart=self.ymlContents['run']['time']['start']    # should be a string like start: '2018-01-01 00:00:00'
        sEnd=self.ymlContents['run']['time']['end']
        sStart=bs.removeQuotesFromString(sStart)
        sEnd=bs.removeQuotesFromString(sEnd)
        pdTimeStart = to_datetime(sStart[:19], format="%Y-%m-%d %H:%M:%S")
        pdTimeStart=pdTimeStart.tz_localize('UTC')
        pdTimeEnd = to_datetime(sEnd[:19], format="%Y-%m-%d %H:%M:%S")
        pdTimeEnd=pdTimeEnd.tz_localize('UTC')
        timeStep=self.ymlContents['run']['time']['timestep']
        
        # discoverObservationsOnCarbonPortal()
        tracer=hk.getTracer(self.ymlContents['run']['tracers'])
        if(self.bUseCachedList):
            # re-use the existing DiscoveredObservations.csv file, i.e. do not hunt for available data on the carbon portal
            self.badPidsLst=[] 
            # copy the existing and re-used DiscoveredObservations.csv file  over to the current output directory
            # Preserve its filename so the correct creation date is carried forward (relevant in case it may be re-used again and again)
            sOutputPrfx=self.ymlContents[ 'run']['thisRun']['uniqueOutputPrefix']
            sOutDir = os.path.dirname(sOutputPrfx)
            fname = os.path.basename(self.oldDiscoveredObservations)
            self.fDiscoveredObservations=os.path.join(sOutDir,  fname)
            sCmd=f'cp {self.oldDiscoveredObservations} {self.fDiscoveredObservations}'
            hk.runSysCmd(sCmd)
            hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'observations', tracer,  'file',  'dicoveredObsData'],   
                                                                        value=self.fDiscoveredObservations, bNewValue=True)
        else:
            (dobjLst, selectedDobjLst, dfObsDataInfo, self.fDiscoveredObservations, self.badPidsLst)=discoverObservationsOnCarbonPortal(self.tracer,   
                                pdTimeStart, pdTimeEnd, timeStep,  self.ymlContents,  sDataType=None, printProgress=True,  iVerbosityLv='INFO')
            hk.setKeyVal_Nested_CreateIfNecessary(self.ymlContents, [ 'observations', tracer,  'file',  'dicoveredObsData'],   
                                                                        value=self.fDiscoveredObservations, bNewValue=True)
        
        if (len(self.badPidsLst) > 0):
            sFOut=self.ymlContents['observations'][self.tracer]['file']['selectedPIDs']
            sFOut=sFOut[:-21]+'bad-PIDs.csv'
            with open( sFOut, 'w') as fp:
                for item in self.badPidsLst:
                    fp.write("%s\n" % item)
        # re-organise the self.fDiscoveredObservations dataframe,
        # It is presently sorted by country, station, dataRanking (dClass), productionTime and samplingHeight -- in that order
        #   columnNames=['pid', 'selected','stationID', 'country', 'IcosClass','latitude','longitude','altitude','samplingHeight','size', 
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
        #AllObsColumnNames=['pid', 'selected','stationID', 'country', 'IcosClass','latitude','longitude','altitude','samplingHeight','size', 
        #            'nRows','dataLevel','obsStart','obsStop','productionTime','accessUrl','fileName','dClass','dataSetLabel'] 
        self.getFilters()
        newColumnNames=['selected','country', 'stationID', 'altOk', 'altitude', 'HghtOk', 'samplingHeight', 'IcosClass', 'latitude', 'longitude', 'dClass', 'dataSetLabel', 'pid', 'includeCountry', 'includeStation']
        logger.debug(f'reading the listing of all discovered obs data sets from file {self.fDiscoveredObservations}')
        dfAllObs = pd.read_csv (self.fDiscoveredObservations)
        strIcos='IcosClass'
        for index, row in dfAllObs.iterrows():
            hLst=[row['samplingHeight'] ]
            pidLst=[ row['pid']]
            if (index==0):
                try:
                    sicos=row['IcosClass']
                except:  # if IcosClass column does not exist, use the old name for it
                    strIcos='isICOS'
            # bStationAltOk and bSamplHghtOk are helper variables that make filtering easier if requested at a later stage
            bStationAltOk = (((row['altitude'] >= self.stationMinAlt) &
                                (row['altitude'] <= self.stationMaxAlt) ) | (self.bUseStationAltitudeFilter==False)) 
            bSamplHghtOk = (((row['samplingHeight'] >= self.samplingMinHght) &
                                (row['samplingHeight'] <= self.samplingMaxHght) ) | (self.bUseSamplingHeightFilter==False))
            newRow=[bTrue,row['country'], row['stationID'], bStationAltOk, row['altitude'],  
                            bSamplHghtOk, hLst, row[strIcos], row['latitude'], row['longitude'], row['dClass'], row['dataSetLabel'],  pidLst, True,  True]
            
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
        #newDf.to_csv(self.sTmpPrfx+'_dbg_newDfObs.csv', mode='w', sep=',')  
        nObs=len(newDf)
        #filtered = ((newDf['selected'] == True))
        #dfq= newDf[filtered]
        #nSelected=len(dfq)
        logger.info(f"There are {nObs} valid data sets in the selected geographical region ingoring multiple sampling heights.")
        isDifferent=True
        self.nRows=0
        isSameStation = False
        strICOS='IcosClass'
        for index, row in newDf.iterrows():
            if((row['altOk']==False) or (row['HghtOk']==False)):
                newDf.at[(self.nRows) ,  ('selected')] = False
            if (index==0):
                try:
                    sicos=row['IcosClass']
                except:  # if IcosClass column does not exist, use the old name for it
                    strICOS='isICOS'
            icosStatus=row[strICOS] # [1,2,A,no] are possible values (strings) meaning ICOS affiliation class 1, 2 or Associated or no ICOS status
            bIcosOk=((self.bICOSonly==False)or( '1' in icosStatus)or( '2' in icosStatus)or('A' in icosStatus)or('a' in icosStatus))
            if(not bIcosOk):
                newDf.at[(self.nRows) ,  ('selected')] = False
            if(self.nRows>0):
                isSameStation = ((row['stationID'] in newDf['stationID'][self.nRows-1]))
            if(isSameStation):
                newDf.at[(self.nRows) ,  ('selected')] = False
            self.nRows+=1
        #newDf.to_csv(self.sTmpPrfx+'_dbg_selectedObs.csv', mode='w', sep=',')  

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
        return True

# Button click callbacks should take a single argument which will be the button widget that was clicked.  
# You don't have to use it, but the function must take that button as an argument.  
# Using a lambda that takes a single argument is also acceptable.
# button.on_click(lambda b: hello_world())

    # ====================================================================
    # EventHandler and helper functions for widgets of the second GUI page  -- part of lumiaGuiApp (root window)
    # ====================================================================
    def guiPg2createRowOfObsWidgets(self, scrollableFrame4Widgets, num, rowidx, row, guiRow, sSamplingHeights, 
                                                                    fsNORMAL, xPadding, yPadding, fsSMALL):
        ''' draw all the widgets belonging to one observational data set corresponding to a single line on the GUI '''
        # There are 5 active widgets per ObsData entry or row: selected,  country,  stationID,  samplingHeight,  and 
        # one textLabel for the remaining info on station altitude, network, lat, lon, dataRanking and dataDescription
        self.nWidgetsPerRow=5
        gridRow=[]
        self.activeTextColor='gray10'
        self.inactiveTextColor='gray50'
        sOptMenuTextColor='snow'
        sOptMenuInactiveTextColor='gray20'
        
        bSelected=row['selected']
        if(bSelected):
            sTextColor=self.activeTextColor
        else:
            sTextColor=self.inactiveTextColor
        countryInactive=row['country'] in self.excludedCountriesList
        stationInactive=row['stationID'] in self.excludedStationsList
        
        colidx=int(0)  # row['selected']
        gridID=int((100*rowidx)+colidx)  # encode row and column in the button's variable
        myWidgetVar= ge.guiBooleanVar(value=row['selected'])
        myWidgetSelect  = ge.GridCTkCheckBox(self, scrollableFrame4Widgets, gridID,  variable=myWidgetVar, command=self.EvHdPg2myCheckboxEvent, 
                                                text="",font=("Georgia", fsNORMAL), text_color=sTextColor, text_color_disabled=sTextColor, onvalue=True, offvalue=False) 
        ge.guiSetCheckBox(myWidgetSelect, bSelected)
        if((USE_TKINTER) and ((countryInactive) or (stationInactive))):
            ge.guiSetCheckBox(myWidgetSelect, False) # myWidgetSelect.deselect()
        ge.guiPlaceWidget(self.wdgGrid3, myWidgetSelect, row=guiRow, column=colidx, columnspan=1, rowspan=1, widgetID_LUT=self.widgetID_LUT, padx=xPadding, pady=yPadding, sticky='news')
        self.widgetsLst.append(myWidgetSelect) 

        gridRow.append(row['selected'])   
        
        colidx+=1 # =1  row['includeCountry']
        # ###################################################
        num+=1
        if((rowidx==0) or (row['includeCountry'] == True)):
            gridID=int((100*rowidx)+colidx)
            myWidgetVar= ge.guiBooleanVar(value=row['includeCountry'])
            try:
                myWidgetCountry  = ge.GridCTkCheckBox(self, scrollableFrame4Widgets, gridID, command=self.EvHdPg2myCheckboxEvent,  variable=myWidgetVar, 
                    text=row['country'],text_color=sTextColor, text_color_disabled=sTextColor, font=("Georgia", fsNORMAL), onvalue=True, offvalue=False)  
            except:
                logger.error(f'creation of widget myWidgetCountry with gridID {gridID} failed')
            #if(USE_TKINTER):
            #countryCkbEvtCommand=lambda widgetID=myWidgetCountry.widgetGridID : self.EvHdPg2myCheckboxEvent(myWidgetCountry.widgetGridID)
                #myWidgetCountry.configure(command=countryCkbEvtCommand) 
                # myWidgetCountry.configure(command=lambda widgetID=myWidgetCountry.widgetGridID : self.EvHdPg2myCheckboxEvent(myWidgetCountry.widgetGridID)) 
            #ge.guiConfigureWdg(self, widget=myWidgetCountry,  command=countryCkbEvtCommand)
            #if not (USE_TKINTER):
            #    myWidgetCountry.observe(myWidgetCountry.actOnCheckBoxChanges)
            if((USE_TKINTER) and (countryInactive)):
                ge.guiSetCheckBox(myWidgetCountry, False) # myWidgetCountry.deselect()
            else:
                ge.guiSetCheckBox(myWidgetCountry, True) # myWidgetCountry.select()
            ge.guiPlaceWidget(self.wdgGrid3, myWidgetCountry, row=guiRow, column=colidx, columnspan=1, rowspan=1, widgetID_LUT=self.widgetID_LUT, padx=xPadding, pady=yPadding, sticky='news')
            #myWidgetCountry.grid(row=guiRow, column=colidx, columnspan=1, padx=xPadding, pady=yPadding,sticky='news')
            self.widgetsLst.append(myWidgetCountry)
        else:
            self.widgetsLst.append(None)
        gridRow.append(row['country'])
        
        colidx+=1 # =2   row['includeStation']
        # ###################################################
        num+=1
        gridID=int((100*rowidx)+colidx)
        myWidgetVar= ge.guiBooleanVar(value=row['includeStation'])
        try:
            myWidgetStationid  = ge.GridCTkCheckBox(self, scrollableFrame4Widgets, gridID, command=self.EvHdPg2myCheckboxEvent, variable=myWidgetVar, 
                                                    text=row['stationID'],text_color=sTextColor, text_color_disabled=sTextColor, font=("Georgia", fsNORMAL), onvalue=True, offvalue=False) 
        except:
            logger.error(f'creation of widget myWidgetStationid with gridID {gridID} failed')
        #if(USE_TKINTER):
        #    myWidgetStationid.configure(command=lambda widgetID=myWidgetStationid.widgetGridID : self.EvHdPg2myCheckboxEvent(myWidgetStationid.widgetGridID)) 
            #if not (USE_TKINTER):
            #    myWidgetStationid.observe(myWidgetStationid.actOnCheckBoxChanges)
        if((USE_TKINTER) and (stationInactive)):
            ge.guiSetCheckBox(myWidgetStationid, False) # myWidgetStationid.deselect()
        else:
            ge.guiSetCheckBox(myWidgetStationid, True) # myWidgetStationid.select()
        ge.guiPlaceWidget(self.wdgGrid3, myWidgetStationid, row=guiRow, column=colidx, columnspan=1, rowspan=1, widgetID_LUT=self.widgetID_LUT, padx=xPadding, pady=yPadding, sticky='news')
        #myWidgetStationid.grid(row=guiRow, column=colidx, columnspan=1, padx=xPadding, pady=yPadding, sticky='news')
        self.widgetsLst.append(myWidgetStationid)
        gridRow.append(row['stationID'])

        colidx+=1 # =3  row['samplingHeight']
        # ###################################################
        if(bSelected):
            state="normal"
        else:
            state="disabled"
        gridID=int((100*rowidx)+colidx)
        myWidgetVar= ge.guiStringVar(value=str(row['samplingHeight'][0])) 
        #def __init__(self, parent,  root, myGridID, command=None, values=None,  *args, **kwargs):
        if(USE_TKINTER):
            if(bSelected):
                sTextColor=sOptMenuTextColor
            else:
                sTextColor=sOptMenuInactiveTextColor
            myWidgetSamplingHeight  = ge.oldGridCTkOptionMenu(scrollableFrame4Widgets, gridID, values=sSamplingHeights,
                                                            variable=myWidgetVar, text_color=sOptMenuTextColor, text_color_disabled=sOptMenuInactiveTextColor,
                                                            font=("Georgia", fsNORMAL), dropdown_font=("Georgia",  fsSMALL), state=state) 
            myWidgetSamplingHeight.configure(command=lambda widget=myWidgetSamplingHeight.widgetGridID : self.EvHdPg2myOptionMenuEvent(myWidgetSamplingHeight.widgetGridID, 
                                                                                            sSamplingHeights))  
            
        else:
            myWidgetSamplingHeight  = ge.GridCTkOptionMenu(self, scrollableFrame4Widgets, gridID, command=self.EvHdPg2myOptionMenuEvent, 
                                                            values=sSamplingHeights, variable=myWidgetVar, text_color=sTextColor, text_color_disabled=sTextColor,
                                                            font=("Georgia", fsNORMAL), dropdown_font=("Georgia",  fsSMALL)) 
        #if(USE_TKINTER):
        #    myWidgetSamplingHeight.configure(command=lambda widget=myWidgetSamplingHeight.widgetGridID : self.EvHdPg2myOptionMenuEvent(myWidgetSamplingHeight.widgetGridID, sSamplingHeights))  
        ge.guiPlaceWidget(self.wdgGrid3, myWidgetSamplingHeight, row=guiRow, column=colidx, columnspan=1, rowspan=1, widgetID_LUT=self.widgetID_LUT, padx=xPadding, pady=yPadding, sticky='news')
        #myWidgetSamplingHeight.grid(row=guiRow, column=colidx, columnspan=1, padx=xPadding, pady=yPadding, sticky='news')
        self.widgetsLst.append(myWidgetSamplingHeight)
        gridRow.append(row['samplingHeight'][0])
        if(bSelected):
            sTextColor=self.activeTextColor
        else:
            sTextColor=self.inactiveTextColor

        colidx+=1 # =4  Remaining Labels in one string
        # ###################################################
        gridID=int((100*rowidx)+colidx)
        affiliationICOS=row['IcosClass'] #"ICOS"
        affiliationICOS=str(affiliationICOS)
        #if('no' in row['IcosClass']):
        #    affiliationICOS="non-ICOS"
        sLat="{:.2f}".format(row['latitude'])
        sLon="{:.2f}".format(row['longitude'])
        myWidgetVar= bs.formatMyString(str(row['altitude']), 9, 'm')\
                                +bs.formatMyString(affiliationICOS, 12, '')\
                                +bs.formatMyString((sLat), 11, '°N')\
                                +bs.formatMyString((sLon), 10, '°E')\
                                +bs.formatMyString(str(row['dClass']), 8, '')\
                                +bs.formatMyString(str(row['dataSetLabel']), 34, '')
        myWidgetOtherLabels  = ge.GridCTkLabel(scrollableFrame4Widgets, gridID, text=myWidgetVar,text_color=sTextColor, text_color_disabled=sTextColor, 
                                                            font=("Georgia", fsNORMAL), textvariable="", justify="right", anchor="e") 
        nRemaining=self.nCols-4  # spread the remaining info over the remaining width of the canvas.                                                       
        ge.guiPlaceWidget(self.wdgGrid3, myWidgetOtherLabels, row=guiRow, column=colidx, columnspan=nRemaining, rowspan=1, widgetID_LUT=self.widgetID_LUT, padx=xPadding, pady=yPadding, sticky='news')
        #myWidgetOtherLabels.grid(row=guiRow, column=colidx, columnspan=6, padx=xPadding, pady=yPadding, sticky='nw')
        self.widgetsLst.append(myWidgetOtherLabels)
        gridRow.append(myWidgetVar)
        # ###################################################
        # guiPg2createRowOfObsWidgets() completed
        return True

    def EvHdPg2myCheckboxEvent(self, gridID=None,  wdgGridTxt='',  value=None,  description=''):
        #  NOTE: The callback from method guiElements_ipyWdg.GridCTkCheckBox.actOnCheckBoxChanges(change)
        #  to this method sends its widget's "self" but we need the "self" representing its parent, the lumiaGuiApp, as opposed to the checkboxe's identity.
        #print(f'EvHdPg2myCheckboxEvent L675: EvHdPg2myCheckboxEvent: gridID={gridID},  wdgGridTxt={wdgGridTxt},  description={description}')
        ri=int(0.01*gridID)  # row index for the widget on the grid
        ci=int(gridID-(100*ri))  # column index for the widget on the grid
        row=self.newDf.iloc[ri]
        widgetID=(ri*self.nWidgetsPerRow)+ci  # calculate the corresponding index to access the right widget in widgetsLst
        if(self.widgetsLst[widgetID] is not None):
            bChkBxIsSelected=ge.getWidgetValue(self.widgetsLst[widgetID])
            try:
                if(ci==0):
                    if(bChkBxIsSelected):
                        self.newDf.at[(ri) ,  ('selected')] =True
                        row=self.newDf.iloc[ri]
                        ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID], text='',  text_color='blue') # 'On'
                    else:
                        self.newDf.at[(ri) ,  ('selected')] =False
                        row=self.newDf.iloc[ri]
                        ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID], text='',  text_color='green') # 'Off'
                    self.EvHdPg2updateRowOfObsWidgets(ri, row)
                elif(ci==1):  # Country
                    bSameCountry=True  # multiple rows may be affected
                    self.nRows=len(self.newDf)
                    thisCountry=self.newDf.at[(ri) ,  ('country')]
                    while((bSameCountry) and (ri<self.nRows)):
                        if(bChkBxIsSelected):
                            # Set 'selected' to True only if the station, AltOk & HghtOk are presently selected AND dClass is the highest available, else not
                            try:
                                self.excludedCountriesList.remove(row['country'])
                            except:
                                pass
                            self.newDf.at[(ri) ,  ('includeCountry')] =True
                            if ((row['includeStation']) and (int(row['dClass'])==4) and (row['altOk']) and (row['HghtOk'])) :  #bIncludeStation) 
                                self.newDf.at[(ri) ,  ('selected')] =True
                                row=self.newDf.iloc[ri]
                                if(self.widgetsLst[widgetID] is not None):
                                    ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID],text_color=self.activeTextColor)
                            else:
                                self.newDf.at[(ri) ,  ('selected')] =False
                                row=self.newDf.iloc[ri]
                                if(self.widgetsLst[widgetID] is not None):
                                    ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID],text_color=self.inactiveTextColor)
                            self.newDf.iloc[ri, 13]=True
                            row=self.newDf.iloc[ri]
                        else:
                            # Remove country from list of excluded countries
                            c=row['country']
                            if(row['country'] not in self.excludedCountriesList):
                                self.excludedCountriesList.append(row['country'])
                            self.newDf.at[(ri) ,  ('includeCountry')] =False
                            self.newDf.at[(ri) ,  ('selected')] =False
                            row=self.newDf.iloc[ri]
                            if(self.widgetsLst[widgetID] is not None):
                                ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID],text_color=self.inactiveTextColor)
                        self.EvHdPg2updateRowOfObsWidgets(ri, row)
                        ri+=1
                        widgetID+=self.nWidgetsPerRow
                        if(ri>=self.nRows):
                            break
                        row=self.newDf.iloc[ri]
                        if (thisCountry not in row['country']) :
                            bSameCountry=False
                elif(ci==2):  # stationID
                    bSameStation=True  # multiple rows may be affected
                    self.nRows=len(self.newDf)
                    thisStation=self.newDf.at[(ri) ,  ('stationID')]
                    while((bSameStation) and (ri<self.nRows)):
                        if(bChkBxIsSelected):
                            try:
                                self.excludedStationsList.remove(row['stationID'])
                            except:
                                pass
                            ge.guiSetCheckBox(self.widgetsLst[widgetID], True) # self.widgetsLst[widgetID].select()
                            if((self.newDf.at[(ri) ,  ('includeCountry')]==True) and
                                (self.newDf.at[(ri) ,  ('altOk')]==True) and
                                (self.newDf.at[(ri) ,  ('HghtOk')]==True) and
                                (row['dClass']==4)):
                                self.newDf.at[(ri) ,  ('selected')] =True
                                row=self.newDf.iloc[ri]
                            else:
                                self.newDf.at[(ri) ,  ('selected')] =False
                                row=self.newDf.iloc[ri]
                            self.newDf.at[(ri) ,  ('includeStation')] =True
                            row=self.newDf.iloc[ri]
                            ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID],text_color=self.activeTextColor)
                        else:
                            ge.guiSetCheckBox(self.widgetsLst[widgetID], False) #  self.widgetsLst[widgetID].deselect()
                            if(row['stationID'] not in self.excludedStationsList):
                                self.excludedStationsList.append(row['stationID'])
                            self.newDf.at[(ri) ,  ('selected')] =False
                            self.newDf.at[(ri) ,  ('includeStation')] =False
                            row=self.newDf.iloc[ri]
                            ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID],text_color=self.inactiveTextColor)
                        self.EvHdPg2updateRowOfObsWidgets(ri, row)
                        ri+=1
                        widgetID+=self.nWidgetsPerRow
                        if(ri>=self.nRows):
                            break
                        row=self.newDf.iloc[ri]
                        if (thisStation not in row['stationID']) :
                            bSameStation=False
            except:
                logger.warning('EvHdPg2myCheckboxEvent().L836: event processing failed')
        else:
            logger.error(f'widgetID={widgetID} not found in self.widgetsLst[]')
        #print(f'self.excludedCountriesList={self.excludedCountriesList}')
        #print(f'self.excludedStationsList={self.excludedStationsList}')
        return True
 

    def EvHdPg2myOptionMenuEvent(self, gridID, sSamplingHeights,  selectedValue=None):
        ri=int(0.01*gridID)  # row index for the widget on the grid
        ci=int(gridID-(100*ri))  # column index for the widget on the grid
        widgetID=(ri*self.nWidgetsPerRow)+ci  # calculate the widgetID to access the right widget in widgetsLst
        #print(f"OptionMenuEventHandler: (ri={ri}, ci4={ci}, widgetID={widgetID}, gridID={gridID})")
        if(self.widgetsLst[widgetID] is not None):
           #print(f'EvHdPg2myOptionMenuEvent: widgetID={widgetID},  ri={ri},  ci={ci},  sSamplingHeights={sSamplingHeights}')
            if(ci==3):  # SamplingHeight
                if(selectedValue is None):
                    nsh=ge.getWidgetValue(self.widgetsLst[widgetID])
                else:
                    nsh=selectedValue
               #print(f'selected sampling height={nsh}')
                newSamplingHeight=nsh # a string var 
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
                            self.newDf.at[(ri) ,  ('samplingHeight')] =f
                            # TODO: keep PIDs in sync!
                            pids=self.newDf.at[(ri) ,  ('pid')]
                            bs.swapListElements(pids, 0, nPos)
                            self.newDf.at[(ri) ,  ('pid')]=pids
                           #print(f"new samplingHeights={self.newDf.at[(ri) ,  ('samplingHeight')]}")
                            break
                        nPos+=1
                except:
                    pass
        return True


    def EvHdPg2updateRowOfObsWidgets(self, rowidx, row):
        ''' toggle active/inactiveTextColor and verify the ChkBox states all the widgets belonging to one observational data 
            set corresponding to a single line on the GUI.
            We only modify the state parameters of existing widgets.
        '''
        ri=rowidx
        colidx=int(0)  # row['selected']
        # ###################################################
        widgetID=(ri*self.nWidgetsPerRow)+colidx  # calculate the corresponding index to access the right widget in widgetsLst
        b=False
        if(self.widgetsLst[widgetID] is not None):
            b=row['selected']
            if(b):
                ge.guiSetCheckBox(self.widgetsLst[widgetID], True) # self.widgetsLst[widgetID].select()
                ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID],text_color=self.activeTextColor)
                if(self.widgetsLst[widgetID+1] is not None):
                    if(ge.getWidgetValue(self.widgetsLst[widgetID+1]) == True): # this Country is not excluded
                        if(USE_TKINTER):
                            self.widgetsLst[widgetID+1].configure(text_color=self.activeTextColor)
                ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID+2],text_color=self.activeTextColor)
                ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID+3],text_color=self.activeTextColor)
                ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID+4],text_color=self.activeTextColor)
            else:
                ge.guiSetCheckBox(self.widgetsLst[widgetID], False) # self.widgetsLst[widgetID].deselect()
                ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID],text_color=self.inactiveTextColor)
                if(self.widgetsLst[widgetID+1] is not None):
                    ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID+1],text_color=self.inactiveTextColor)
                ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID+2],text_color=self.inactiveTextColor)
                ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID+3],text_color=self.inactiveTextColor)
                ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID+4],text_color=self.inactiveTextColor)
 
        colidx+=1  # row['includeCountry']
        # ###################################################
        widgetID+=1 # calculate the corresponding index to access the right widget in widgetsLst
        if(self.widgetsLst[widgetID] is not None):
            bIncludeCountry=row['includeCountry']
            bChkBxIsSelected=ge.getWidgetValue(self.widgetsLst[widgetID])
            if(bIncludeCountry != bChkBxIsSelected):  # update the widget selection status if necessary
                if(bIncludeCountry): # and  (row['dClass']==4) and  (row['altOk']==True) and (row['HghtOk']==True) ):
                    ge.guiSetCheckBox(self.widgetsLst[widgetID], True) #self.widgetsLst[widgetID].select()
                else:
                    ge.guiSetCheckBox(self.widgetsLst[widgetID], False) # self.widgetsLst[widgetID].deselect()
        
        colidx+=1  # row['includeStation'] 
        # ###################################################
        widgetID+=1 # calculate the corresponding index to access the right widget in widgetsLst
        if(self.widgetsLst[widgetID] is not None):
            try:  
                bIncludeStation=row['includeStation']
                bChkBxIsSelected=ge.getWidgetValue(self.widgetsLst[widgetID])
                if(bIncludeStation != bChkBxIsSelected):  # update the widget selection status if necessary
                    if(bIncludeStation) and  (row['dClass']==4) : #  and  (row['altOk']==True) and (row['HghtOk']==True) ):
                        ge.guiSetCheckBox(self.widgetsLst[widgetID], True) #self.widgetsLst[widgetID].select()
                    else:
                        ge.guiSetCheckBox(self.widgetsLst[widgetID], False) #self.widgetsLst[widgetID].deselect()
            except:
                pass
        colidx+=1  # row['samplingHeight'] 
        # ###################################################
        widgetID+=1 # calculate the corresponding index to access the right widget in widgetsLst
        if(USE_TKINTER):
            sOptMenuTextColor='snow'
            sOptMenuInactiveTextColor='gray20'
        if(self.widgetsLst[widgetID] is not None):
            try:  
                if(b):
                    if(USE_TKINTER):
                        ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID],text_color=sOptMenuTextColor, disabled=False)
                        sTextColor=self.activeTextColor
                    #ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID], disabled=False)
                else:
                    if(USE_TKINTER):
                        ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID],text_color=sOptMenuInactiveTextColor,disabled=True )
                        sTextColor=self.inactiveTextColor
                    #ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID],disabled=True )
            except:
                pass
        return True
        


    # ====================================================================
    # body & brain of first GUI page  -- part of lumiaGuiApp (toplevel window)
    # ====================================================================
        
    def guiPage1AsTopLv(self,  iVerbosityLv='INFO'):  # of lumiaGuiApp
        # Plan the layout of the GUI - get screen dimensions, choose a reasonable font size for it, xPadding, etc.
        nCols=5 # sum of labels and entry fields per row
        nRows=12 # number of rows in the GUI
        self.nCols=nCols
        self.nRows=nRows        
        
        if(USE_TKINTER):
            self.wdgGrid=None
            if(self.guiPg1TpLv is None):
                self.root.iconify()
                # self.guiPg1TpLv = tk.Toplevel(self.root,  bg="cadet blue")
                self.guiPg1TpLv = ge.guiToplevel(self.root,  bg="cadet blue")
        else:
            self.wdgGrid = wdg.GridspecLayout(n_rows=nRows, n_columns=nCols,  grid_gap="3px")
            if(self.guiPg1TpLv is None):
                self.guiPg1TpLv = ge.guiToplevel() # ,  bg="cadet blue"
                # self.guiPg1TpLv.configure(background='cadet blue')

        self.bSuggestOldDiscoveredObservations=False
        self.haveDiscoveredObs=False
        # Get the currently selected tracer from the yml config file
        self.tracer=hk.getTracer(self.ymlContents['run']['tracers'])
        self.obsLocation=self.ymlContents['observations'][self.tracer]['file']['location']
        if('CARBONPORTAL' in self.obsLocation):
            self.check4recentDiscoveredObservations(self.ymlContents)
        bs.stakeOutSpacesAndFonts(self.root, nCols, nRows, USE_TKINTER,  sLongestTxt="Start date (00:00h):")
        # this gives us self.root.colWidth self.root.rowHeight, self.myFontFamily, self.fontHeight, self.fsNORMAL & friends
        xPadding=self.root.xPadding
        yPadding=self.root.yPadding
        # Dimensions of the window
        appWidth, appHeight = self.root.appWidth, self.root.appHeight
        # action if the gui window is closed by the user (==canceled)
        if(USE_TKINTER):
            self.guiPg1TpLv.protocol("WM_DELETE_WINDOW", self.closeTopLv)
            # set the size of the gui window before showing it
            sufficentHeight=int(((nRows+1)*self.root.fontHeight*1.88)+0.5)
            if(sufficentHeight < appHeight):
                appHeight=sufficentHeight
            self.guiPg1TpLv.geometry(f"{appWidth+50}x{appHeight}")

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
        self.lonMin = float(-25.0)  # this is a property of Lumia and protects against use it was not designed or tested for
        self.lonMax = float(45.0)
        self.latMin = float(23.0)
        self.latMax = float(83.0)
        Lat0=self.ymlContents['run']['region']['lat0']  # 33.0
        Lat1=self.ymlContents['run']['region']['lat1']   #73.0
        self.sLat0=ge.guiStringVar(value=f'{Lat0:.3f}')
        self.sLat1=ge.guiStringVar(value=f'{Lat1:.3f}')
        Lon0=self.ymlContents['run']['region']['lon0']  # -15.0
        Lon1=self.ymlContents['run']['region']['lon1']   #35.0
        self.sLon0=ge.guiStringVar(value=f'{Lon0:.3f}')
        self.sLon1=ge.guiStringVar(value=f'{Lon1:.3f}')
        # Get the currently selected tracer from the yml config file
        self.tracer=hk.getTracer(self.ymlContents['run']['tracers'])
        # Set the Tracer radiobutton initial status in accordance with the (first) tracer extracted from the yml config file
        self.iTracerRbVal= ge.guiIntVar(value=0)
        if(('ch4' in self.tracer) or ('CH4' in self.tracer)):
            self.iTracerRbVal = ge.guiIntVar(value=1)
        else:
            self.iTracerRbVal = ge.guiIntVar(value=0)
        # Ranking of data records from CarbonPortal : populate the ObsFileRankingBoxTxt
        rankingList=self.ymlContents['observations'][self.tracer]['file']['ranking']
        self.ObsFileRankingBoxTxt=""
        rank=4
        for sEntry in  rankingList:
            self.ObsFileRankingBoxTxt+=str(rank)+': '+sEntry+'\n'
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
        self.iObservationsFileLocation= ge.guiIntVar(value=0)
        self.obsLocation=self.ymlContents['observations'][self.tracer]['file']['location']
        if('CARBONPORTAL' in self.obsLocation):
            self.iObservationsFileLocation = ge.guiIntVar(value=1)
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
        if not (USE_TKINTER):
            repeat=True
            while(repeat):
                whichButton=ge.guiWidgetsThatWait4UserInput(watchedWidget=self.Pg1GoButton,watchedWidget2=self.Pg1CancelButton, title='',  myDescription="PROCEED",  myDescription2="Cancel", width=240)
                #self.askUserGoOrCancel=ge.guiWidgetsThatWait4UserInput(watchedWidget=self.Pg1GoButton,watchedWidget2=self.Pg1CancelButton, title='',  myDescription="PROCEED",  myDescription2="Cancel", width=240)
                #whichButton=self.askUserGoOrCancel.selectedBtn
                # print(f'obtained whichButton={whichButton}')
                if(whichButton==2): 
                    self.closeTopLv(bWriteStop=True)  # Abort. Do not proceed to page 2                
                self.ObsFileLocation=self.Pg1ObsFileLocationRadioButtons.value
                
                #print(f'ObsFileLocation={ObsFileLocation}')
                #self.ymlContents['observations'][self.tracer]['file']['location'] =ObsFileLocation
                tracer=self.Pg1TracerRadioButton.value 
                #print(f'tracer={tracer}')
                self.ymlContents['run']['tracers'] = tracer
                repeat=self.EvHdPg1GotoPage2()
        return True

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
        # Label for message box 
        self.Pg1MsgBoxLabel = ge.guiTxtLabel(self.guiPg1TpLv, anchor="w",
                        text="Feedback (if any)", width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL)
        # Row 3:  Geographical Region Heading & Message Box
        # ################################################################
        self.Pg1LatitudesLabel = ge.guiTxtLabel(self.guiPg1TpLv, anchor="w", text="Geographical extent of the area modelled:",
                                                                    fontName=self.root.myFontFamily,  fontSize=self.root.fsLARGE)
        #    Text Box for messages, warnings, etc
        self.Pg1displayBox = ge.guiTextBox(self.guiPg1TpLv, width=self.root.colWidth,  height=(4*self.root.rowHeight),  fontName=self.root.myFontFamily,  fontSize=self.root.fsSMALL, text_color="red") 
        #if(USE_TKINTER):
        #    self.Pg1displayBox.configure(state=tk.DISABLED)  # configure textbox to be read-only
        ge.guiConfigureWdg(self, widget=self.Pg1displayBox,  disabled=True)
        # Row 4: Latitudes Entry Fields
        # ################################################################
        txt=f"Latitude (between {self.latMin} and {self.latMax} °North):" 
        self.Pg1LatitudeLabel = ge.guiTxtLabel(self.guiPg1TpLv,anchor="w", text=txt, width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, nCols=self.nCols,  colwidth=2)
        self.Pg1Latitude0Entry = ge.guiDataEntry(self.guiPg1TpLv,textvariable=self.sLat0, placeholder_text=self.sLat0, width=self.root.colWidth)
        self.Pg1Latitude1Entry = ge.guiDataEntry(self.guiPg1TpLv,textvariable=self.sLat1, placeholder_text=self.sLat1, width=self.root.colWidth)
        # Row 5: Longitudes Entry Fields
        # ################################################################
        txt=f"Longitude (between {self.lonMin} and {self.lonMax} °East):" 
        self.Pg1LongitudeLabel = ge.guiTxtLabel(self.guiPg1TpLv, text=txt, anchor="w", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, width=self.root.colWidth, nCols=self.nCols,  colwidth=2)
        self.Pg1Longitude0Entry = ge.guiDataEntry(self.guiPg1TpLv, textvariable=self.sLon0, placeholder_text=self.sLon0, width=self.root.colWidth)
        self.Pg1Longitude1Entry = ge.guiDataEntry(self.guiPg1TpLv, textvariable=self.sLon1, placeholder_text=self.sLon1, width=self.root.colWidth)
        # Row 6:  Label for Tracer radiobutton (CO2/CH4)
        # ################################################################
        self.Pg1TracerLabel = ge.guiTxtLabel(self.guiPg1TpLv,anchor="center", text="Tracer:   ", width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, nCols=self.nCols,  colwidth=1)
        self.Pg1EmissionsLabel = ge.guiTxtLabel(self.guiPg1TpLv, anchor="w", text="       Emissions data (a priori)",  fontName=self.root.myFontFamily,  fontSize=self.root.fsLARGE, nCols=self.nCols,  colwidth=2)
        self.Pg1TuningParamLabel = ge.guiTxtLabel(self.guiPg1TpLv, text="LUMIA may adjust:", width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, anchor="w", nCols=self.nCols,  colwidth=1)
        # Row 7: Emissions data (a prioris): Heading and Land/Vegetation choice
        # ################################################################
        if(USE_TKINTER):
            self.Pg1TracerRadioButton = ge.guiRadioButton(self.guiPg1TpLv,
                                       text="CO2", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, 
                                       variable=self.iTracerRbVal,  value=0, command=self.EvHdPg1SetTracer)
            self.Pg1TracerRadioButton2 = ge.guiRadioButton(self.guiPg1TpLv,
                                       text="CH4", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                                       variable=self.iTracerRbVal,  value=1, command=self.EvHdPg1SetTracer)
        else:
            self.Pg1TracerRadioButton = ge.guiRadioButton(parent=self,  options=['CO2', 'CH4'], preselected=self.iTracerRbVal, description='', 
                                                            command=self.EvHdPg1SetTracer)
        #       Emissions data (a prioris) : dyn.vegetation net exchange model
        self.Pg1FossilEmisLabel = ge.guiTxtLabel(self.guiPg1TpLv, text="Fossil emissions:", width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, anchor="e", nCols=self.nCols,  colwidth=1)
        self.Pg1FossilEmisOptionMenu = ge.guiOptionMenu(self.guiPg1TpLv, values=AVAIL_FOSSIL_EMISS_DATA, 
                                        variable=self.FossilEmisCkbVar, dropdown_fontName=self.root.myFontFamily, dropdown_fontSize=self.root.fsNORMAL)
        self.Pg1FossilCkb = ge.guiCheckBox(self.guiPg1TpLv, parent=self, text="Fossil (off)", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                            variable=self.FossilCkbVar, onvalue=True, offvalue=False)                             
        # Row 8: Emissions data (a prioris) continued: fossil+ocean
        # ################################################################
        self.Pg1NeeLabel = ge.guiTxtLabel(self.guiPg1TpLv, text="Land/Vegetation NEE:", width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, anchor="e", nCols=self.nCols,  colwidth=1)
        #       Land/Vegetation Net Exchange combo box
        self.Pg1LandNetExchangeOptionMenu = ge.guiOptionMenu(self.guiPg1TpLv,  values=AVAIL_LAND_NETEX_DATA, variable=self.LandNetExchangeModelCkbVar, 
                                                                                                        dropdown_fontName=self.root.myFontFamily, dropdown_fontSize=self.root.fsNORMAL)
        self.Pg1LandVegCkb = ge.guiCheckBox(self.guiPg1TpLv, parent=self, text="Land/Vegetation", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, 
                             variable=self.LandVegCkbVar, onvalue=True, offvalue=False)
        # Row 9: Obs data location radiobutton
        # ################################################################
        # Ocean Net Exchange combo box (a prioris)
        self.Pg1OceanNetExchangeLabel = ge.guiTxtLabel(self.guiPg1TpLv, text="Ocean net exchange:", width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, anchor="e", nCols=self.nCols,  colwidth=1)
        self.Pg1OceanNetExchangeOptionMenu = ge.guiOptionMenu(self.guiPg1TpLv, values=AVAIL_OCEAN_NETEX_DATA,
                                        variable=self.OceanNetExchangeCkbVar, dropdown_fontName=self.root.myFontFamily, dropdown_fontSize=self.root.fsNORMAL)
        self.Pg1OceanCkb = ge.guiCheckBox(self.guiPg1TpLv, parent=self,  text="Ocean (off)", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                            variable=self.OceanCkbVar, onvalue=True, offvalue=False)                            
        #       Ignore ChkBx
        self.Pg1ignoreWarningsCkb = ge.guiCheckBox(self.guiPg1TpLv,parent=self, disabled=True, text="Ignore Warnings", fontName=self.root.myFontFamily,  
                            fontSize=self.root.fsNORMAL, variable=self.bIgnoreWarningsCkbVar, onvalue=True, offvalue=False) # text_color='gray5',  text_color_disabled='gray70', 
        # Row 10: Obs data entries
        # ################################################################
        # Label for local  obs data path
        labelTxt=f'Observational \n{self.tracer} data'
        self.Pg1ObsDataSourceLabel = ge.guiTxtLabel(self.guiPg1TpLv, text=labelTxt, width=self.root.colWidth,  fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, nCols=self.nCols,  colwidth=1)
        if(USE_TKINTER):
            self.Pg1ObsFileLocationLocalRadioButton = ge.guiRadioButton(self.guiPg1TpLv,
                                       text="from local file", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                                       variable=self.iObservationsFileLocation,  value=0, command=self.EvHdPg1SetObsFileLocation)
            self.Pg1ObsFileLocationCPortalRadioButton = ge.guiRadioButton(self.guiPg1TpLv,
                                       text="from CarbonPortal", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, 
                                       variable=self.iObservationsFileLocation,  value=1, command=self.EvHdPg1SetObsFileLocation)
        else:
            self.obsLocation=self.ymlContents['observations'][self.tracer]['file']['location']
            if ('LOCAL' in self.obsLocation):
                preselected=0
            else:
                preselected=1
            self.Pg1ObsFileLocationRadioButtons = ge.guiRadioButton(parent=self,  options=['LOCAL','CARBONPORTAL' ],  preselected=preselected,
                                                                                                        command=self.EvHdPg1SetObsFileLocation, description='')
        self.Pg1FileSelectButton = ge.guiButton(self.guiPg1TpLv, text="Select local obsdata file",  command=self.EvHdPg1selectFile,  
                                                                        fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL) 
        if(USE_TKINTER): 
            ge.updateWidget(self.Pg1FileSelectButton,  value='gray1', bText_color=True)
            ge.updateWidget(self.Pg1FileSelectButton,  value='light goldenrod', bFg_color=True) # in CTk this is the main button color (not the text color)
        # Entry for local  obs data file
        self.Pg1ObsFileLocationLocalEntry = ge.guiDataEntry(self.guiPg1TpLv, textvariable=self.ObsFileLocationEntryVar, placeholder_text=self.ObsFileLocationEntryVar, 
                                                                                                width=self.root.colWidth)
        if(USE_TKINTER): 
            ge.updateWidget(self.Pg1ObsFileLocationLocalEntry,  value='lemon chiffon', bFg_color=True) # in CTk this is the main button color (not the text color)
        if(USE_TKINTER):
            # if textvariable is longer than entry box, i.e. the path spills over, it will be right-aligned, showing the end with the file name
            self.Pg1ObsFileLocationLocalEntry.xview_moveto(1)  
        # Row 11:  Local Obs data filename entry and Cancel Button
        # ################################################################
        # Cancel Button
        self.Pg1CancelButton = ge.guiButton(self.guiPg1TpLv, text="Cancel",  command=self.closeTopLv,  fontName=self.root.myFontFamily,  fontSize=self.root.fsLARGE) 
        if(USE_TKINTER): 
            ge.updateWidget(self.Pg1CancelButton,  value='gray1', bText_color=True)
            ge.updateWidget(self.Pg1CancelButton,  value='DarkOrange1', bFg_color=True) # in CTk this is the main button color (not the text color)
        #  Go button
        self.Pg1GoButton = ge.guiButton(self.guiPg1TpLv, text="PROCEED", command=self.EvHdPg1GotoPage2, fontName=self.root.myFontFamily,  fontSize=self.root.fsLARGE)
        if(USE_TKINTER): 
            ge.updateWidget(self.Pg1GoButton,  value='gray1', bText_color=True)
            ge.updateWidget(self.Pg1GoButton,  value='green3', bFg_color=True) # in CTk this is the main button color (not the text color)
        return True


    def  placeAllPg1WidgetsOnCanvas(self, nCols,  nRows,  xPadding,  yPadding):
        # ====================================================================
        # Place all widgets onto the first GUI page  -- part of lumiaGuiApp (toplevel window)
        # ====================================================================
        # ################################################################
        # Row 0:  Title Label
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1TitleLabel, row=0, column=0, columnspan=nCols,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 1:  Time interval Heading
        # ################################################################
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1TimeHeaderLabel, row=1, column=0, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 2: Time interval Entry
        # ################################################################
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1TimeStartLabel, row=2, column=0, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1TimeStartEntry, row=2, column=1, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1TimeEndLabel, row=2, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1TimeEndEntry, row=2, column=3, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Label for message box Pg1MsgBoxLabel
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1MsgBoxLabel, row=2, column=4, columnspan=1,  padx=xPadding, pady=yPadding, sticky="ew")
        # Row 3:  Geographical Region Heading & Message Box
        # ################################################################
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1LatitudesLabel, row=3, column=0, columnspan=3,padx=xPadding, pady=yPadding, sticky="ew")
        #    Text Box for messages, warnings, etc
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1displayBox, row=3, column=4, columnspan=1, rowspan=6, padx=xPadding, pady=yPadding, sticky="ew")
        # Row 4: Latitudes Entry Fields
        # ################################################################
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1LatitudeLabel, row=4, column=0, columnspan=2,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1Latitude0Entry, row=4, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1Latitude1Entry, row=4, column=3, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 5: Longitudes Entry Fields
        # ################################################################
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1LongitudeLabel, row=5, column=0, columnspan=2,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1Longitude0Entry, row=5, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1Longitude1Entry, row=5, column=3, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 6:  Tracer radiobutton (CO2/CH4)
        # ################################################################
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1TracerLabel, row=6, column=0, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1EmissionsLabel, row=6, column=1, columnspan=2,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1TuningParamLabel, row=6, column=3, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 7: Emissions data (a prioris): Heading and Land/Vegetation choice
        # ################################################################
        if(USE_TKINTER):
            rbRowspan=1
        else:
            rbRowspan=2                
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1TracerRadioButton, row=7, column=0, columnspan=1, rowspan=rbRowspan,padx=xPadding, pady=yPadding, sticky="")
        #       Emissions data (a prioris) : dyn.vegetation net exchange model
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1FossilEmisLabel, row=7, column=1, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1FossilEmisOptionMenu, row=7, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1FossilCkb, row=7, column=3, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        #       Land/Vegetation Net Exchange combo box
        # Row 8: Emissions data (a prioris) continued: fossil+ocean
        # ################################################################
        if(USE_TKINTER):
            ge.guiPlaceWidget(self.wdgGrid, self.Pg1TracerRadioButton2, row=8, column=0, columnspan=1,padx=xPadding, pady=yPadding, sticky="")
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1NeeLabel , row=8, column=1, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Ocean Net Exchange combo box (a prioris)
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1LandNetExchangeOptionMenu, row=8, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1LandVegCkb, row=8, column=3, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 9: Obs data location radiobutton
        # ################################################################
        # Label for local  obs data path
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1OceanNetExchangeLabel, row=9, column=1, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1OceanNetExchangeOptionMenu, row=9, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1OceanCkb, row=9, column=3, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        #     Ignore ChkBx
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1ignoreWarningsCkb, row=9, column=4, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 10: Obs data entries
        # ################################################################
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1ObsDataSourceLabel, row=10, column=0, columnspan=1,  rowspan=2, padx=xPadding, pady=yPadding, sticky="e")
        if(USE_TKINTER):
            ge.guiPlaceWidget(self.wdgGrid, self.Pg1ObsFileLocationLocalRadioButton, row=10, column=1, rowspan=rbRowspan, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        else:
            ge.guiPlaceWidget(self.wdgGrid, self.Pg1ObsFileLocationRadioButtons, row=10, column=1, rowspan=rbRowspan, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # File selector widget for local file
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1FileSelectButton, row=10, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Entry field for local obs data file
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1ObsFileLocationLocalEntry, row=10, column=3, columnspan=2,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 11:  Obs data entries
        # ################################################################
        # Row Cancel Button
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1CancelButton, row=11, column=3, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        #  GoButton 
        if(USE_TKINTER):
            ge.guiPlaceWidget(self.wdgGrid, self.Pg1ObsFileLocationCPortalRadioButton, row=11, column=1, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.Pg1GoButton, row=11, column=4, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # ################################################################
        # ################################################################
        if(not USE_TKINTER):
            self.wdgGrid
            display(self.wdgGrid)
        return True


    # ====================================================================
    #  general helper functions of the first GUI page -- part of lumiaGuiApp (toplevel window) 
    # ====================================================================
    # 
    # checkGuiValues gathers all the selected options and text from the available entry
    # fields and boxes and then generates a prompt using them
    def checkGuiValues(self):
       #print('Entered checkGuiValues')
        bErrors=False
        sErrorMsg=""
        bWarnings=False
        sWarningsMsg=""

        # Get the Start- & End Dates/Times and do some sanity checks 
        # start format: '2018-01-01 00:00:00'    
        bTimeError=False
        if(USE_TKINTER):
            strStartTime=ge.getWidgetValue(self.Pg1TimeStartEntry)
            strEndTime = ge.getWidgetValue(self.Pg1TimeEndEntry)
        else:
            strStartTime=self.Pg1TimeStartEntry.value
            strEndTime=self.Pg1TimeEndEntry.value
            #print(f'strStartTime={strStartTime},  strEndTime={strEndTime}')
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
        Lat0=float(ge.getWidgetValue(self.Pg1Latitude0Entry))
        Lat1=float(ge.getWidgetValue(self.Pg1Latitude1Entry))
        Lon0=float(ge.getWidgetValue(self.Pg1Longitude0Entry))
        Lon1=float(ge.getWidgetValue(self.Pg1Longitude1Entry))
        #print(f'Lat0={Lat0},  Lat1={Lat1},  Lon0={Lon0},  Lon1={Lon1}')
        if(USE_TKINTER): # TODO fix
            if(Lat0 < self.latMin):
                bLatLonError=True
                sErrorMsg+=f"Error: Region cannot extend below {self.latMin}°N.\n"
            if(Lat1 > self.latMax):
                bLatLonError=True
                sErrorMsg+=f"Error: Region cannot extend above {self.latMax}°N.\n"
            if(Lat0 > Lat1):
                bLatLonError=True
                sErrorMsg+=f"Maximum latitude {self.latMax}°N cannot be smaller than minimum latitude {self.latMin}°N.\n"
            if(Lon0 < self.lonMin):
                bLatLonError=True
                sErrorMsg+=f"Error: Region cannot extend west of {self.lonMin}°E.\n"
            if(Lon1 > self.lonMax):
                bLatLonError=True
                sErrorMsg+=f"Error: Region cannot extend east of {self.lonMax}°E.\n"
            if(Lon0 > Lon1):
                bLatLonError=True
                sErrorMsg+=f"Most eastern longitude {self.lonMax}°E cannot be west of the minimum longitude {self.lonMin}°E.\n"
            if(bLatLonError):
                bErrors=True
        if(not bErrors):
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
        if(USE_TKINTER):
            sObservationsFileLocation=ge.getVarValue(self.iObservationsFileLocation) # returns an index (int)
            if (sObservationsFileLocation==1):
                self.ymlContents['observations'][self.tracer]['file']['location'] = 'CARBONPORTAL'
            else:
                self.ymlContents['observations'][self.tracer]['file']['location'] = 'LOCAL'
        else:
            sObservationsFileLocation=ge.getWidgetValue(self.Pg1ObsFileLocationRadioButtons) # returns a string
            #print(f'sObservationsFileLocation={sObservationsFileLocation}')
            self.ymlContents['observations'][self.tracer]['file']['location'] = sObservationsFileLocation
        # Get the name of the local obs data file. This is ignored, if (self.ymlContents['observations'][self.tracer]['file']['location'] == 'CARBONPORTAL')
        
        fname=ge.getWidgetValue(self.Pg1ObsFileLocationLocalEntry)
        #print(f'self.Pg1ObsFileLocationLocalEntry.value={fname}')
        self.ymlContents['observations'][self.tracer]['file']['path'] = fname
            
        # Emissions data (a prioris)
        # Land/Vegetation Net Exchange combo box
        sLandVegModel= ge.getWidgetValue(self.Pg1LandNetExchangeOptionMenu) # 'VPRM', 'LPJ-GUESS'
       #print(f'sLandVegModel={sLandVegModel}')
        self.ymlContents['emissions'][self.tracer]['categories']['biosphere']['origin']=sLandVegModel
        # Fossil emissions combo box
        sFossilEmisDataset =ge.getWidgetValue(self.Pg1FossilEmisOptionMenu)   # "EDGARv4_LATEST")
       #print(f'sFossilEmisDataset={sFossilEmisDataset}')
        self.ymlContents['emissions'][self.tracer]['categories']['fossil']['origin']=sFossilEmisDataset
        # Ocean Net Exchange combo box
        
        sOceanNetExchangeDataset =  ge.getWidgetValue(self.Pg1OceanNetExchangeOptionMenu) # "mikaloff01"
        self.ymlContents['emissions'][self.tracer]['categories']['ocean']['origin']=sOceanNetExchangeDataset

        # Get the adjust Land/Fossil/Ocean checkBoxes and do some sanity checks 
        bAdjustLandVeg  =ge.getWidgetValue(self.Pg1LandVegCkb)
        bAdjustFossilCkb =ge.getWidgetValue(self.Pg1FossilCkb)
        bAdjustOceanCkb=ge.getWidgetValue(self.Pg1OceanCkb)
        if((not bAdjustLandVeg) and  (not bAdjustFossilCkb) and (not bAdjustOceanCkb)):
            bErrors=True
            sErrorMsg+="Error: At least one of Land, Fossil or Ocean needs to be adjustable, preferably Land.\n"
        elif(not bAdjustLandVeg):
            bWarnings=True
            sWarningsMsg+="Warning: It is usually a bad idea NOT to adjust the land/vegetation net exchange. Are you sure?!\n"
        if(bAdjustFossilCkb):
            bWarnings=True
            sWarningsMsg+="Warning: It is unusual wanting to adjust the fossil emissions in LUMIA. Are you sure?!\n"
        if(bAdjustOceanCkb):
            bWarnings=True
            sWarningsMsg+="Warning: It is unusual wanting to adjust the ocean net exchange in LUMIA. Are you sure?!\n"
        if(bErrors==False):
            self.ymlContents['optimize']['emissions'][self.tracer]['biosphere']['adjust'] = bAdjustLandVeg
            self.ymlContents['optimize']['emissions'][self.tracer]['fossil']['adjust'] = bAdjustFossilCkb
            self.ymlContents['optimize']['emissions'][self.tracer]['ocean']['adjust'] = bAdjustOceanCkb                
        
        # Can we suggest to the user to use the existing DiscoveredObservations.csv file?
        if(self.haveDiscoveredObs):
            self.bSuggestOldDiscoveredObservations=self.canUseRecentDiscoveredObservations()
        # Deal with any errors or warnings
        #print('exiting checkGuiValues()')
        return(bErrors, sErrorMsg, bWarnings, sWarningsMsg)

    def canUseRecentDiscoveredObservations(self):
        # Does the currently selected geographical region exceed the one used in self.oldDiscoveredObservations?
        if(     (self.ymlContents['run']['region']['lat0'] < self.oldLat0) 
            or (self.ymlContents['run']['region']['lat1'] > self.oldLat1)
            or (self.ymlContents['run']['region']['lon0'] < self.oldLon0)
            or (self.ymlContents['run']['region']['lon1'] > self.oldLon1)):
            return(False)
        # Does the currently selected start/end time exceed the ones used in self.oldDiscoveredObservations?
        sStart=self.ymlContents['run']['time']['start']    # should be a string like start: '2018-01-01 00:00:00'
        sEnd=self.ymlContents['run']['time']['end']
        pdTimeStart = to_datetime(sStart[:19], format="%Y-%m-%d %H:%M:%S")
        pdTimeStart=pdTimeStart.tz_localize('UTC')
        pdTimeEnd = to_datetime(sEnd[:19], format="%Y-%m-%d %H:%M:%S")
        pdTimeEnd=pdTimeEnd.tz_localize('UTC')
        if((pdTimeStart < self.oldpdTimeStart) or (pdTimeEnd > self.oldpdTimeEnd)):
            return(False)
        #self.oldDiscoveredObservations=fDiscoveredObservations    
        return(True)
            
            
        
    def applyFilterRulesPg2(self):
        #bICOSonly=self.ymlContents['observations']['filters']['ICOSonly']
        self.getFilters()
        # For each observational data set we check whether the station altitude, sampling height, or ICOSonly filters apply. 
        # We also check for manually rejected countries or stations
        for ri, row in self.newDf.iterrows():
            bSel=False 
            if (ri==0):
                try:
                    strICOS=row['IcosClass'] # if this works then the column IcosClass exists and is named as such
                    strICOS='IcosClass'
                except:
                    strICOS='isICOS' # if not, it is an old file where the column was called isICOS
            row['altOk'] = (((float(row['altitude']) >= self.stationMinAlt) &
                                (float(row['altitude']) <= self.stationMaxAlt) ) | (self.bUseStationAltitudeFilter==False)) 
            if(isinstance(row['samplingHeight'], list)):
                sH=float(row['samplingHeight'][0])
            else:
                sH=float(row['samplingHeight'])
            row['HghtOk'] = (((sH >= self.samplingMinHght) &
                                            (sH <= self.samplingMaxHght) ) | (self.bUseSamplingHeightFilter==False))
            countryInactive=row['country'] in self.excludedCountriesList
            stationInactive=row['stationID'] in self.excludedStationsList
            # The row['includeCountry'] flag tells us whether we draw it as we draw country names only once for all its data sets
            if((row['includeCountry']) and (row['includeStation']) and 
                (not countryInactive) and (not stationInactive) and 
                (row['altOk']) and (row['HghtOk']) and 
                (int(row['dClass'])==4) ):
                # if station and country are selected only then may we toggle the 'selected' entry to True
                bSel=True

            icosStatus=row[strICOS] # [1,2,A,no] are possible values (strings) meaning ICOS affiliation class 1, 2 or Associated or no ICOS status
            bIcosOk=((self.bICOSonly==False)or( '1' in icosStatus)or( '2' in icosStatus)or('A' in icosStatus)or('a' in icosStatus))
            if(not bIcosOk):
                bSel=False

            bS=row['selected']
            row.iloc[0]=bSel  # row['selected'] is the same as row.iloc[0]
            self.newDf.at[(ri) ,  ('selected')] = bSel
            self.newDf.at[(ri) ,  ('includeCountry')] = row['includeCountry']
            self.newDf.at[(ri) ,  ('includeStation')] = row['includeStation']
            self.newDf.at[(ri) ,  ('altOk')] = row['altOk']
            self.newDf.at[(ri) ,  ('HghtOk')] = row['HghtOk']
            if((bSel != bS) and (int(row['dClass'])==4)): 
                self.EvHdPg2updateRowOfObsWidgets(ri, row)
        return True
                
                

    def EvHdPg2stationAltitudeFilterAction(self):
        #print('Entering EvHdPg2stationAltitudeFilterAction')
        stationMinAltCommonSense= -100 #m Dead Sea
        stationMaxAltCommonSense= 9000 #m Himalaya
        bStationFilterActive= ge.getWidgetValue(self.FilterStationAltitudesCkb)
        sErrorMsg=""
        bStationFilterError=False        
        if(bStationFilterActive):
            self.ymlContents['observations']['filters']['bStationAltitude']=True
            mnh=int(ge.getWidgetValue(self.stationMinAltEntry))
            mxh=int(ge.getWidgetValue(self.stationMaxAltEntry))
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
        self.applyFilterRulesPg2()                       
        return True

        
    def EvHdPg2stationSamplingHghtAction(self, actualSelf=None, value=None):
        if((value is not None) and (actualSelf is not None)):
            #print(f'EvHdPg2stationSamplingHghtAction called with value={value}')
            self=actualSelf 
        inletMinHeightCommonSense = 0    # in meters
        inletMaxHeightCommonSense = 850 # in meters World's heighest buildings
        sErrorMsg=""
        bStationFilterError=False
        bSamplingHghtFilterActive=ge.getWidgetValue(self.FilterSamplingHghtCkb)
        if(bSamplingHghtFilterActive):
            self.ymlContents['observations']['filters']['bSamplingHeight']=True
            mnh=int(ge.getWidgetValue(self.samplingMinHghtEntry))
            mxh=int(ge.getWidgetValue(self.samplingMaxHghtEntry))
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
        self.applyFilterRulesPg2()
        return True
    
    def EvHdPg2isICOSfilter(self, actualSelf=None, value=None):
        #if((value is not None) and (actualSelf is not None)):
        #print(f'EvHdPg2isICOSfilter called with value={value}')
        #print(f'self={self}')   
        #print(f'actualSelf={actualSelf}')
        self.bICOSonly=True
        if(USE_TKINTER): # tkinter returns the index (int) of the selected radiobutton
            rbvariable=self.Pg2isICOSradioButton.cget("variable")
            isICOSrbValue = rbvariable.get()
            if(isICOSrbValue==0):
                self.bICOSonly=False
        else: # ipywidgets returns the value (string) of the selected radiobutton
            # self.Pg2isICOSradioButton = ge.guiRadioButton(['Any station', 'ICOS only'], description='is ICOS station')
            isICOSrbValue=self.Pg2isICOSradioButton.value
            if('Any station' in isICOSrbValue):
                self.bICOSonly=False
        #print(f'isICOSrbValue={isICOSrbValue}')
        self.ymlContents['observations']['filters']['ICOSonly']=self.bICOSonly
        self.applyFilterRulesPg2()                       
        return True


    def EvHdPg2GoBtnHit(self):
        sOutputPrfx=self.ymlContents[ 'run']['thisRun']['uniqueOutputPrefix']
        self.obsLocation=self.ymlContents['observations'][self.tracer]['file']['location']
        if('CARBONPORTAL' in self.obsLocation): # else there is no dfq dataframe
            try:
                nObs=len(self.newDf)
                filtered = ((self.newDf['selected'] == True))
                dfq= self.newDf[filtered]
                nSelected=len(dfq)
                logger.info(f"There are {nObs} valid data sets in the selected geographical region ingoring multiple sampling heights.")
                logger.info(f"Thereof {nSelected} are presently selected.")
            except:
                pass
            for index, row in dfq.iterrows():
                if(row['includeCountry']==False):
                    if not (row['country'] in self.excludedCountriesList):
                        self.excludedCountriesList.append(row['country'])
                if(row['includeStation']==False):
                    if not (row['stationID'] in self.excludedStationsList):
                        self.excludedStationsList.append(row['stationID'])
            #print(f'Writing to yaml File: self.excludedCountriesList={self.excludedCountriesList}')
            self.ymlContents['observations']['filters']['CountriesExcluded'] = self.excludedCountriesList
            #print(f'Writing to yaml File: self.excludedStationsList={self.excludedStationsList}')
            self.ymlContents['observations']['filters']['StationsExcluded'] = self.excludedStationsList
            try:
                #dfq.to_csv(sOutputPrfx+'_dbg_dfq_all.csv', mode='w', sep=',')
                dfq['pid2'] = dfq['pid'].apply(bs.grabFirstEntryFromList)
                dfq['samplingHeight2'] = dfq['samplingHeight'].apply(bs.grabFirstEntryFromList)
                #,selected,country,stationID,altOk,altitude,HghtOk,samplingHeight[Lst],isICOS,latitude,longitude,dClass,dataSetLabel,includeCountry,includeStation,pid[Lst],pid2,samplingHeight2
                dfq.drop(columns='pid',inplace=True) # drop columns with lists. These are replaced with single values from the first list entry
                #dfq.drop(columns='samplingHeight',inplace=True) # drop columns with lists. These are replaced with single values from the first list entry
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
            logger.error(f"Fatal Error: Failed to write to text file {self.ymlFile}. Please check your write permissions in the output directory and possibly disk space etc.")
            self.closeApp
            return
        self.closeApp(bWriteStop=False)
        sCmd="touch LumiaGui.go"
        hk.runSysCmd(sCmd)
        logger.info("Done. LumiaGui completed successfully. Config and Log file written.")
        # self.bPleaseCloseTheGui.set(True)
        global LOOP2_ACTIVE
        LOOP2_ACTIVE = False
        return True

    
    # ====================================================================
    # body & brain of second GUI page  -- part of lumiaGuiApp (root window)
    # ====================================================================
        
    def runPage2(self):  # of lumiaGuiApp
        # ====================================================================
        # EventHandler for widgets of second GUI page  -- part of lumiaGuiApp (root window)
        # ====================================================================

        # ====================================================================
        # body & brain of second GUI page  -- part of lumiaGuiApp (root window)
        # ====================================================================
        if(os.path.isfile("LumiaGui.stop")):
            sys.exit(-1)
        # The obsData from the Carbon Portal is already known at this stage. This was done before the toplevel window was closed

        # Plan the layout of the GUI - get screen dimensions, choose a reasonable font size for it, xPadding, etc.
        nCols=8 # sum of labels and entry fields per row
        nRows=32 #5+len(self.newDf) # number of rows in the GUI - not so important - window is scrollable
        self.nCols=nCols
        maxAspectRatio=16/9.0 # we need the full width of the sceen, so allow max screen width when checked against the max aspect ratio
        bs.stakeOutSpacesAndFonts(self.root, nCols, nRows, USE_TKINTER,  sLongestTxt="Obsdata Rankin",  maxAspectRatio=maxAspectRatio)
        # this gives us self.root.colWidth self.root.rowHeight, self.myFontFamily, self.fontHeight, self.fsNORMAL & friends
        xPadding=self.root.xPadding
        yPadding=self.root.yPadding
        self.activeTextColor='gray10'
        self.inactiveTextColor='gray50'
        # Dimensions of the window
        appWidth, appHeight = self.root.appWidth, self.root.appHeight
        # action if the gui window is closed by the user (==canceled)
        # self.guiPg1TpLv.protocol("WM_DELETE_WINDOW", self.closeTopLv)
        # set the size of the gui window before showing it
        self.widgetsLst = [] # list to hold dynamically created widgets that are created for each dataset found.
        if(USE_TKINTER):
            #self.root.xoffset=int(0.5*1920)
            self.root.geometry(f"{appWidth}x{appHeight}+{self.root.xoffset}+0")   
            #logger.debug(f'requested dimensions  GuiApp w={appWidth} h={appHeight}')
            #sufficentHeight=int(((nRows+1)*self.root.fontHeight*1.88)+0.5)
            #if(sufficentHeight < appHeight):
            #    appHeight=sufficentHeight
            # Now we venture to make the root scrollable....
            rootFrame = tk.Frame(self.root) #, width=cWidth)
            rootFrame.configure(background='cadet blue')  # 'sienna1'
            rootFrame.grid(sticky='news')
            self.wdgGrid = None
            self.wdgGrid3 = None
        else:
            rootFrame=ge.pseudoRootFrame()
            self.wdgGrid = wdg.GridspecLayout(n_rows=6, n_columns=self.nCols,  grid_gap="3px")
            out = wdg.Output()
            display(out)
        #self.deiconify()
            

        # ====================================================================
        # variables needed for the widgets of the second GUI page  -- part of lumiaGuiApp (root window)
        # ====================================================================
        self.Pg2title="LUMIA  --  Refine your selections among the data discovered"
        #rankingList=self.ymlContents['observations'][self.tracer]['file']['ranking']
        self.ObsFileRankingTbxVar = ge.guiStringVar(value="ObsPack")
        # ObservationsFileLocation
        self.tracer=hk.getTracer(self.ymlContents['run']['tracers'])
        self.iObservationsFileLocation= ge.guiIntVar(value=1) # Read observations from local file
        self.obsLocation=self.ymlContents['observations'][self.tracer]['file']['location']
        if ('CARBONPORTAL' in self.obsLocation):
            if(USE_TKINTER):
                self.iObservationsFileLocation.set(1) # Read observations from CarbonPortal
            else:
                self.iObservationsFileLocation=1 # Read observations from CarbonPortal
        self.ObsLv1CkbVar = ge.guiBooleanVar(value=True)
        self.ObsNRTCkbVar = ge.guiBooleanVar(value=True)
        self.ObsOtherCkbVar = ge.guiBooleanVar(value=True)
        self.getFilters()
        self.FilterStationAltitudesCkbVar = ge.guiBooleanVar(value=self.ymlContents['observations']['filters']['bStationAltitude'])
        self.FilterSamplingHghtCkbVar = ge.guiBooleanVar(value=self.ymlContents['observations']['filters']['bSamplingHeight'])
        self.isICOSRadioButtonVar = ge.guiIntVar(value=0)
        self.bICOSonly=False
        if (self.ymlContents['observations']['filters']['ICOSonly']==True):
            if(USE_TKINTER):
                self.isICOSRadioButtonVar.set(1)
            else:
                self.isICOSRadioButtonVar=1
            self.bICOSonly=True
        
        # ====================================================================
        # Creation of the static widgets of second GUI page  -- part of lumiaGuiApp (root window)
        # 1) Static widgets (top part)
        # ====================================================================
        self.createPg2staticWidgets(rootFrame)
        # ====================================================================
        # Placement of the static widgets of the second GUI page  -- part of lumiaGuiApp (root window)
        # 1) Static widgets (top part)
        # ====================================================================
        self.placePg2staticWidgetsOnCanvas(self.nCols,  nRows,  xPadding,  yPadding)
        if(not USE_TKINTER):
            self.wdgGrid
            display(self.wdgGrid)
            self.nCols=10
            self.wdgGrid3 = wdg.GridspecLayout(n_rows=128, n_columns=self.nCols,  grid_gap="3px")
        self.obsLocation=self.ymlContents['observations'][self.tracer]['file']['location']
        if('CARBONPORTAL' in self.obsLocation):
            # ====================================================================
            # Create a scrollable Frame within the rootFrame of the second GUI page to receive the dynamically created 
            #             widgets from the obsDataSets  -- part of lumiaGuiApp (root window)
            # ====================================================================
            if(USE_TKINTER):
                # Create a scrollable frame onto which to place the many widgets that represent all valid observations found
                #  ##################################################################
                # Create a frame for the canvas with non-zero row&column weights
                rootFrameCanvas = tk.Frame(rootFrame)
                rootFrameCanvas.configure(background='thistle1') # 'OliveDrab1')
                rootFrameCanvas.grid(row=5, column=0,  columnspan=11,  rowspan=20, pady=(5, 0), sticky='nw') #, columnspan=11,  rowspan=10
                rootFrameCanvas.grid_rowconfigure(0, weight=2) # the weight>0 effectively moves the scrollbar to the far right though there may be better ways to achieve this
                rootFrameCanvas.grid_columnconfigure(0, weight=2)
                cWidth = appWidth - xPadding
                cHeight = appHeight - (7*self.root.rowHeight) - (3*yPadding)
                cHeight = appHeight - (7*self.root.rowHeight) - (3*yPadding)
                logger.debug(f'requested dimensions for scrollableCanvas: w={cWidth} h={cHeight}. GuiApp w={self.root.appWidth} h={self.root.appHeight}')
                if (cWidth > self.root.appWidth):
                    cWidth = self.root.appWidth-1
                # Add a scrollableCanvas in that frame
                scrollableCanvas = tk.Canvas(rootFrameCanvas, width=cWidth, height=cHeight, borderwidth=0, highlightthickness=0)
                scrollableCanvas.configure(background='CadetBlue3')  # 'cadet blue')
                scrollableCanvas.grid(row=0, column=0,  columnspan=11,  rowspan=10, sticky="news")
                # Link a scrollbar to the scrollableCanvas
                vsb = tk.Scrollbar(rootFrameCanvas, orient="vertical", command=scrollableCanvas.yview)
                vsb.grid(row=0, column=1, sticky='ns')
                scrollableCanvas.configure(yscrollcommand=vsb.set)
                # Create a frame to contain the widgets for all obs data sets found following initial user criteria
                scrollableFrame4Widgets = tk.Frame(scrollableCanvas) #, bg="#82d0d2") #  slightly lighter than "cadet blue"
                scrollableFrame4Widgets.configure(background='#82d0d2')  # 'orchid1')
                scrollableCanvas.create_window((0, 0), window=scrollableFrame4Widgets, anchor='nw')
            else:
                scrollableFrame4Widgets = ge.pseudoRootFrame()
                rootFrameCanvas = ge.pseudoRootFrame()
    
            # newColumnNames=['selected','country', 'stationID', 'altOk', 'altitude', 'HghtOk', 'samplingHeight', 'IcosClass', 'latitude', 'longitude', 'dClass', 'dataSetLabel', 'pid', 'includeCountry', 'includeStation']
            sLastCountry=''
            sLastStation=''
            num = 0  # index for 
            self.widgetID_LUT={}  # wdgGrid3 assigns each widget a consecutive ID (a string) of style 'widget001', .. , 'widget488' for which we need a lookup table to convert into widgetGridID
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
                    row['includeCountry']=False
                if((rowidx==0) or (row['stationID'] not in sLastStation)):
                    row['includeStation']=True
                    sLastStation=row['stationID']
                else:
                    row['includeStation']=False
                sSamplingHeights=[str(row['samplingHeight'][0])] 
                for element in row['samplingHeight']:
                    sElement=str(element)
                    if(not sElement in sSamplingHeights):
                        sSamplingHeights.append(sElement)
                self.guiPg2createRowOfObsWidgets(scrollableFrame4Widgets, num,  rowidx, row,guiRow, sSamplingHeights, 
                                                                            self.root.fsNORMAL, xPadding, yPadding, self.root.fsSMALL)
                # After drawing the initial widgets, all entries of station and country are set to true unless they are on an exclusion list
                countryInactive=row['country'] in self.excludedCountriesList
                stationInactive=row['stationID'] in self.excludedStationsList
                self.newDf.at[(rowidx) ,  ('includeCountry')] = (not countryInactive)
                self.newDf.at[(rowidx) ,  ('includeStation')] =(not stationInactive)
            if(USE_TKINTER):
                # Update buttons frames idle tasks to let tkinter calculate buttons sizes
                scrollableFrame4Widgets.update_idletasks()
                # Set the scrollableCanvas scrolling region
                scrollableCanvas.config(scrollregion=scrollableCanvas.bbox("all"))
            else: 
                # print(f'widgetID_LUT={self.widgetID_LUT}')
                self.wdgGrid3
                display(self.wdgGrid3)
        #else:
        #    self.update_idletasks()
        if(not USE_TKINTER):
            whichButton=int(ge.guiWidgetsThatWait4UserInput(watchedWidget=self.Pg2GoButton,watchedWidget2=self.Pg2CancelButton, 
                                                                                            title='',  myDescription="PROCEED",  myDescription2="Cancel", width=240))
            #self.askUserGoOrCancel=int(ge.guiWidgetsThatWait4UserInput(watchedWidget=self.Pg2GoButton,watchedWidget2=self.Pg2CancelButton, 
            #                                                                                title='',  myDescription="PROCEED",  myDescription2="Cancel", width=240))
            #whichButton=self.askUserGoOrCancel.selectedBtn
            # 1975 self.Pg2GoButton = ge.guiButton(rootFrame, text="GO!", command=self.EvHdPg2GoBtnHit, fontName=self.root.myFontFamily,  fontSize=self.root.fsLARGE)
            # 2040 ge.guiPlaceWidget(self.wdgGrid, self.Pg2GoButton, row=3, column=7, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
            if(whichButton==1): 
                self.EvHdPg2GoBtnHit()
            else:
                print('User abort.')
                self.closeApp(bWriteStop=True) # Abort run
        return True


    def createPg2staticWidgets(self, rootFrame):
        # ====================================================================
        # Creation  the static widgets  of second GUI page  -- part of lumiaGuiApp (root window)
        # 1) Static widgets (top part)
        # ====================================================================
        self.getFilters()
        # Row 0:  Title Label
        #  ##############################################################################
        self.Pg2TitleLabel = ge.guiTxtLabel(rootFrame, self.Pg2title, fontName=self.root.myFontFamily, fontSize=self.root.fsGIGANTIC, style="bold", nCols=self.nCols,  colwidth=self.nCols)
        # Row 1-4:  Header part with pre-selctions
        #  ##############################################################################
        # Ranking for Observation Files
        self.RankingLabel = ge.guiTxtLabel(rootFrame, text="Obsdata Ranking", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, nCols=self.nCols,  colwidth=1)
        try:
            self.ObsFileRankingBox = ge.guiTextBox(rootFrame,  text=self.ObsFileRankingBoxTxt,  width=self.root.colWidth,  height=(2.4*self.root.rowHeight+self.root.vSpacer),  fontName=self.root.myFontFamily,  fontSize=self.root.fsSMALL)
        except:
            self.ObsFileRankingBox=None
        # Col2
        #print(f'lumiaGUI.createPg2staticWidgets L1912: parent=self={self}')
        #  ##############################################################################
        self.ObsLv1Ckb = ge.guiCheckBox(rootFrame, self, text="Level1", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                                                                variable=self.ObsLv1CkbVar, onvalue=True, offvalue=False)                             
        self.ObsNRTCkb = ge.guiCheckBox(rootFrame, self, text="NRT", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                                                                variable=self.ObsNRTCkbVar, onvalue=True, offvalue=False)                             
        self.ObsOtherCkb = ge.guiCheckBox(rootFrame, self, text="Other", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                                                                variable=self.ObsOtherCkbVar, onvalue=True, offvalue=False)                             
                                                            #command=self.EvHdPg2stationSamplingHghtAction
        # Col 3    Filtering of station altitudes
        #  ##############################################################################
        self.FilterStationAltitudesCkb = ge.guiCheckBox(rootFrame, parent=self, text="Filter station altitudes", fontName=self.root.myFontFamily,  
                                                    fontSize=self.root.fsNORMAL, variable=self.FilterStationAltitudesCkbVar, onvalue=True, offvalue=False, 
                                                    command=self.EvHdPg2stationAltitudeFilterAction, nameOfEvtHd='EvHdPg2stationAltitudeFilterAction') 
        self.minAltLabel = ge.guiTxtLabel(rootFrame, text="min alt:", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, nCols=self.nCols,  colwidth=1)
        self.maxAltLabel = ge.guiTxtLabel(rootFrame, text="max alt:", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, nCols=self.nCols,  colwidth=1)
        # min Altitude Entry
        self.stationMinAltEntry = ge.guiDataEntry(rootFrame,textvariable=self.stationMinAltVar, placeholder_text=str(self.stationMinAlt), width=self.root.colWidth)
        # max Altitude Entry
        self.stationMaxAltEntry = ge.guiDataEntry(rootFrame,textvariable=self.stationMaxAltVar, placeholder_text=str(self.stationMaxAlt), width=self.root.colWidth)
        # Col 5    -  sampling height filter
        #  ##############################################################################
        self.FilterSamplingHghtCkb = ge.guiCheckBox(rootFrame, self, text="Filter sampling heights", fontName=self.root.myFontFamily,  
                                                            fontSize=self.root.fsNORMAL, variable=self.FilterSamplingHghtCkbVar, onvalue=True, offvalue=False, 
                                                            command=self.EvHdPg2stationSamplingHghtAction, nameOfEvtHd='EvHdPg2stationSamplingHghtAction')                             
        self.minHghtLabel = ge.guiTxtLabel(rootFrame, text="min alt:", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, nCols=self.nCols,  colwidth=1)
        self.maxHghtLabel = ge.guiTxtLabel(rootFrame, text="max alt:", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, nCols=self.nCols,  colwidth=1)
        # min inlet height
        self.samplingMinHghtEntry = ge.guiDataEntry(rootFrame,textvariable=self.samplingMinHghtVar, placeholder_text=str(self.samplingMinHght), width=self.root.colWidth)
        # max inlet height
        self.samplingMaxHghtEntry = ge.guiDataEntry(rootFrame,textvariable=self.samplingMaxHghtVar, placeholder_text=str(self.samplingMaxHght), width=self.root.colWidth)
        # Col7
        #  ##############################################################################
        self.ICOSstationsLabel = ge.guiTxtLabel(rootFrame, text="ICOS stations", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, nCols=self.nCols,  colwidth=1)
        if(USE_TKINTER):
            self.Pg2isICOSradioButton = ge.guiRadioButton(rootFrame, text="Any station", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL,
                                                               variable=self.isICOSRadioButtonVar,  value=0,  command=self.EvHdPg2isICOSfilter)
            self.Pg2isICOSradioButton2 = ge.guiRadioButton(rootFrame, text="ICOS only", fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, 
                                                               variable=self.isICOSRadioButtonVar,  value=1,  command=self.EvHdPg2isICOSfilter)
        else:
            self.Pg2isICOSradioButton = ge.guiRadioButton(parent=self, options=['Any station', 'ICOS only'], preselected=self.isICOSRadioButtonVar, description='', 
                                                                                        command=self.EvHdPg2isICOSfilter)
           
        # Col_11                
        #  ##############################################################################
        # Cancel Button
        #self.CancelButton = ctk.CTkButton(master=rootFrame, font=(self.root.myFontFamily, self.root.fsNORMAL), text="Cancel", fg_color='orange red', command=self.closeApp)
        self.Pg2CancelButton = ge.guiButton(rootFrame, text="Cancel", command=self.closeApp, fontName=self.root.myFontFamily, fontSize=self.root.fsNORMAL) 
        if(USE_TKINTER): # TODO fix
            ge.updateWidget(self.Pg2CancelButton,  value='gray1', bText_color=True)
            ge.updateWidget(self.Pg2CancelButton,  value='DarkOrange1', bFg_color=True) # in CTk this is the main button color (not the text color)
        # Col_12
        #  ##############################################################################
        # GO! Button
        #self.GoButton = ctk.CTkButton(rootFrame, font=(self.root.myFontFamily, self.root.fsLARGE), command=EvHdPg2GoBtnHit,  text="GO!")  # Note: expressions after command= cannot have parameters or they will be executed at initialisation which is unwanted
        #print('runPg2 creating createPg2staticWidgets.ge.guiButton()')
        self.Pg2GoButton = ge.guiButton(rootFrame, text="GO!", command=self.EvHdPg2GoBtnHit, fontName=self.root.myFontFamily,  fontSize=self.root.fsLARGE)
        if(USE_TKINTER): # TODO fix
            ge.updateWidget(self.Pg2GoButton,  value='gray1', bText_color=True)
            ge.updateWidget(self.Pg2GoButton,  value='green3', bFg_color=True) # in CTk this is the main button color (not the text color)
        # Row 4 title for individual entries
        #  ##############################################################################
        # newColumnNames=['selected','country', 'stationID', 'altOk', 'altitude', 'HghtOk', 'samplingHeight', 'IcosClass', 'latitude', 'longitude', 'dClass', 'dataSetLabel', 'pid', 'includeCountry', 'includeStation']
        myLabels="Selected            Country                StationID              SamplingHeight    Stat.altitude  ICOS-affil. Latitude Longitude  DataRanking DataDescription"
        if(USE_TKINTER):
            myLabels="Selected     Country     StationID     SamplingHeight   Stat.altitude  ICOS-affil. Latitude Longitude  DataRanking DataDescription"
        self.ColLabels = ge.guiTxtLabel(rootFrame, anchor="w", text=myLabels, fontName=self.root.myFontFamily,  fontSize=self.root.fsNORMAL, nCols=self.nCols,  colwidth=(self.nCols-1))
        return True

   
    def  placePg2staticWidgetsOnCanvas(self, nCols,  nRows,  xPadding,  yPadding):
        # ====================================================================
        # Placement  the static widgets  of the second GUI page  -- part of lumiaGuiApp (root window)
        # 1) Static widgets (top part)
        # ====================================================================
        ge.guiPlaceWidget(self.wdgGrid, self.Pg2TitleLabel, row=0, column=0, columnspan=nCols,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.RankingLabel, row=1, column=0, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        #self.RankingLabel.grid(row=1, column=0, columnspan=1, padx=xPadding, pady=yPadding, sticky="nw")
        if not (self.ObsFileRankingBox is None):
            ge.guiPlaceWidget(self.wdgGrid, self.ObsFileRankingBox, row=2, column=0, columnspan=1,  rowspan=2,padx=xPadding, pady=yPadding, sticky="ew")
        #self.ObsFileRankingBox.grid(row=2, column=0, columnspan=1, rowspan=2, padx=xPadding, pady=yPadding, sticky="nsew")
        # Col2
        #  ##############################################################################
        ge.guiPlaceWidget(self.wdgGrid, self.ObsLv1Ckb, row=1, column=1, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.ObsNRTCkb, row=2, column=1, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.ObsOtherCkb, row=3, column=1, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Col 3    Filtering of station altitudes
        #  ##############################################################################
        ge.guiPlaceWidget(self.wdgGrid, self.FilterStationAltitudesCkb, row=1, column=2, columnspan=2,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.minAltLabel, row=2, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.maxAltLabel, row=3, column=2, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # min Altitude Entry
        ge.guiPlaceWidget(self.wdgGrid, self.stationMinAltEntry, row=2, column=3, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # max Altitude Entry
        ge.guiPlaceWidget(self.wdgGrid, self.stationMaxAltEntry, row=3, column=3, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Col 5+6    -  sampling height filter
        #  ##############################################################################
        # 
        ge.guiPlaceWidget(self.wdgGrid, self.FilterSamplingHghtCkb, row=1, column=4, columnspan=2,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.minHghtLabel, row=2, column=4, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.wdgGrid, self.maxHghtLabel, row=3, column=4, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # min inlet height
        ge.guiPlaceWidget(self.wdgGrid, self.samplingMinHghtEntry, row=2, column=5, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # max inlet height
        ge.guiPlaceWidget(self.wdgGrid, self.samplingMaxHghtEntry, row=3, column=5, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Col7
        #  ##############################################################################
        # 
        ge.guiPlaceWidget(self.wdgGrid, self.ICOSstationsLabel, row=1, column=6, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        if(USE_TKINTER):
            ge.guiPlaceWidget(self.wdgGrid, self.Pg2isICOSradioButton, row=2, column=6, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
            ge.guiPlaceWidget(self.wdgGrid, self.Pg2isICOSradioButton2, row=3, column=6, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        else:
            ge.guiPlaceWidget(self.wdgGrid, self.Pg2isICOSradioButton, row=2, column=6, columnspan=1, rowspan=2 ,padx=xPadding, pady=yPadding, sticky="ew")
        # Col_11
        #  ##############################################################################
        # 
        ge.guiPlaceWidget(self.wdgGrid, self.Pg2CancelButton, row=2, column=7, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Col_12
        #  ##############################################################################
        # GO! Button
        ge.guiPlaceWidget(self.wdgGrid, self.Pg2GoButton, row=3, column=7, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        # Row 4 title for individual entries
        #  ##############################################################################
        # newColumnNames=['selected','country', 'stationID', 'altOk', 'altitude', 'HghtOk', 'samplingHeight', 'IcosClass', 'latitude', 'longitude', 'dClass', 'dataSetLabel', 'pid', 'includeCountry', 'includeStation']
        ge.guiPlaceWidget(self.wdgGrid, self.ColLabels, row=4, column=0, columnspan=nCols,padx=xPadding, pady=yPadding, sticky="ew")
        #self.ColLabels.grid(row=4, column=0, columnspan=10, padx=2, pady=yPadding, sticky="nw")
        return True
   

def  readMyYamlFile(ymlFile):
    '''
    Function readMyYamlFile

    @param ymlFile : the LUMIA YAML configuration file in yaml (or rc) data format (formatted text)
    @type string (file name)
    @return contents of the ymlFile
    @type yamlObject
    '''
    ymlContents=None
    try:
        with open(ymlFile, 'r') as file:
            ymlContents = yaml.safe_load(file)
    except:
        sCmd="cp "+ymlFile+'.bac '+ymlFile # recover from most recent backup file.
        os.system(sCmd)
        try:
            with open(ymlFile, 'r') as file:
                ymlContents = yaml.safe_load(file)
            #sCmd="cp "+ymlFile+' '+ymlFile+'.bac' # This is now already done in housekeeping.py, which is more consistent
            #os.system(sCmd)
        except:
            logger.error(f"Abort! Unable to read yaml configuration file {ymlFile} - failed to read its contents with yaml.safe_load()")
            sys.exit(1)
    return(ymlContents)


# def main():    
p = argparse.ArgumentParser()
p.add_argument('--start', dest='start', default=None, help="Start of the simulation in date+time ISO notation as in \'2018-08-31 00:18:00\'. Overwrites the value in the rc-file")
p.add_argument('--end', dest='end', default=None, help="End of the simulation as in \'2018-12-31 23:59:59\'. Overwrites the value in the rc-file")
p.add_argument('--rcf', dest='rcf', default=None, help="Same as the --ymf option. Deprecated. For backward compatibility only.")   
p.add_argument('--ymf', dest='ymf', default=None,  help='yaml configuration file where the user plans his or her Lumia run: parameters, input files etc.')   
p.add_argument('--serial', '-s', action='store_true', default=False, help="Run on a single CPU")
p.add_argument('--noTkinter', '-n', action='store_true', default=False, help="Do not use tkinter (=> use ipywidgets)")
p.add_argument('--verbosity', '-v', dest='verbosity', default='INFO')
args, unknown = p.parse_known_args(sys.argv[1:])

# Set the verbosity in the logger (loguru quirks ...)
logger.remove()
logger.add(sys.stderr, level=args.verbosity)

USE_TKINTER=False # when called from lumiaGUInotebook.ipynb there are no commandline options
if((args.start is not None) or (args.rcf is not None) or (args.ymf is not None)):
    USE_TKINTER=True # called as a notebook, not from the commandline
if(args.noTkinter):
    USE_TKINTER=False
if(USE_TKINTER):
    import guiElementsTk as ge
    import tkinter as tk    
    #import customtkinter as ctk
else:
    import ipywidgets  as wdg
    #from jupyter_ui_poll import ui_events
    #from functools import partial
    import guiElements_ipyWdg as ge
    from IPython.display import display #, HTML,  clear_output
    # from ipywidgets import  Dropdown, Output, Button, FileUpload, SelectMultiple, Text, HBox, IntProgress
if(args.rcf is None):
    if(args.ymf is None):
        ymlFile=None
    else:
        ymlFile = args.ymf
else:            
    ymlFile = args.rcf


# Call the main method
prepareCallToLumiaGUI(ymlFile, args)


