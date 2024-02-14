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
        root = LumiaTkGui() #tk.Tk()
        lumiaGuiAppInst=lumiaGuiApp(root)
        #lumiaGuiAppInst.button.invoke()
        lumiaGuiAppInst.sLogCfgPath = sLogCfgPath 
        lumiaGuiAppInst.ymlContents = ymlContents
        lumiaGuiAppInst.ymlFile = ymlFile
        guiPg1TpLv=lumiaGuiAppInst.guiPage1AsTopLv(iVerbosityLv)
        lumiaGuiAppInst.runPage2(iVerbosityLv)  # of lumiaGuiApp
        root.mainloop()
        sys.exit(0)
        
        #guiPages=[]
        #guiPages.append(LumiaTkGui())  # 1st page ctk.ctk window instance
        #guiPages.append(LumiaTkGui())  # 2nd page ctk.ctk window instance
        #root=guiPages[0] # LumiaTkGui() # .pack(side="top", fill="both", expand=True)
    else:
        notify_output = widgets.Output()
        display(notify_output)
    '''
    (bFirstGuiPageSuccessful, ymlContents)=LumiaGuiPage1(root, sLogCfgPath, ymlContents, ymlFile, bRefine=False, iVerbosityLv=1) 
    
    # Go hunt for the data
    
    # Present to the user the data sets found
    if(USE_TKINTER):
        root=guiPages[1] 
    (bFirstGuiPageSuccessful, ymlContents)=LumiaGuiPage1(root,  sLogCfgPath, ymlContents, ymlFile, bRefine=True, iVerbosityLv=1) 
    '''
    logger.info('LumiaGUI completed successfully. The updated Lumia config file has been written to:')
    logger.info(ymlFile)
    return

class lumiaGuiApp:
    def __init__(self, root):
        self.root = root
        #self.initActive=True
        self.guiPg1TpLv=None
        self.label1 = tk.Label(self.root, text="App main window - hosting the second GUI page.")
        #self.button = tk.Button(self.root, text="Go to page 1", command=self.guiPage1AsTopLv) # TODO: for testing only. remove this button.
        #self.initActive=False
        self.label1.pack()
        #self.button.pack()
        #self.button.invoke()
        
    def closeTopLv(self, bWriteStop=True):  # of lumiaGuiApp
        #if(self.initActive):
        #    return
        self.guiPg1TpLv.destroy()
        if(bWriteStop):
            self.cleanUp(bWriteStop)
            logger.info('lumiaGUI canceled by user.')
            self.closeApp(False)
        self.guiPg1TpLv=None
        self.root.deiconify()
        #self.runPage2()

    def gotoPage2(self):
        self.closeTopLv(bWriteStop=False)

    def closeApp(self, bWriteStop=True):  # of lumiaGuiApp
        self.cleanUp(bWriteStop)
        logger.info("Closing the GUI...")
        self.root.destroy()
        if(bWriteStop):
            logger.info('lumiaGUI canceled by user.')
        else:
            logger.info(f'LumiaGUI completed successfully. The updated Lumia config file has been written to: {self.ymlFile}')
        sys.exit(0)

    def exitWithSuccess(self):
        self.closeApp(bWriteStop=False)
        
    def guiPage1AsTopLv(self, iVerbosityLv='INFO'):  # of lumiaGuiApp
        #if(self.initActive):
        #    return
        if(self.guiPg1TpLv is None):
            self.guiPg1TpLv = tk.Toplevel(self.root)
            #self.initActive=False
        self.root.iconify()
        #self.initActive=True
        self.label2 = tk.Label(self.guiPg1TpLv, text="I'm your page1 toplevel window.")
        self.guiPg1TpLv.protocol("WM_DELETE_WINDOW", self.closeTopLv)
        self.button3 = tk.Button(self.guiPg1TpLv, text="Go to page 2", command=self.gotoPage2)
        #self.initActive=False
        self.label2.pack()
        self.button3.pack()

    def runPage2(self, iVerbosityLv='INFO'):  # of lumiaGuiApp
        self.button2 = tk.Button(self.root, text="Exit", command=self.exitWithSuccess) # Note: expressions after command= cannot have parameters or they will be executed at initialisation which is unwanted
        self.button2.pack()

    def cleanUp(self,  bWriteStop=True):  # of lumiaGuiApp
        if(bWriteStop): # the user selected Cancel - else the LumiaGui.go message has already been written
            logger.info("LumiaGUI was canceled.")
            sCmd="touch LumiaGui.stop"
            hk.runSysCmd(sCmd)
    
   

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
    global LOOP_ACTIVE
    LOOP_ACTIVE=True
    def CloseTheGUI(bAskUser=True,  bWriteStop=True):
        if(bAskUser):  # only happens if the GUI window was closed brutally
            if ge.guiAskOkCancel(title="Quit?",  message="Is it OK to abort your Lumia run?"):  # tk.messagebox.askokcancel("Quit", "Is it OK to abort your Lumia run?"):
                if(bWriteStop):
                    logger.info("LumiaGUI was canceled.")
                    sCmd="touch LumiaGui.stop"
                    hk.runSysCmd(sCmd)
        else:  # the user clicked Cancel or Go
            if(bWriteStop): # the user selected Cancel - else the LumiaGui.go message has already been written
                logger.info("LumiaGUI was canceled.")
                sCmd="touch LumiaGui.stop"
                hk.runSysCmd(sCmd)
            
            global LOOP_ACTIVE
            LOOP_ACTIVE = False
            logger.info("Closing the GUI...")
            try:
                if(guiPage1 is not None):
                    try:
                        # close the current gui page
                        if(bRefine):
                            guiPage1.quit()
                            #guiPage1.after(100, guiPage1.event_generate("<Destroy>"))
                        else:
                            guiPage1.iconify
                            #guiPage1.close()
                    except:
                        pass
            except:
                pass
        #if(USE_TKINTER):
        #    guiPage1.protocol("WM_DELETE_WINDOW", CloseTheGUI)
    
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
    
    # Plan the layout of the GUI - get screen dimensions, choose a reasonable font size for it, xPadding, etc.
    def stakeOutSpacesAndFonts(guiPage1, nCols, nRows):
        guiPage1.appWidth, guiPage1.appHeight,  guiPage1.xPadding, guiPage1.yPadding = displayGeometry(maxAspectRatio=1.2)
        wSpacer=2*guiPage1.xPadding
        #vSpacer=2*guiPage1.yPadding
        likedFonts=["Georgia", "Liberation","Arial", "Microsoft","Ubuntu","Helvetica"]
        sLongestTxt="Start date (00:00h):"  # "Latitude (≥33°N):"
        for myFontFamily in likedFonts:
            (bFontFound, guiPage1.fsTINY,  guiPage1.fsSMALL,  guiPage1.fsNORMAL,  guiPage1.fsLARGE,  guiPage1.fsHUGE,  guiPage1.fsGIGANTIC,  bWeMustStackColumns, bSuccess)= \
                bs.calculateEstheticFontSizes(myFontFamily,  guiPage1.appWidth,  guiPage1.appHeight, sLongestTxt, nCols, nRows, 
                                                                xPad=guiPage1.xPadding, yPad=guiPage1.yPadding, maxFontSize=20, 
                                                                USE_TKINTER=USE_TKINTER, bWeCanStackColumns=False)
            if(not bSuccess):
                if(not bFontFound):
                    myFontFamily="Times New Roman"  # should exist on any operating system with western language fonts installed...
            if(bFontFound):
                break
        if(not bFontFound):
            myFontFamily="Times New Roman"  # should exist on any operating system with western language fonts installed...
        guiPage1.myFontFamily=myFontFamily
        hDeadSpace=wSpacer+(nCols*guiPage1.xPadding*2)+wSpacer
        vDeadSpace=2*guiPage1.yPadding #vSpacer+(nRows*guiPage1.yPadding*2)+vSpacer
        guiPage1.colWidth=int((guiPage1.appWidth - hDeadSpace)/(nCols*1.0))
        guiPage1.rowHeight=int((guiPage1.appHeight - vDeadSpace)/(nRows*1.0))
        return()
        
        
    # Plan the layout of the GUI - get screen dimensions, choose a reasonable font size for it, xPadding, etc.
    if(bRefine):
        nCols=12 # sum of labels and entry fields per row
        nRows=32 #5+len(newDf) # number of rows in the GUI - not so important - window is scrollable
    else:
        nCols=5 # sum of labels and entry fields per row
        nRows=13 # number of rows in the GUI
    stakeOutSpacesAndFonts(guiPage1, nCols, nRows)
    xPadding=guiPage1.xPadding
    yPadding=guiPage1.yPadding

    # Dimensions of the window
    appWidth, appHeight = guiPage1.appWidth, guiPage1.appHeight

    # Define all widgets needed. 
    #guiPage1.lonMin = ge.guiDoubleVar(value=-25.0)
    #guiPage1.lonMax = ge.guiDoubleVar(value=45.0)
    #guiPage1.latMin = ge.guiDoubleVar(value=23.0)
    #guiPage1.latMax = ge.guiDoubleVar(value=83.0)
    # Row 0:  Title Label
    # ################################################################
    if(bRefine):
        title="LUMIA  --  Refine your selections among the data discovered"
    else:
        title="LUMIA  --  Configure your next LUMIA run"
    guiPage1.TitleLabel = ge.guiTxtLabel(guiPage1, title, fontName=guiPage1.myFontFamily, fontSize=guiPage1.fsGIGANTIC, bold=True)
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
    # Sets the dimensions of the window to something reasonable with respect to the user's screen properties
    xPadding=int(0.008*maxW)
    yPadding=int(0.008*maxH)
    return(maxW, maxH, xPadding, yPadding)




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


