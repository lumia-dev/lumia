#!/usr/bin/env python

import os
import sys
import argparse
import time
import platform
import traceback
import _thread
from loguru import logger
from pandas import DataFrame,  read_csv, concat  #, to_datetime, concat , read_csv, Timestamp, Timedelta, offsets  #, read_hdf, Series  #, read_hdf, Series
from pandas.api.types import is_float_dtype
from icoscp.cpb import metadata
from icoscp_core.icos import meta as iccMeta
from icoscp.cpb.dobj import Dobj
import xarray as xr
from icoscp_core.icos import data as iccData

from screeninfo import get_monitors
import myCarbonPortalTools
import housekeeping as hk
#from ipywidgets import widgets

USE_TKINTER=True
scriptName=sys.argv[0]
if('.ipynb' in scriptName[-6:]):
    USE_TKINTER=False
USE_TKINTER=False
if(USE_TKINTER):
    import guiElementsTk as ge
    import tkinter as tk    
    import customtkinter as ctk
else:
    #import guiElements_ipyWdg as ge
    from IPython.display import display, HTML,  clear_output
import boringStuff as bs
MIN_SCREEN_RES=480 # pxl - just in case querying screen size fails for whatever reason...

def testGui():
    scriptDirectory = os.path.dirname(os.path.abspath(sys.argv[0]))
    myDsp=os.environ.get('DISPLAY','')
    if (myDsp == ''):
        logger.warning('DISPLAY not listed in os.environ. On simple systems DISPLAY is usually :0.0  ...so I will give that one a shot. Proceeding with fingers crossed...')
        os.environ.__setitem__('DISPLAY', ':0.0')
    else:
        logger.info(f'found Display {myDsp}')
    if(USE_TKINTER):
        ctk.set_appearance_mode("System")  
        # background color (R,G,B)=(13,57,64)  = #0D3940
        # Sets the color of the widgets in the window
        # Supported themes : green, dark-blue, blue   
        # ctk.set_default_color_theme("dark-blue")  # Themes: "blue" (standard), "green", "dark-blue"
        ctk.set_default_color_theme(scriptDirectory+"/doc/lumia-dark-theme.json") 
        root=ctk.CTk()
        root.title('LUMIA - the Lund University Modular Inversion Algorithm')  
        # rootFrame.geometry(f"{maxW+1}x{maxH+1}")   
        ctk.set_appearance_mode("System")  
        ctk.set_default_color_theme(scriptDirectory+"/doc/lumia-dark-theme.json")
    #appWidth, appHeight,  xPadding, yPadding = displayGeometry() 
    widgetsLst = []
    sLogCfgPath=""
    if(0<1):
        if(USE_TKINTER):
            rootFrame = tk.Frame(root, bg="cadet blue")
            rootFrame.grid(sticky='news')
            FrameCanvas1 = tk.Frame(rootFrame)
            guiPage1=LumiaGui(FrameCanvas1,  root) 
            # query the CarbonPortal for suitable data sets that match the requests defined in the config.yml file and the latest GUI choices
            #root.withdraw()
            guiPage1.attributes('-topmost', 'true')
        else:
            guiPage1=LumiaGui(None,  None) 
            root=None
        rVal=guiPage1.run(root, sLogCfgPath) 
        # root.wait_window(guiPage1)
        if(rVal):
            guiPage1.show()
    else:
        rVal=True
    rVal=False
    if(rVal):
        rootFrame2 = tk.Frame(root, bg="cadet blue")
        rootFrame2.grid(sticky='news')
        FrameCanvas2 = tk.Frame(rootFrame2)
        #guiPage2=RefineObsSelectionGUI(root,  widgetsLst=widgetsLst) 
        #guiPage2.run(root, sLogCfgPath) 
        guiPage2=RefineObsSelectionGUI(FrameCanvas2, root) #,  widgetsLst=widgetsLst) 
        guiPage2.run(FrameCanvas2, sLogCfgPath) 
        guiPage2.show()
    # root.mainloop()
    return

def displayGeometry(maxAspectRatio=1.778):  
    '''
    maxAspectRatio 1920/1080.0=1.778 is here to prevant silly things if one uses multiple screens....
    '''
    # Get the screen resolution
    # m may return a string like
    # Monitor(x=0, y=0, width=1920, height=1080, width_mm=1210, height_mm=680, name='HDMI-1', is_primary=True)
    try:
        monitors=get_monitors()  # may require apt install -y libxrandr2  on some ubuntu systems
        screenWidth=0
        for screen in monitors:
            try:
                useThisOne=screen.is_primary
            except:
                useThisOne=True # 'isprimary' entry may not be present on simple systems, then it is the only one
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
    logger.debug(f'self.winfo_screenwidth()={screenWidth},   self.winfo_screenheight()={screenHeight},  multiple screen correction')
    maxW = int(0.92*screenWidth)
    maxH = int(0.92*screenHeight)
    if(maxW > 1.2*maxH):
        maxW = int((1.2*maxH)+0.5)
        logger.debug(f'maxW={maxW},  maxH={maxH},  max aspect ratio fix.')
    # Sets the dimensions of the window to something reasonable with respect to the user's screren properties
    xPadding=int(0.008*maxW)
    yPadding=int(0.008*maxH)
    return(maxW, maxH, xPadding, yPadding)



# LumiaGui Class =============================================================
class LumiaGui():  #ctk.CTkToplevel  ctk.CTk):
    # =====================================================================
    # The layout of the window will be written
    # in the init function itself
    def __init__(self, root, parent):  # *args, **kwargs):
        if(USE_TKINTER):
            self.__init__(self, root)
            self.parent = parent #root
        # Get the screen resolution to scale the GUI in the smartest way possible...
        if(os.path.isfile("LumiaGui.stop")):
            sCmd="rm LumiaGui.stop"
            hk.runSysCmd(sCmd,  ignoreError=True)
        if(os.path.isfile("LumiaGui.go")):
            sCmd="rm LumiaGui.go"
            hk.runSysCmd(sCmd,  ignoreError=True)

    def run(self, root,  sLogCfgPath): 
        notify_output = None #widgets.Output()
        display(notify_output)
        self.appWidth, self.appHeight,  self.xPadding, self.yPadding = displayGeometry(maxAspectRatio=1.2)
        xPadding=self.xPadding
        wSpacer=2*self.xPadding
        yPadding=self.yPadding
        vSpacer=2*self.yPadding
        if(USE_TKINTER):
            root.grid_rowconfigure(0, weight=1)
            root.columnconfigure(0, weight=1)
        nCols=5 # sum of labels and entry fields per row
        nRows=13 # number of rows in the GUI
        likedFonts=["Georgia", "Liberation","Arial", "Microsoft","Ubuntu","Helvetica"]
        sLongestTxt="Start date (00:00h):"  # "Latitude (≥33°N):"
        for myFontFamily in likedFonts:
            (bFontFound, fsTINY,  fsSMALL,  fsNORMAL,  fsLARGE,  fsHUGE,  fsGIGANTIC,  bWeMustStackColumns, bSuccess)= \
                bs.calculateEstheticFontSizes(myFontFamily,  self.appWidth,  self.appHeight, sLongestTxt, nCols, nRows, xPad=xPadding, 
                                                            yPad=yPadding, maxFontSize=20, USE_TKINTER=USE_TKINTER, bWeCanStackColumns=False)
            if(bFontFound):
                break
        hDeadSpace=wSpacer+(nCols*xPadding*2)+wSpacer
        vDeadSpace=2*yPadding #vSpacer+(nRows*yPadding*2)+vSpacer
        colWidth=int((self.appWidth - hDeadSpace)/(nCols*1.0))
        rowHeight=int((self.appHeight - vDeadSpace)/(nRows*1.0))
        # Dimensions of the window
        appWidth, appHeight = self.appWidth, self.appHeight
        #activeTextColor='gray10'
        #inactiveTextColor='gray50'
        #self.lonMin = ge.guiDoubleVar(value=-25.0)
        #self.lonMax = ge.guiDoubleVar(value=45.0)
        #self.latMin = ge.guiDoubleVar(value=23.0)
        #self.latMax = ge.guiDoubleVar(value=83.0)
        
        def CloseTheGUI(bAskUser=True,  bWriteStop=True):
            if(bAskUser):  # only happens if the GUI window was closed brutally
                if (False): #ge.guiAskOkCancel(title="Quit?",  message="Is it OK to abort your Lumia run?"):  # tk.messagebox.askokcancel("Quit", "Is it OK to abort your Lumia run?"):
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
            if(USE_TKINTER):
                self.protocol("WM_DELETE_WINDOW", CloseTheGUI)
        
        def CancelAndQuit(): 
            #logger.info("LumiaGUI was canceled.")
            #sCmd="touch LumiaGui.stop"
            #hk.runSysCmd(sCmd)
            CloseTheGUI(bAskUser=False,  bWriteStop=True)
            #global LOOP_ACTIVE
            #LOOP_ACTIVE = False

        if(USE_TKINTER):
            # Sets the title of the window to "LumiaGui"
            self.title("LUMIA run configuration")  
            # Sets the dimensions of the window to 800x900
            self.geometry(f"{appWidth}x{appHeight}")   
        # Row 12
        # ################################################################
        # Cancel Button
        if(USE_TKINTER):
            self.CancelButton = None #ge.guiButton(self, text="Cancel",  command=CancelAndQuit,  fontName="Georgia",  fontSize=fsLARGE) 
            self.CancelButton.grid(row=12, column=4,
                                        columnspan=1, padx=xPadding,
                                        pady=yPadding, sticky="ew")
        else:
            @notify_output.capture()
            def popup(text):
                clear_output()
                display(HTML("<script>alert('{}');</script>".format(text)))
            
            def clickme(b):
                popup('Hello World!')
            
            CancelButton = None #ge.guiButton(self, text="Cancel",  command=CancelAndQuit,  fontName="Georgia",  fontSize=fsLARGE) 
            #btn_clickme = widgets.Button(description='Cancel')
            CancelButton.on_click(CancelAndQuit)  # clickme)
            
            display(CancelButton)
            #self.CancelButton.on_click(CancelAndQuit)
            #self.CancelButton

        # Row 0:  Title Label
        # ################################################################

        self.LatitudesLabel = None#ge.guiTxtLabel(self, "LUMIA  --  Configure your next LUMIA run",  
                                                     #               fontName="Arial MT Extra Bold", fontSize=fsGIGANTIC)
        if(USE_TKINTER):
            self.LatitudesLabel.grid(row=0, column=0,
                            columnspan=8,padx=xPadding, pady=yPadding,
                            sticky="ew")
        else:
            self.LatitudesLabel

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

        if(not USE_TKINTER):
            toolbar_widget = None #widgets.VBox()
            toolbar_widget.children = [
                self.LatitudesLabel, 
                CancelButton
            ]
            toolbar_widget   
            display(toolbar_widget) 
        _thread.start_new_thread(loop_function, ())
        #root.mainloop()
        return(True)

    def show(self):
        if(USE_TKINTER):
            self.wm_protocol("WM_DELETE_WINDOW", self.destroy)
            self.wait_window(self)
        return 


# LumiaGui Class =============================================================
class RefineObsSelectionGUI(): # ctk.CTk
    # =====================================================================
    # The layout of the window is now written
    # in the init function itself
    def __init__(self, root,  parent):  # *args, **kwargs):
        ctk.CTk.__init__(self, root)
        self.parent = parent #root

    def run(self, rootFrame,  sLogCfgPath):
        nWidgetsPerRow=5
        #cpDir=ymlContents['observations'][tracer]['file']['cpDir']
        # sOutputPrfx=ymlContents[ 'run']['thisRun']['uniqueOutputPrefix']
        nCols=12 # sum of labels and entry fields per row
        nRows=32 #5+len(newDf) # number of rows in the GUI - not so important - window is scrollable
        self.appWidth, self.appHeight,  self.xPadding, self.yPadding = displayGeometry() 
        #self.title("LUMIA: Refine the selection of observations to be used")
        xPadding=self.xPadding
        wSpacer=2*self.xPadding
        yPadding=self.yPadding
        vSpacer=2*self.yPadding
        myFontFamily="Georgia"
        sLongestTxt="Latitude NN :"
        (bFontFound, fsTINY,  fsSMALL,  fsNORMAL,  fsLARGE,  fsHUGE,  fsGIGANTIC,  bWeMustStackColumns, bSuccess)= \
            bs.calculateEstheticFontSizes(myFontFamily,  self.appWidth,  self.appHeight, sLongestTxt, nCols, nRows, xPad=xPadding, 
                                                        yPad=yPadding, maxFontSize=20,  bWeCanStackColumns=False)
        hDeadSpace=wSpacer+(nCols*xPadding*2)+wSpacer
        vDeadSpace=2*yPadding 
        colWidth=int((self.appWidth - hDeadSpace)/(nCols*1.0))
        rowHeight=int((self.appHeight - vDeadSpace)/(nRows*1.0))
        activeTextColor='gray10'
        inactiveTextColor='gray50'
        nRows=int(0)
        #rootFrame = tk.Frame(root, bg="cadet blue")
        rootFrame.grid(sticky='news')
        rootFrameCanvas = rootFrame # tk.Frame(rootFrame)
        rootFrameCanvas.grid(row=5, column=0,  columnspan=12,  rowspan=20, pady=(5, 0), sticky='nw') #, columnspan=11,  rowspan=10
        rootFrameCanvas.grid_rowconfigure(0, weight=8)
        rootFrameCanvas.grid_columnconfigure(0, weight=8)
        # run through the first page of the GUI so we select the right time interval, region, emissions files etc
        # Now we venture to make the root scrollable....
        #main_frame = tk.Frame(root)

        # Create a scrollable frame onto which to place the many widgets that represent all valid observations found
        #  ##################################################################
        # Create a frame for the canvas with non-zero row&column weights
        
        #rootFrame.update() # above widgets are drawn and we can get column width measures
        #x, y, width, height = rootFrame.grid_bbox()

        #rootFrameCanvas = tk.Frame(rootFrame)
        #rootFrameCanvas.grid(row=5, column=0,  columnspan=12,  rowspan=20, pady=(5, 0), sticky='nw') #, columnspan=11,  rowspan=10
        #rootFrameCanvas.grid_rowconfigure(0, weight=8)
        #rootFrameCanvas.grid_columnconfigure(0, weight=8)
        # Set grid_propagate to False to allow 5-by-5 buttons resizing later
        #rootFrameCanvas.grid_propagate(False)
        cWidth = self.appWidth - xPadding
        cHeight = self.appHeight - (7*rowHeight) - (3*yPadding)
        cHeight = self.appHeight - (7*rowHeight) - (3*yPadding)
        
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

        self.LatitudesLabel = ctk.CTkLabel(rootFrame,
                                text="LUMIA  --  Refine the selection of observations to be used",  font=("Arial MT Extra Bold",  fsGIGANTIC))
        self.LatitudesLabel.grid(row=0, column=0,
                            columnspan=8,padx=xPadding, pady=yPadding,
                            sticky="nw")
        self.iObservationsFileLocation= None #ge.guiIntVar(value=1)
        #iObservationsFileLocation.set(1) # Read observations from local file
        self.RankingLabel = None #ge.guiTxtLabel(rootFrame,
                                  # text="Obsdata Ranking", fontName="Georgia",  fontSize=fsNORMAL)
        self.RankingLabel.grid(row=1, column=0,
                                  columnspan=1, padx=xPadding, pady=yPadding,
                                  sticky="nw")
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

        # Update buttons frames idle tasks to let tkinter calculate buttons sizes
        #scrollableFrame4Widgets.update_idletasks()
              
        # Set the scrollableCanvas scrolling region
        #scrollableCanvas.config(scrollregion=scrollableCanvas.bbox("all"))

            
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

    def show(self):
        self.wm_protocol("WM_DELETE_WINDOW", self.destroy)
        self.wait_window(self)
        return 


def  getMetaDataFromPid_via_icosCore(pid, icosStationLut):
    mdata=[]
    ndr=0
    bAcceptable=True  
    url="https://meta.icos-cp.eu/objects/"+pid
    sid='   '
    try:
        # first try the low-level icscp-core library to query the metadata
        pidMetadataCore =  iccMeta.get_dobj_meta(url)
        #columnNames=['pid', 'dobjMeta.ok', 'icoscpMeta.ok', 'file.ok', 'metafName', 'fName','url', 
        # 'stationID', 'country', 'latitude','longitude','altitude','samplingHeight','is-ICOS','size', 'nRows','dataLevel',
        # 'obsStart','obsStop','productionTime','accessUrl','fileName','dataSetLabel'] 
        try:
            d=pidMetadataCore.specificInfo.acquisition.station.id
            sid=d[:3]
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read stationid from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.acquisition.station.countryCode
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read countryCode from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.acquisition.station.location.lat
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station latitude from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.acquisition.station.location.lon
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station longitude from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.acquisition.station.location.alt
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station altitude from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.acquisition.samplingHeight
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station samplingHeight from metadata')
        mdata.append(d)
        try:
            #       projectUrl=pidMetadataCore.specification.project.self.uri 
            # is the station an ICOS station? values are {'1','2','A','no'} class 1, 2, or A(ssociated) or else 'no' meaning it is not an ICOS station
            d='no'
            try:
                value=icosStationLut[sid]
                d=value
            except:
                pass
        except:
            ndr+=1
            logger.debug('Failed to read ICOS flag from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.size
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read size from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.nRows
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read nRows from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specification.dataLevel
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read dataLevel from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.acquisition.interval.start
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read acquisition start from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.acquisition.interval.stop
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read acquisition stop from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.specificInfo.productionInfo.dateTime
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read productionInfo from metadata')
        mdata.append(d)
        try:
            d=pidMetadataCore.accessUrl
        except:
            d=''
            #ndr+=1  # we got here via the url, so no drama if we don't have a value we already know
        mdata.append(d)
        try:
            d=pidMetadataCore.fileName
        except:
            d=''
            #ndr+=1 # informativ but it is not being used
        mdata.append(d)
        try:
            d=pidMetadataCore.specification.self.label
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read data label from metadata')
        mdata.append(d)
    except:
        print(f'Failed to get the metadata using icoscp_core.icos.iccMeta.get_dobj_meta(url) for url={url}')
        return(None,  False)  # Tell the calling parent routine that things went south...

    if(ndr>1):
        bAcceptable=False
    return (mdata, bAcceptable)

def  getMetaDataFromPid_via_icoscp(pid, icosStationLut):
    mdata=[]
    ndr=0
    bAcceptable=True  
    url="https://meta.icos-cp.eu/objects/"+pid
    sid='   '
    try:
        # first try the low-level icscp-core library to query the metadata
        pidMetadata =  metadata.get(url)
        #columnNames=['pid', 'dobjMeta.ok', 'icoscpMeta.ok', 'file.ok', 'metafName', 'fName','url', 
        # 'stationID', 'country', 'latitude','longitude','altitude','samplingHeight','is-ICOS','size', 'nRows','dataLevel',
        # 'obsStart','obsStop','productionTime','accessUrl','fileName','dataSetLabel'] 
        try:
            d=pidMetadata['specificInfo']['acquisition']['station']['id']
            sid=d[:3]
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read stationid from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['acquisition']['station']['countryCode']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read countryCode from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['acquisition']['station']['location']['lat']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station latitude from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['acquisition']['station']['location']['lon']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station longitude from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['acquisition']['station']['location']['alt']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station altitude from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['acquisition']['samplingHeight']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read station samplingHeight from metadata')
        mdata.append(d)
        try:
            #       projectUrl=pidMetadataCore.specification.project.self.uri 
            # is the station an ICOS station? values are {'1','2','A','no'} class 1, 2, or A(ssociated) or else 'no' meaning it is not an ICOS station
            d='no'
            try:
                value=icosStationLut[sid]
                d=value
            except:
                pass
        except:
            pass
        try:
            projectUrl=pidMetadata['specification']['project']['self']['uri'] 
            d=False
            if(projectUrl == 'http://meta.icos-cp.eu/resources/projects/icos'):
                d=True
            #d=pidMetadata['specification']['project']['self']['uri'] == 'http://meta.icos-cp.eu/resources/projects/icos' # does the self project url point to being an ICOS project?
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read ICOS flag from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['size']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read size from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['nRows']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read nRows from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specification']['dataLevel']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read dataLevel from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['acquisition']['interval']['start']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read acquisition start from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['acquisition']['interval']['stop']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read acquisition stop from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['specificInfo']['productionInfo']['dateTime']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read productionInfo from metadata')
        mdata.append(d)
        try:
            d=pidMetadata['accessUrl']
        except:
            d=''
            #ndr+=1  # we got here via the url, so no drama if we don't have a value we already know
        mdata.append(d)
        try:
            d=pidMetadata['fileName']
        except:
            d=''
            #ndr+=1 # informativ but it is not being used
        mdata.append(d)
        try:
            d=pidMetadata['specification']['self']['label']
        except:
            d=''
            ndr+=1
            logger.debug('Failed to read data label from metadata')
        mdata.append(d)
    except:
        print(f'Failed to get the metadata using icoscp.cpb.metadata.get(url) for url={url}')
        return(None,  False)  # Tell the calling parent routine that things went south...
    if(ndr>1):
        bAcceptable=False
    return (mdata, bAcceptable)



def testPID(pidLst, sOutputPrfx=''):
    nBadies=0
    nBadMeta=0
    nTotal=0
    nGoodPIDs=0
    nBadIcoscpMeta=0
    icosStationLut=myCarbonPortalTools.createIcosStationLut()   
    #print(f'statClassLookup={statClassLookup}')
    #station_basic_meta_lookup = {s.uri: s for s in meta.list_stations()}
    #print(f'station_basic_meta_lookup={station_basic_meta_lookup}')
    nLst=len(pidLst)
    step=int((nLst/10.0)+0.5)
    bPrintProgress=False #True
    sStart='2018-01-01 00:00:00'
    sEnd='2018-02-01 23:59:59'
    pdSliceStartTime=bs.getPdTime(sStart,  tzUT=True)
    pdSliceEndTime=bs.getPdTime(sEnd,  tzUT=True)
    bFirstDf=True
    CrudeErrorEstimate="1.5" # 1.5 ppm
    errorEstimate=CrudeErrorEstimate
    nDataSets=0
    for pid in pidLst:
        fileOk='failed'
        fNamePid='data record not found'
        metaDataRetrieved=True
        datafileFound=False
        icosMetaOk=False
        url="https://meta.icos-cp.eu/objects/"+pid
        (mdata, icosMetaOk)=myCarbonPortalTools.getMetaDataFromPid_via_icoscp_core(pid,  icosStationLut)
        if(mdata is None):
            logger.error(f'Failed: Obtaining the metadata for data object {url} was unsuccessful. Thus this data set cannot be used.')
        if(mdata is None):
            metaDataRetrieved=False
        if(icosMetaOk):
            icoscpMetaOk='   yes'
        else:
            icoscpMetaOk='    no'

        fNamePid=myCarbonPortalTools.getPidFname(pid)
        fileOk='   yes'
        datafileFound=True
        if(fNamePid is None):
            fileOk='    no'
            datafileFound=False
        data=[pid,icoscpMetaOk,  fileOk, fNamePid]+mdata
        if((datafileFound) and (metaDataRetrieved) and(icosMetaOk)):
            if(nGoodPIDs==0):
                '''
                returns a list of these objects: ['stationID', 'country', 'isICOS','latitude','longitude','altitude','samplingHeight','size', 
                        'nRows','dataLevel','obsStart','obsStop','productionTime','accessUrl','fileName','dClass','dataSetLabel'] 
                '''
                columnNames=['pid', 'icoscpMeta.ok', 'file.ok',  'fNamePid','stationID', \
                    'country','is-ICOS', 'latitude','longitude','altitude','samplingHeight','size', 'nRows','dataLevel',\
                    'obsStart','obsStop','productionTime','accessUrl','fileName','dClass','dataSetLabel'] 
                dfgood=DataFrame(data=[data], columns=columnNames)
            else:
                dfgood.loc[len(dfgood)] = data
            nGoodPIDs+=1
        else:
            if(nBadies==0):
                columnNames=['pid', 'icoscpMeta.ok', 'file.ok',  'fNamePid','stationID', \
                    'country','is-ICOS', 'latitude','longitude','altitude','samplingHeight','size', 'nRows','dataLevel',\
                    'obsStart','obsStop','productionTime','accessUrl','fileName','dClass','dataSetLabel'] 
                dfbad=DataFrame(data=[data], columns=columnNames)
                print(f'Data records with some issues:\r\n{columnNames}')
            else:
                dfbad.loc[len(dfbad)] = data
            print(f'{data}')
            nBadies+=1
        nTotal+=1
        if((bPrintProgress) and (nTotal % step ==0)):
            myCarbonPortalTools.printProgressBar(nTotal, nLst, prefix = 'Gathering meta data progress:', suffix = 'Done', length = 50)
        try:
            pidUrl="https://meta.icos-cp.eu/objects/"+pid
            logger.info(f"pidUrl={pidUrl}")
            dob = Dobj(pidUrl)
            print(f'dob={dob}')
            # obsData1site = dob.get()
            dobjMeta = iccMeta.get_dobj_meta(pidUrl)
            #logger.debug(f'dobjMeta={dobjMeta}')
            dobj_arrays = iccData.get_columns_as_arrays(dobjMeta) #, ['TIMESTAMP', 'co2'])
            logger.debug(f'dobj_arrays={dobj_arrays}')
            obsData1site = DataFrame(dobj_arrays)
            logger.info(f"I have read the observed co2 data successfully from: PID={pid} station={dob.station['org']['name']}, located at station latitude={dob.lat},  longitude={dob.lon},  altitude={dob.alt},  elevation={dob.elevation}")
            obsData1site.iloc[:512, :].to_csv('_dbg_icc_'+pid+'-obsData1site-obsPortalDbL188.csv', mode='w', sep=',')  
            try:
                dftest = dob.meta()
                logger.debug(f'dob.meta(PID)= {dftest}')
            except:
                pass
            print('795')
            # logger.info(f"samplingHeight={dob.meta['specificInfo']['acquisition']['samplingHeight']}")
            availColNames = list(obsData1site.columns.values)
            if ('Site' not in availColNames):
                obsData1site.loc[:,'Site'] = dob.station['id'].lower()
            print('800')
            if ( (any('value' in entry for entry in availColNames))  and (any('value_std_dev' in entry for entry in availColNames)) and (any('time' in entry for entry in availColNames)) ):
                # There is an issue with the icoscp package when reading ObsPack data. Although the netcdf files store seconds since 1970-01-01T00:00:00, 
                # what ends up in the pandas dataframe is only the hour of the day without date....
                # Let's read the netcdf file ourselves then... for each PID there are 2 files, one without extension (the netcdf file) and one with .cpb extension
                # example for an ObsPack Lv 2 data set: fn='/data/dataAppStorage/netcdfTimeSeries/bALaWpparMpY6_N-zW2Cia9a'
                fn=myCarbonPortalTools.getPidFname(pid)
                if(fn is None):
                    #datafileFound=False
                    logger.error("Failed to locate the file for data object {pidUrl} on the server. Make sure that the carbon portal file system is mounted at /data/dataAppStorage/")
                    # Remove this pid from the list of used data sets: pid in selectedDobjLst                
                    nBadies+=1 
                else:
                    ds = xr.open_dataset(fn)
                    df = ds.to_dataframe()
                    availColNames = list(df.columns.values)
                    print('816')
                    if ('Site' not in availColNames):
                        df.loc[:,'Site'] = dob.station['id'].lower()
                    # df.iloc[:100, :].to_csv(sTmpPrfx+'_dbg_df-'+pid+'-obsPortalDbL262.csv', mode='w', sep=',')  
                    df = df.reset_index()
                    # df.iloc[:100, :].to_csv(sTmpPrfx+'_dbg_df-'+pid+'-obsPortalDbL264.csv', mode='w', sep=',')  
                    if((any('start_time' in entry for entry in availColNames)) and (any('time' in entry for entry in availColNames))):
                        df.drop(columns=['time'], inplace=True)   #  rename(columns={'time':'halftime'}, inplace=True)
                    if(any('start_time' in entry for entry in availColNames)):
                        if(any('TIMESTAMP' in entry for entry in availColNames)):
                            df.drop(columns=['TIMESTAMP'], inplace=True) # rename(columns={'TIMESTAMP':'timeStmp'}, inplace=True)
                        df.rename(columns={'start_time':'time'}, inplace=True)
                    df.rename(columns={'value':'obs','value_std_dev':'stddev', 'nvalue':'NbPoints'}, inplace=True)
                    print('829')
                    obsData1site=df
                    if(is_float_dtype(obsData1site['obs'])==False):
                        obsData1site['obs']=obsData1site['obs'].astype(float)
                    if(is_float_dtype(obsData1site['stddev'])==False):
                        obsData1site['stddev']=obsData1site['stddev'].astype(float)
                    print('835')
                    # this pid is probably an ObsPack in netcdf format. co2 values are then in mol/mol and must be multiplied by 1e6 to get ppm
                    obsData1site['obs']= 1.0e6*obsData1site['obs']  # from mol/mol to micromol/mol
                    obsData1site['stddev']= 1.0e6*obsData1site['stddev']
                    if ( 'qc_flag'  in entry for entry in availColNames):
                        obsData1site.rename(columns={'qc_flag':'icos_flag'}, inplace=True)
                    else:
                        obsData1site['icos_flag']= 'O'
                    bAllImportantColsArePresent=True
                    print('844')
            elif (any('TIMESTAMP' in entry for entry in availColNames)) :
                    print('846')
                    obsData1site.rename(columns={'TIMESTAMP':'time'}, inplace=True)
            else:
                print('849')
                logger.warning(f"Suspicious data object {pidUrl}  :")
                logger.warning("This data set is missing at least one of the expected columns >> time/TIMESTAMP, obs/co2/value, stdev/value_std_dev or Flag <<. This data object will be ignored.")
                # Remove this pid from the list of used data sets: pid in selectedDobjLst                
                # TODO should also remove entry from dataframe dfObsDataInfo and write an updated copy to csv file.
                nBadies+=1  # Have: icos_LTR, icos_SMR, icos_STTB, icos_datalevel, qc_flag, time, value, value_std_dev, Site
            if (any('Flag' in entry for entry in availColNames)):
                obsData1site.rename(columns={'Flag':'icos_flag'}, inplace=True)
            print('857')
            if (any('Stdev' in entry for entry in availColNames)):
                obsData1site.rename(columns={'Stdev':'stddev'}, inplace=True)
            if (any('co2' in entry for entry in availColNames)):
                obsData1site.rename(columns={'co2':'obs'}, inplace=True)
            availColNames = list(obsData1site.columns.values)    
            if ( (any('time' in entry for entry in availColNames))  and (any('obs' in entry for entry in availColNames)) and (any('stddev' in entry for entry in availColNames))   and (any('icos_flag' in entry for entry in availColNames))):
                bAllImportantColsArePresent=True
                for col in ['calendar_components','dim_concerns','datetime','time_components','solartime_components','instrument','icos_datalevel','obs_flag','assimilation_concerns']:
                   if (any(col in entry for entry in availColNames)): 
                        obsData1site.drop(columns=[col], inplace=True)
            print('868')
            if(bAllImportantColsArePresent==True):
                # grab only the time interval needed. This reduces the amount of data drastically if it is an obspack.
                print('871')
                obsData1siteTimed = obsData1site.loc[(
                    (obsData1site.time >= pdSliceStartTime) &
                    (obsData1site.time <= pdSliceEndTime) &
                    (obsData1site['NbPoints'] > 0)
                )]  
                print('877')
                # obsData1siteTimed.to_csv(sTmpPrfx+'_dbg_obsData1siteTimed-obsPortalDbL310.csv', mode='w', sep=',')  
                # TODO: one might argue that 'err' should be named 'err_obs' straight away, but in the case of using a local
                # observations.tar.gz file, that is not the case and while e.g.uncertainties are being set up, the name of 'err' is assumed 
                # for the name of the column  containing the observational error in that dataframe and is only being renamed later.
                # Hence I decided to mimic the behaviour of a local observations.tar.gz file
                # Parameters like site-code, latitude, longitude, elevation, and sampling height are not present as data columns, but can be 
                # extracted from the header, which really is written as a comment. Thus the following columns need to be created: 
                fn=dob.info['fileName']
                # 'SamplingHeight':'height' (taken from metadata)
                obsData1siteTimed.loc[:,'site'] = dob.station['id'].lower()
                obsData1siteTimed.loc[:,'lat']=dob.lat
                obsData1siteTimed.loc[:,'lon']=dob.lon
                obsData1siteTimed.loc[:,'alt']=dob.alt
                obsData1siteTimed.loc[:,'height']=dob.meta['specificInfo']['acquisition']['samplingHeight']
                print('892')
                # site name/code is in capitals, but needs conversion to lower case:
                obsData1siteTimed.loc[:,'code'] = dob.station['id'].lower()
                if ('Site' not in availColNames):
                    obsData1siteTimed.loc[:,'Site'] = dob.station['id'].lower()
                # and the Time format has to change from "2018-01-02 15:00:00" to "20180102150000"
                # Note that the ['TIMESTAMP'] column is a pandas.series at this stage, not a Timestamp nor a string
                # I tried to pull my hair out converting the series into a timestamp object or similar and format the output,
                # but that is not necessary. When reading a local tar file with all observations, it is also a pandas series object, 
                # (not a timestamp type variable) and since I'm reading the data here directly into the dataframe, and not 
                # elsewhere, no further changes are required.
                print('901')
                logger.info(f"obsData1siteTimed= {obsData1siteTimed}")
                logger.debug(f"columns present:{obsData1siteTimed.columns}")
                availColNames = list(obsData1siteTimed.columns.values)
                if ('obs' in availColNames):
                    print('906')
                    if(bFirstDf):
                        allObsDfs= obsData1siteTimed.copy()
                    else:
                        allObsDfs= concat([allObsDfs, obsData1siteTimed])
                    # Now let's create the list of sites and store it in self....
                    # The example I have from the observations.tar.gz files looks like this:
                    # site,code,name,lat,lon,alt,height,mobile,file,sitecode_CSR,err
                    # trn,trn,Trainou,47.9647,2.1125,131.0,180.0,,/proj/inversion/LUMIA/observations/eurocom2018/rona/TRN_180m_air.hdf.all.COMBI_Drought2018_20190522.co2,dtTR4i,1.5
                    # sFileNameOnCarbonPortal = ICOS_ATC_L2_L2-2022.1_TOH_147.0_CTS_CO2.zip
                    print('916')
                    sFileNameOnCarbonPortal = dob.meta['fileName']
                    logger.info(f"station name    : {dob.station['org']['name']}")
                    logger.info(f"PID             : {pid}")
                    logger.info(f"file name (csv) : {dob.meta['fileName']}")
                    logger.info(f"access url      : {dob.meta['accessUrl']}")
                    # logger.info(f"mobile flag: {}")
                    mobileFlag=None
                    #scCSR=getSitecodeCsr(dob.station['id'].lower())
                    #logger.info(f"sitecode_CSR: {scCSR}")
                    # 'optimize.observations.uncertainty.type' key to 'dyn' (setup_uncertainties in ui/main_functions.py, ~l174)                    
                    data =( {
                      "site":dob.station['id'].lower() ,
                      "code": dob.station['id'].lower(),
                      "name": dob.station['org']['name'] ,
                      "dataObject": pidUrl ,
                      "fnameUrl": dob.meta['accessUrl'] ,
                      "lat": dob.lat,
                      "lon":dob.lon ,
                      "alt": dob.alt,
                      "height": dob.meta['specificInfo']['acquisition']['samplingHeight'],
                      "mobile": mobileFlag,
                      "file": sFileNameOnCarbonPortal,
                      "err": errorEstimate
                    })
                    #  "sitecode_CSR": scCSR,
                    print('942')
                    df = DataFrame([data])                    
                    #if(bFirstDf):
                    #    df.to_csv('mySites.csv', encoding='utf-8', sep=',', mode='w')
                    #else:
                    #    df.to_csv('mySites.csv', encoding='utf-8', sep=',', mode='a', header=False)
                    if(bFirstDf):
                        allSitesDfs = df.copy()
                        bFirstDf=False
                    else:
                        allSitesDfs = concat([allSitesDfs, df])
                    nDataSets+=1
                else:
                    logger.error("Houston we have a problem. This datafame has no column by the name of >>obs<<. Please check (all) your dry mole fraction observational files for the selected tracer.")
                    logger.info(f"Available columns in the file with pid= {pid} are: {availColNames}")
                    logger.error(f"Data from pid={pid} is discarded.")
        except:
            logger.error(f"Error: reading of the Dobj failed for pidUrl={pidUrl}.")
            traceback.print_exc()
    if(nBadies > 0):
        logger.warning(f"A total of {nBadies} PIDs out of {nTotal} data objects had some issues,  thereof {nBadMeta} had bad dobjMetaData and {nBadIcoscpMeta} had bad icoscMetaData.")
        dfbad.to_csv(sOutputPrfx+'bad-PIDs-testresult.csv', encoding='utf-8', mode='w', sep=',')
        logger.info(f'Bad PIDs with some issues have been written to {sOutputPrfx}bad-PIDs-testresult.csv')
    if(nGoodPIDs > 0):
        dfgood.to_csv(sOutputPrfx+'good-PIDs-testresult.csv', encoding='utf-8', mode='w', sep=',')
        logger.info(f'Good PIDs ()with all queried properties found) have been written to {sOutputPrfx}good-PIDs-testresult.csv')
        

def readLstOfPids(pidFile):
    with open(pidFile) as file:
       selectedDobjLst = [line.rstrip() for line in file]
    # read the observational data from all the files in the dobjLst. These are of type ICOS ATC time series
    if((selectedDobjLst is None) or (len(selectedDobjLst)<1)):
        logger.error(f"Fatal Error! User specified pidFile {pidFile} is empty.")
        sys.exit(-1)
    return(selectedDobjLst)


def runPidTest(pidFile, sOutputPrfx):
    if('.csv'==pidFile[-4:]):
        dfs = read_csv (pidFile)
        try:
            pidLst = dfs["pid"].values.tolist()
        except:
            logger.error(f"Failed to extract the >>pid<< column from your input csv file {pidFile}.")
            sys.exit(-1)
    else:
        try:
            pidLst=readLstOfPids(pidFile)
        except:
            logger.error(f"Failed to read the PIDs from your input file {pidFile}.")
            sys.exit(-1)
    testPID(pidLst, sOutputPrfx)
    print('Done. testPID task completed.')

'''
2024-03-28 12:32:28.520 | ERROR    | lumia.obsdb.obsCPortalDb:gatherObs_fromCPortal:407 - Error: reading of the Dobj failed for pidUrl=https://meta.icos-cp.eu/objects/QeqT9ATwpxrCvd159Djt2eFr. 
2024-03-28 12:32:40.182 | ERROR    | lumia.obsdb.obsCPortalDb:gatherObs_fromCPortal:407 - Error: reading of the Dobj failed for pidUrl=https://meta.icos-cp.eu/objects/FPidg1GxfRN-xBbP-BBRGSqf.
2024-03-28 12:32:41.909 | ERROR    | lumia.obsdb.obsCPortalDb:gatherObs_fromCPortal:407 - Error: reading of the Dobj failed for pidUrl=https://meta.icos-cp.eu/objects/WqyL0AmwwqhaRLYCZuDHH1Ln.

'''

p = argparse.ArgumentParser()
p.add_argument('--pidLst', default=None,  help='Name of a text file listing the carbon portal PIDs (as a single column) that you wish to test.')
#p.add_argument('--fDiscoveredObs', dest='fDiscoveredObservations', default=None,  help="If step2 is set you must specify the .csv file that lists the observations discovered by LUMIA, typically named DiscoveredObservations.csv")   # yaml configuration file where the user plans his or her Lumia run: parameters, input files etc.
p.add_argument('--verbosity', '-v', dest='verbosity', default='INFO')
p.add_argument('--sOutputPrfx', '-o', default='', help='Output file names will be prefixed with your string -- your string may contain a path or a partial name or both')
args, unknown = p.parse_known_args(sys.argv[1:])

# Set the verbosity in the logger (loguru quirks ...)
logger.remove()
logger.add(sys.stderr, level=args.verbosity)

myMachine=platform.node()
myPlatformCore=platform.platform()  # Linux-5.15.0-89-generic-x86_64-with-glibc2.35
myPlatformFlavour=platform.version() #99-Ubuntu SMP Mon Oct 30 20:42:41 UTC 2023
print(f'node={myMachine}')
print(f'platform={myPlatformCore}')
print(f'platformFlavour={myPlatformFlavour}')
#testGui()
#sys.exit(0)

if(args.pidLst is None):
    #logger.error("testPID: Fatal error: no user configuration (yaml) file provided.")
    logger.error("testPID: Fatal error: no csv /PID file provided.")
    sys.exit(1)
else:
    pidFile = args.pidLst

if (not os.path.isfile(pidFile)):
    logger.error(f"Fatal error in testPID: User specified pidLst file {pidFile} does not exist. Abort.")
    sys.exit(-3)

runPidTest(pidFile, args.sOutputPrfx)

'''
...some wisdom from the horse's mouth...

Hi all,

Arndt, "meta" is a property on a Dobj class, not a function, you must not use parentheses after it.

When it comes to the 'coverageGeo' property, it's an arbitrary GeoJSON (which is an official standard, actually).
Depending on the data object, the GeoJSON may look differently. So, you cannot always count on availability of "geometry" property.

ObservationStation geo-location -- obtaining latitude, longitude, altitude reliably:
==========================
coverageGeo is an attribute of "meta" property, but yes, I would probably mostly recommend using for the purposes of visualization, not robust station location lookup. 
For the latter, one should keep in mind that in general, not all data object have geostationary station they are associated with. But, for atmospheric station measurement
data, when using icoscp lib, you could write something like

station_location = dob.meta['specificInfo']['acquisition']['station']['location']

When using icoscp_core lib, one take two approaches. If one is only interested in a single data object, the full example code could be

from icoscp_core.icos import meta
dob_meta = meta.get_dobj_meta('https://meta.icos-cp.eu/objects/uWgpKU-q44efg-KX7_H3GHAY')
station_location = dob_meta.specificInfo.acquisition.station.location

But when working with many objects, it's more performant to list all stations first, and make a lookup table from station URI to the station object:

station_lookup = {s.uri: s for s in meta.list_stations()}

This dictionary can then be used to quickly look up all kinds of metadata information about a station, based on station URI. Then one does not need to fetch full metadata 
for every data object one works with (this is relatively slow operation, especially if you work with many objects).

is-ICOS-flag:
==============
Arndt, answering your question about ICOS/non-ICOS data object discrimination while I am at it: when using icoscp, you can write

is_icos = (dob.meta['specification']['project']['self']['uri'] == 'http://meta.icos-cp.eu/resources/projects/icos')

(mind the "http", not "https", in the URL), and for icoscp_core the syntax would be

is_icos = (dob_meta.specification.project.self.uri == 'http://meta.icos-cp.eu/resources/projects/icos')

(again, no need for the quotes and brackets, plus you get autocomplete support)

Hope this helps. Best regards,
/Oleg.
'''
