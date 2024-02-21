#!/usr/bin/env python

import os
import sys
import argparse
import housekeeping as hk
import pandas as pd
from time import  sleep
from loguru import logger
from screeninfo import get_monitors
USE_TKINTER=True
scriptName=sys.argv[0]
if('.ipynb' in scriptName[-6:]):
    USE_TKINTER=False
# For testing of ipywidgets uncomment the next line (even if not a notebook)
USE_TKINTER=False
import boringStuff as bs
MIN_SCREEN_RES=480 # pxl - just in case querying screen size fails for whatever reason...

def prepareCallToLumiaGUI(iVerbosityLv='DEBUG'):
    myDsp=os.environ.get('DISPLAY','')
    if (myDsp == ''):
        logger.warning('DISPLAY not listed in os.environ. On simple systems DISPLAY is usually :0.0  ...so I will give that one a shot. Proceeding with fingers crossed...')
        os.environ.__setitem__('DISPLAY', ':0.0')
    else:
        logger.debug(f'found Display {myDsp}')

    root=None
    if(USE_TKINTER):
        root = ge.LumiaGui() #tk.Tk()
    else:
        root = ge.LumiaGui
        notify_output = widgets.Output()
        display(notify_output)
    lumiaGuiAppInst=lumiaGuiApp(root)
    lumiaGuiAppInst.waitForTasksToComplete=True
    guiPg1TpLv=lumiaGuiAppInst.guiPage1AsTopLv(iVerbosityLv)
    #lumiaGuiAppInst.runPage2(iVerbosityLv)  # execute the central method of lumiaGuiApp
    if(USE_TKINTER):
        root.mainloop()
    else:
        while(lumiaGuiAppInst.waitForTasksToComplete):
            sleep(1.1) # 1.1 seconds
    sys.exit(0)
    '''
    (bFirstGuiPageSuccessful, ymlContents)=LumiaGuiPage1(root, sLogCfgPath, ymlContents, ymlFile, bRefine=False, iVerbosityLv=1)

    # Go hunt for the data

    # Present to the user the data sets found
    if(USE_TKINTER):
        root=guiPages[1]
    (bFirstGuiPageSuccessful, ymlContents)=LumiaGuiPage1(root,  sLogCfgPath, ymlContents, ymlFile, bRefine=True, iVerbosityLv=1)
    '''
    logger.info('LumiaGUI completed successfully. The updated Lumia config file has been written to:')
    #logger.info(ymlFile)
    return

class lumiaGuiApp:
    def __init__(self, root):
        self.root = root
        self.guiPg1TpLv=None
        if(USE_TKINTER):
            self.label1 = tk.Label(self.root, text="App main window - hosting the second GUI page.")
            self.root.protocol("WM_DELETE_WINDOW", self.closeApp)
        
    def closeTopLv(self, bWriteStop=True):  # of lumiaGuiApp
        if(USE_TKINTER):
            self.guiPg1TpLv.destroy()
        if(bWriteStop):
            bs.cleanUp(bWriteStop)
            logger.info('lumiaGUI canceled by user.')
            self.closeApp(False)
        self.guiPg1TpLv=None
        if(USE_TKINTER):
            self.root.deiconify()
        self.runPage2()  # done in parent method

    def EvHdPg1GotoPage2(self):
        if(USE_TKINTER):
            self.guiPg1TpLv.iconify()
        fDiscoveredObservations='DiscoveredObservations.csv'
        for i in range (500):
            logger.debug(f'{i} printing a silly line to spend some time - just for testing.....')
            dfAllObs = pd.read_csv (fDiscoveredObservations)
        self.closeTopLv(bWriteStop=False)

    def closeApp(self, bWriteStop=True):  # of lumiaGuiApp
        bs.cleanUp(bWriteStop)
        logger.info("Closing the GUI...")
        if(USE_TKINTER):
            self.root.destroy()
        if(bWriteStop):
            logger.info('lumiaGUI canceled by user.')
        else:
            # TODO: write the GO message to file
            logger.info('LumiaGUI completed successfully. The updated Lumia config file has been written to: ')
        self.waitForTasksToComplete=False
        sys.exit(0)

    def guiPage1AsTopLv(self, iVerbosityLv='INFO'):  # of lumiaGuiApp
        if(self.guiPg1TpLv is None):
            self.guiPg1TpLv = ge.guiToplevel()#,  bg="cadet blue")
        if(USE_TKINTER):
            self.root.iconify()
        # Plan the layout of the GUI - get screen dimensions, choose a reasonable font size for it, xPadding, etc.
        nCols=5 # sum of labels and entry fields per row
        nRows=13 # number of rows in the GUI
        bs.stakeOutSpacesAndFonts(self.root, nCols, nRows, USE_TKINTER)
        # this gives us self.root.colWidth self.root.rowHeight, self.myFontFamily, self.fontHeight, self.fsNORMAL & friends
        xPadding=self.root.xPadding
        yPadding=self.root.yPadding
        # Dimensions of the window
        appWidth, appHeight = self.root.appWidth, self.root.appHeight
        # action if the gui window is closed by the user (==canceled)
        self.nCols=3
        if(USE_TKINTER):
            self.guiPg1TpLv.protocol("WM_DELETE_WINDOW", self.closeTopLv)
            # set the size of the gui window before showing it
            #self.root.xoffset=int(0.5*1920)
            self.root.geometry(f"{appWidth}x{appHeight}+{self.root.xoffset}+0")   
            sufficentHeight=int(((nRows+1)*self.root.fontHeight*1.88)+0.5)
            if(sufficentHeight < appHeight):
                appHeight=sufficentHeight
            self.guiPg1TpLv.geometry(f"{appWidth}x{appHeight}")
            self.root.iconify()
        
        title="LUMIA  --  Configure your next LUMIA run"
        self.guiPg1TpLv.Pg1TitleLabel = ge.guiTxtLabel(self.guiPg1TpLv, title, fontName=self.root.myFontFamily, fontSize=self.root.fsGIGANTIC, style="bold")
        if(USE_TKINTER):
            self.guiPg1TpLv.protocol("WM_DELETE_WINDOW", self.closeTopLv)
        #self.guiPg1TpLv.button3 = tk.Button(self.guiPg1TpLv, text="Go to page 2", command=self.EvHdPg1GotoPage2)
        self.guiPg1TpLv.button3 = ge.guiButton(self.guiPg1TpLv, text="Go to page 2",  command=self.EvHdPg1GotoPage2) 
        ge.guiPlaceWidget(self.guiPg1TpLv.Pg1TitleLabel, row=0, column=1, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        ge.guiPlaceWidget(self.guiPg1TpLv.button3, row=1, column=1, columnspan=1,padx=xPadding, pady=yPadding, sticky="ew")
        if(not USE_TKINTER):
            self.guiPg1TpLv.button3.on_click(self.EvHdPg1GotoPage2)
            self.guiPg1TpLv.button3

    def runPage2(self, iVerbosityLv='INFO'):  # of lumiaGuiApp
        self.button2 = ge.guiButton(self.root, text="Exit",  command=self.closeApp) 
        #self.button2 = tk.Button(self.root, text="Exit", command=self.closeApp)
        ge.guiPlaceWidget(self.button2, row=1, column=1, columnspan=1)
        if(not USE_TKINTER):
            self.button2.on_click(self.closeApp)
            self.button2

    def cleanUp(self,  bWriteStop=True):  # of lumiaGuiApp
        if(bWriteStop): # the user selected Cancel - else the LumiaGui.go message has already been written
            logger.info("LumiaGUI was canceled.")
            sCmd="touch LumiaGui.stop"
            hk.runSysCmd(sCmd)

p = argparse.ArgumentParser()
p.add_argument('--rcf', dest='rcf', default=None, help="Same as the --ymf option. Deprecated. For backward compatibility only.")   
p.add_argument('--ymf', dest='ymf', default=None,  help='yaml configuration file where the user plans his or her Lumia run: parameters, input files etc.')   
p.add_argument('--serial', '-s', action='store_true', default=False, help="Run on a single CPU")
p.add_argument('--noTkinter', '-n', action='store_true', default=False, help="Do not use tkinter (=> use ipywidgets)")
p.add_argument('--verbosity', '-v', dest='verbosity', default='INFO')
args, unknown = p.parse_known_args(sys.argv[1:])

if(args.noTkinter):
    USE_TKINTER=False
if(USE_TKINTER):
    import guiElementsTk as ge
    import tkinter as tk    
    import customtkinter as ctk
else:
    import guiElements_ipyWdg as ge
    from IPython.display import display, HTML,  clear_output
    import ipywidgets as widgets
    from ipywidgets import  Dropdown, Output, Button, FileUpload, SelectMultiple, Text, HBox, IntProgress
prepareCallToLumiaGUI('DEBUG')
