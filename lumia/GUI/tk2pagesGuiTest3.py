#!/usr/bin/env python

import os
import sys
import housekeeping as hk
import pandas as pd

from loguru import logger
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


def prepareCallToLumiaGUI(iVerbosityLv='DEBUG'):
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
        guiPg1TpLv=lumiaGuiAppInst.guiPage1AsTopLv(iVerbosityLv)
        lumiaGuiAppInst.runPage2(iVerbosityLv)  # execute the central method of lumiaGuiApp
        root.mainloop()
        sys.exit(0)
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
    #logger.info(ymlFile)
    return

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
        #self.runPage2()  # done in parent method

    def EvHdPg1GotoPage2(self):
        self.guiPg1TpLv.iconify()
        fDiscoveredObservations='DiscoveredObservations.csv'
        for i in range (500):
            logger.debug(f'{i} printing a silly line to spend some time - just for testing.....')
            dfAllObs = pd.read_csv (fDiscoveredObservations)
        self.closeTopLv(bWriteStop=False)


    def closeApp(self, bWriteStop=True):  # of lumiaGuiApp
        bs.cleanUp(bWriteStop)
        logger.info("Closing the GUI...")
        self.root.destroy()
        if(bWriteStop):
            logger.info('lumiaGUI canceled by user.')
        else:
            # TODO: write the GO message to file
            logger.info('LumiaGUI completed successfully. The updated Lumia config file has been written to: ')
        sys.exit(0)

    def guiPage1AsTopLv(self, iVerbosityLv='INFO'):  # of lumiaGuiApp
        if(self.guiPg1TpLv is None):
            self.guiPg1TpLv = tk.Toplevel(self.root,  bg="cadet blue")
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
        self.guiPg1TpLv.protocol("WM_DELETE_WINDOW", self.closeTopLv)
        # set the size of the gui window before showing it
        if(USE_TKINTER):
            #self.root.xoffset=int(0.5*1920)
            self.root.geometry(f"{appWidth}x{appHeight}+{self.root.xoffset}+0")   
            sufficentHeight=int(((nRows+1)*self.root.fontHeight*1.88)+0.5)
            if(sufficentHeight < appHeight):
                appHeight=sufficentHeight
            self.guiPg1TpLv.geometry(f"{appWidth}x{appHeight}")
        self.root.iconify()
        
        self.guiPg1TpLv.label2 = tk.Label(self.guiPg1TpLv, text="I'm your page1 toplevel window.")
        self.guiPg1TpLv.protocol("WM_DELETE_WINDOW", self.closeTopLv)
        self.guiPg1TpLv.button3 = tk.Button(self.guiPg1TpLv, text="Go to page 2", command=self.EvHdPg1GotoPage2)
        self.guiPg1TpLv.label2.pack()
        self.guiPg1TpLv.button3.pack()

    def runPage2(self, iVerbosityLv='INFO'):  # of lumiaGuiApp
        self.button2 = tk.Button(self.root, text="Exit", command=self.closeApp)
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
        ctk.set_default_color_theme("/home/arndt/dev/lumia/lumiaDA/lumia/lumia/GUI/doc/lumia-dark-theme.json")
        ctk.CTk.__init__(self)
        # self.geometry(f"{maxW+1}x{maxH+1}")   is set later when we know about the screen dimensions
        self.title('LUMIA - the Lund University Modular Inversion Algorithm')
        self.grid_rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.activeTextColor='gray10'
        self.inactiveTextColor='gray50'

prepareCallToLumiaGUI()
