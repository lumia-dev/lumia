import re
import tkinter as tk 
try:
    import housekeeping as hk
except:
    import lumia.GUI.housekeeping as hk
import os
import sys
from loguru import logger
from matplotlib import font_manager
from screeninfo import get_monitors
# from PIL import Image
from PIL import  ImageFont  #, ImageDraw
#from Pillow import ImageFont, ImageDraw
from pandas import to_datetime

MIN_SCREEN_WIDTH=1920 # pxl - just in case querying screen size fails for whatever reason...
MIN_SCREEN_HEIGHT=1080 # pxl - just in case querying screen size fails for whatever reason...

# =============================================================================
# small helper functions for mostly mundane tasks - ordered alphabetically by function name
# =============================================================================


def add_keys_nested_dict(d, keys, value=None):
    for key in keys:
        if key not in d:
            d[key] = {}
        d = d[key]
    d.setdefault(keys[-1], value)


def calculateEstheticFontSizes(sFontFamily,  iAvailWidth,  iAvailHght, sLongestTxt,  nCols=1, nRows=1, xPad=20,
                                                        yPad=10,  maxFontSize=20,  USE_TKINTER=True,  bWeCanStackColumns=False):
    FontSize=int(14)  # for normal text
    bFontFound=False
    bSuccess=False
    w=0
    h=25
    fontHeight=h
    system_fonts = font_manager.findSystemFonts(fontpaths=None, fontext='ttf')
    myfontpath=''
    for fontpath in system_fonts:
        if(re.search(sFontFamily, fontpath,  re.IGNORECASE)):
            if(re.search('bold', fontpath,  re.IGNORECASE) or 
                re.search('italic', fontpath,  re.IGNORECASE) or
                re.search('condensed', fontpath,  re.IGNORECASE)):
                    pass
            else:
                myfontpath=fontpath
                bFontFound=True
                break
    if(len(myfontpath)<10):
        return(bFontFound, 9, 10, 12, 15, 19, 24, fontHeight, False, bSuccess) # and hope for the best....
        logger.warning(f'TTF font for FontFamily {sFontFamily} not found on system. Defaulting to font size 12 as a base size.')
    if(USE_TKINTER):
        try:
            font = tk.font.Font(family=sFontFamily, size=FontSize)
        except:
            logger.warning('Hook to tk app invalid. Defaulting to font size 12 as a base size.')
            return(bFontFound, 9, 10, 12, 15, 19, 24, fontHeight, False, bSuccess) # and hope for the best....
    else:
        try:
            font = ImageFont.truetype(fontpath, FontSize)
        except:
            logger.warning(f'TTF font for FontFamily {sFontFamily} not found on system. Defaulting to font size 12 as a base size.')
            return(False, 9, 10, 12, 15, 19, 24, fontHeight, False, bSuccess)
        (left, top, right, bottom)=font.getbbox(sLongestTxt, anchor="ls")
        w=right - left
        h=abs(top - bottom)
    tooSmall=8 # pt
    bCanvasTooSmall=False
    bWeMustStack=False
    colW=int(iAvailWidth/nCols)-(0.5*xPad)+0.5
    colH=int( iAvailHght/nRows)-(0.5*yPad)+0.5
    # logger.debug(f"avail colWidth= {colW} pxl: colHeight= {colH} pxl")
    if(USE_TKINTER):
        (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
    else:
        font = ImageFont.truetype(fontpath, FontSize)
        (left, top, right, bottom)=font.getbbox(sLongestTxt, anchor="ls")
        w=right - left
        h=abs(top - bottom)
    # logger.debug(f"{sFontFamily}, {FontSize}pt: (w={w},h={h})pxl")
    # Make the font smaller until the text fits into one column width
    while(w>colW):
        FontSize=FontSize-1
        if(USE_TKINTER):
            font = tk.font.Font(family=sFontFamily, size=FontSize)
            (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
        else:
            font = ImageFont.truetype(fontpath, FontSize)
            (left, top, right, bottom)=font.getbbox(sLongestTxt, anchor="ls")
            w=right - left
            h=abs(top - bottom)
        if (FontSize==tooSmall):
            FontSize=FontSize+1
            if(USE_TKINTER):
                font = tk.font.Font(family=sFontFamily, size=FontSize)
                (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
            else:
                font = ImageFont.truetype(fontpath, FontSize)
                (left, top, right, bottom)=font.getbbox(sLongestTxt, anchor="ls")
                w=right - left
                h=abs(top - bottom)
            bCanvasTooSmall=True
            break
        # logger.debug(f"{sFontFamily}, {FontSize}pt: (w={w},h={h})pxl")
    # If the screen is too small, check if we could stack the output vertically using fewer columns
    if((bCanvasTooSmall) and (bWeCanStackColumns) and (nCols>1)):
        nCols=int((nCols*0.5)+0.5)
        if(USE_TKINTER):
            font = tk.font.Font(family=sFontFamily, size=FontSize)
            (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
        else:
            font = ImageFont.truetype(fontpath, FontSize)
            (left, top, right, bottom)=font.getbbox(sLongestTxt, anchor="ls")
            w=right - left
            h=abs(top - bottom)
        if(w<colW+1):
            bCanvasTooSmall=False
            bWeMustStack=True
    # Now make the font as big as we can
    bestFontSize=FontSize
    while((FontSize<maxFontSize)): # and (not bMaxReached) ):
        FontSize=FontSize+1
        if(USE_TKINTER):
            font = tk.font.Font(family=sFontFamily, size=FontSize)
            (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
        else:
            font = ImageFont.truetype(fontpath, FontSize)
            (left, top, right, bottom)=font.getbbox(sLongestTxt, anchor="ls")
            w=right - left
            h=abs(top - bottom)
        if(w<=colW):
            bestFontSize=FontSize
    FontSize=bestFontSize
    if(USE_TKINTER):
        (w,h) = (font.measure(sLongestTxt),font.metrics("linespace"))
    else:
        font = ImageFont.truetype(fontpath, FontSize)
        (left, top, right, bottom)=font.getbbox(sLongestTxt, anchor="ls")
        w=right - left
        h=abs(top - bottom)
    logger.debug(f"{sFontFamily} {FontSize}: (w={w},h={h})pxl")
    fontHeight=h
    if(fontHeight>colH):
        logger.debug("We may need a vertical scrollbar...")
    fsNORMAL=FontSize # 12
    fsTINY=int((0.75*FontSize)+0.5)  # 9/12=0.75
    fsSMALL=int((0.833333*FontSize)+0.5)  # 10/12=0.833333
    fsLARGE=int((1.25*FontSize)+0.5)  # 15
    fsHUGE=int((1.5833333*FontSize)+0.5)  # 19
    fsGIGANTIC=int(2*FontSize)  # 24
    bSuccess=True
    logger.debug(f"fsSMALL={fsSMALL},fsNORMAL={fsNORMAL},fsLARGEL={fsLARGE},fsHUGE={fsHUGE}")
    return(bFontFound, fsTINY,  fsSMALL,  fsNORMAL,  fsLARGE,  fsHUGE,  fsGIGANTIC, fontHeight, bWeMustStack, bSuccess)


def cleanUp(self,  bWriteStop=True):  # of lumiaGuiApp
    if(bWriteStop): # the user selected Cancel - else the LumiaGui.go message has already been written
        print('cleanUp: Removing junk.')
        ymlFile=self.initialYmlFile
        logger.info("LumiaGUI was canceled.")
        sCmd="touch LumiaGui.stop"
        hk.runSysCmd(sCmd)
        sCmd="cp "+ymlFile+'.bac '+ymlFile # recover from most recent backup file.
        hk.runSysCmd(sCmd)
        # Delete junk files that just clutter up the storage space
        junkFiles=[f'{self.sOutputPrfx}python-environment-pipLst.txt',  f'{self.sOutputPrfx}selected-ObsData-co2.csv', 
                          f'{self.sOutputPrfx}selected-PIDs-co2.csv',  f'{self.fDiscoveredObservations}', 
                          f'{self.ymlFile}',  f'{self.ymlFile}.bac', 
                          f'{self.sTmpPrfx}_dbg_newDfObs.csv', f'{self.sTmpPrfx}_dbg_selectedObs.csv'
                          ]
        for jf in junkFiles:
            if(os.path.isfile(jf)):  # if the file (already) exists:
                os.remove(jf) #sCmd="rm "+jf # delete this junk file #hk.runSysCmd(sCmd)
        junkDirs=[self.sOutputPrfx, self.sTmpPrfx]
        for sJD in junkDirs:
            sDir = os.path.dirname(sJD)
            os.rmdir(sDir)  # sCmd="rmdir "+sDir #hk.runSysCmd(sCmd)
        print('cleanUp: Finished cleaning up junk files.')
        


def displayGeometry(USE_TKINTER, maxAspectRatio=1.778):  
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
        if (screenWidth < MIN_SCREEN_WIDTH):
            screenWidth=MIN_SCREEN_WIDTH
            screenHeight=MIN_SCREEN_HEIGHT
            xoffset=1
            
    except:
        if(USE_TKINTER):
            logger.error('screeninfo.get_monitors() failed. Try installing the libxrandr2 library if missing. Setting display to 1080x960pxl')
        screenWidth= int(1080) # not a drama since ipywidgets work on relative screen width
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
    if(maxW > maxAspectRatio*maxH):
        maxW = int((maxAspectRatio*maxH)+0.5)
        logger.debug(f'maxW={maxW},  maxH={maxH},  max aspect ratio fix.')
    if(xoffset==0):
        xoffset=0.5*(screenWidth-maxW) # helps us to place the gui horizontally in the center of the screen
    # Sets the dimensions of the window to something reasonable with respect to the user's screen properties
    xPadding=int(0.008*maxW)
    yPadding=int(0.008*maxH)
    return(maxW, maxH, xPadding, yPadding, xoffset)


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


def getPdTime(timeStr,  tzUT=False):
    timeStr=removeQuotesFromString(timeStr)
    pdTime=None
    if('.000' in timeStr[-5:]):  # strip microseconds
        timeStr=timeStr[:-4]
    try:
        pdTime = to_datetime(timeStr, format="%Y-%m-%d %H:%M:%S")
    except:
        try:
            pdTime = to_datetime(timeStr, format="%Y,%m,%d %H:%M:%S") # sStart=2018,01,01 00:00:00.000
        except:
            try:
                pdTime = to_datetime(timeStr, format="%Y,%m,%d %H,%M,%S") # sStart=2018,01,01 00,00,00.000
            except:
                logger.error(f'Failed to extract a meaningful pd.to_datetime from string timeStr={timeStr}.')
                sys.exit(-73)
    if(tzUT):
        pdTime=pdTime.tz_localize('UTC')
    return(pdTime)


def grabFirstEntryFromList(myList):
  try:
    return myList[0]  
  except:
    return myList



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

def removeQuotesFromString(str):
    snglquote="'"
    idx = str.find(snglquote)    
    while(idx >-1):
        if (idx==0):
            str=str[1:]
        elif(idx==len(str)-1):
            str=str[:-1]
        else:
            str=str[:idx-1]+str[idx+1:]
        idx = str.find(snglquote)
    return(str)

# Plan the layout of the GUI - get screen dimensions, choose a reasonable font size for it, xPadding, etc.
def stakeOutSpacesAndFonts(guiWindow, nCols, nRows, USE_TKINTER,  sLongestTxt="Start date (00:00h):",  maxAspectRatio=1.2):
    guiWindow.appWidth, guiWindow.appHeight,  guiWindow.xPadding, guiWindow.yPadding,  guiWindow.xoffset = displayGeometry(USE_TKINTER, maxAspectRatio=maxAspectRatio)
    wSpacer=2*guiWindow.xPadding
    guiWindow.vSpacer=2*guiWindow.yPadding
    likedFonts=["Georgia", "Liberation","Arial", "Microsoft","Ubuntu","Helvetica"]
    for myFontFamily in likedFonts:
        (bFontFound, guiWindow.fsTINY,  guiWindow.fsSMALL,  guiWindow.fsNORMAL,  guiWindow.fsLARGE,  guiWindow.fsHUGE,  guiWindow.fsGIGANTIC,  guiWindow.fontHeight,  bWeMustStackColumns, bSuccess)= \
            calculateEstheticFontSizes(myFontFamily,  guiWindow.appWidth,  guiWindow.appHeight, sLongestTxt, nCols, nRows, 
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


def swapListElements(list, pos1, pos2):
    # pos1 and pos2 should be counted from zero
    if(pos1 != pos2):
        list[pos1], list[pos2] = list[pos2], list[pos1]
    return list



