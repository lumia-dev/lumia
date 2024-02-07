import re
#import housekeeping as hk
import tkinter as tk 
from loguru import logger
#import Pillow
from PIL import Image
from PIL import  ImageFont, ImageDraw
from matplotlib import font_manager
#from Pillow import ImageFont, ImageDraw

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
        return(bFontFound, 9, 10, 12, 15, 19, 24, False) # and hope for the best....
        logger.warning(f'TTF font for FontFamily {sFontFamily} not found on system. Defaulting to font size 12 as a base size.')
    if(USE_TKINTER):
        font = tk.font.Font(family=sFontFamily, size=FontSize)
    else:
        try:
            font = ImageFont.truetype(fontpath, FontSize)
        except:
            logger.warning(f'TTF font for FontFamily {sFontFamily} not found on system. Defaulting to font size 12 as a base size.')
            return(False, 9, 10, 12, 15, 19, 24, False)
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
    if(h>colH):
        logger.debug("We may need a vertical scrollbar...")
    fsNORMAL=FontSize # 12
    fsTINY=int((0.75*FontSize)+0.5)  # 9/12=0.75
    fsSMALL=int((0.833333*FontSize)+0.5)  # 10/12=0.833333
    fsLARGE=int((1.25*FontSize)+0.5)  # 15
    fsHUGE=int((1.5833333*FontSize)+0.5)  # 19
    fsGIGANTIC=int(2*FontSize)  # 24
    logger.debug(f"fsSMALL={fsSMALL},fsNORMAL={fsNORMAL},fsLARGEL={fsLARGE},fsHUGE={fsHUGE}")
    return(bFontFound, fsTINY,  fsSMALL,  fsNORMAL,  fsLARGE,  fsHUGE,  fsGIGANTIC,  bWeMustStack)


def grabFirstEntryFromList(myList):
  try:
    return myList[0]  
  except:
    return myList


def swapListElements(list, pos1, pos2):
    # pos1 and pos2 should be counted from zero
    if(pos1 != pos2):
        list[pos1], list[pos2] = list[pos2], list[pos1]
    return list


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

