#!/usr/bin/env python3
import ipywidgets  as widgets
import ipywidgets as widgets
from ipywidgets import  Dropdown, Output, Button, FileUpload, SelectMultiple, Text, HBox, IntProgress

from IPython.display import display


# LumiaGui Class =============================================================
class LumiaGui:
    def __init__(self): 
        self.title='LUMIA - the Lund University Modular Inversion Algorithm'
        self.activeTextColor='gray10'
        self.inactiveTextColor='gray50'

class guiToplevel:
    def __init__(self, bg="cadet blue"):
        self.bg=bg
        self.title='LUMIA - the Lund University Modular Inversion Algorithm'
        self.activeTextColor='gray10'
        self.inactiveTextColor='gray50'


from loguru import logger

def guiAskOkCancel(title="Untitled",  message="Is it ok?"):
    # If you click the OK button, the function returns True. Else, if you click the Cancel button, the function returns False
    return()

def guiButton(master, text='Ok',  command=None,  fontName="Georgia",  fontSize=12, ):
    return(widgets.Button(
    description=text,
    disabled=False,
    button_style='', # 'success', 'info', 'warning', 'danger' or ''
    tooltip='',
    icon='check' # (FontAwesome names without the `fa-` prefix)
    )
    )
    #return(ctk.CTkButton(master=master, command=command, font=(fontName, fontSize), text=text)


def guiPlaceWidget(widget, row, column, columnspan, rowspan=1, padx=10,  pady=10,  sticky="ew"):
   #widget.grid(row=row, column=column, columnspan=columnspan, rowspan=rowspan, padx=padx, pady=pady, sticky=sticky)
    display(widget)
    #TODO: add layout with grid
    
def guiTxtLabel(self, text,  anchor=None, fontName="Georgia",  fontSize=12,  width=None, description='' , style="normal"):
    placeholder='x'
    if(width is None):
        placeholder="LUMIA  --  Configure your next LUMIA run"
    else:
        for i in range(width):
            placeholder=placeholder+'x'
    return(widgets.Text(
    value=text,
    placeholder=placeholder,
    description=description,
    disabled=False   
    ))
