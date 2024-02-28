#!/usr/bin/env python3
import ipywidgets  as widgets
from loguru import logger
import time
from jupyter_ui_poll import ui_events
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
    def __init__(self, nCols=5,  nRows=13,  bg="cadet blue"):
        self.bg=bg
        self.title='LUMIA - the Lund University Modular Inversion Algorithm'
        self.activeTextColor='gray10'
        self.inactiveTextColor='gray50'


def guiBooleanVar(value):
    if(value):
        return(True)
    else:
        return(False)

def guiDoubleVar(value):
    return(float(value))

def guiIntVar(value):
    return(int(value))
    
def guiStringVar(value):
    return(str(value))



def guiAskOkCancel(title="Untitled",  message="Is it ok?"):
    # popup boxes like the one needed here was removed from ipywidgets:
    # https://stackoverflow.com/questions/39676933/create-a-popup-widget-in-ipython
    # The suggested workaround is a Box or VBox with a yes/no button pair
    # See also: https://stackoverflow.com/questions/60166968/how-to-create-a-popup-in-a-widget-call-back-function-in-ipywidgets
    # If you click the OK button, the function returns True. Else, if you click the Cancel button, the function returns False
    return(True)


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


def   guiCheckBox(self,disabled=False, text='', fontName="Georgia", command=None, fontSize=12, variable=None, 
                            text_color='gray5',  text_color_disabled='gray70', onvalue=True, offvalue=False):
    return(widgets.Checkbox(
        value=offvalue,
        description=text,
        disabled=disabled,
        indent=False
    ))

# guiDataEntry(self.guiPg1TpLv,textvariable=self.sStartDate, placeholder_text=txt, width=self.root.colWidth)
def guiDataEntry(canvas,textvariable='', placeholder_text='', width:int=40):
    return(widgets.Text(
        value=textvariable,
        placeholder=placeholder_text,
        description='',
        disabled=False   
    ))


                          
def oldGuiFileDialog(filetypes='*', title='Open', multiple=False): 
    fileNameDlg= FileUpload(
        accept=filetypes,  # Accepted file extension e.g. '.txt', '.pdf', 'image/*', 'image/*,.pdf'
        multiple=multiple  # True to accept multiple files upload else False
    )
    return(fileNameDlg)

def guiFileDialog(filetypes='', title='Open', multiple=False):
    ui_done = False

    def on_upload_btn_click(change):
        uploaded_file = next(iter(upload_btn.value.values()))
        #content = uploaded_file['content']
        print(f"File {uploaded_file} uploaded successfully!")
        global ui_done
        ui_done = True
        #btn.description = 'ok'       
        return uploaded_file #content

    upload_btn = widgets.FileUpload(accept=filetypes, multiple=multiple)
    #upload_btn.on_click(on_upload_btn_click)
    upload_btn.observe(on_upload_btn_click, names='value')
    display(upload_btn)

    # Wait for the user to click the file selection dialog
    with ui_events() as poll:
        while ui_done is False:
            poll(10)          # React to UI events (upto 10 at a time)
            print('.', end='')
            time.sleep(0.5)

    return on_upload_btn_click

def guiOptionMenu(self, values:[], variable=int(0),  dropdown_fontName="Georgia",  dropdown_fontSize=12):
    return(widgets.Dropdown(
        options=values,
        value=variable,
        description='',
        disabled=False,
    ))

def guiPlaceWidget(wdgGrid,  widget, row, column, columnspan, rowspan=1,  padx=10,  pady=10,  sticky="ew"):
    #widget.wdgGrid(row=row, column=column, columnspan=columnspan, rowspan=rowspan, padx=padx, pady=pady, sticky=sticky)
    rghtCol=column + columnspan
    btmRow=row+rowspan
   
    if((columnspan==1) and (rowspan==1)):
        wdgGrid[row, column] = widget
    elif((columnspan > 1) and (rowspan==1)):
        wdgGrid[row, column:rghtCol] = widget
    elif((columnspan==1) and (rowspan > 1)):
        wdgGrid[row:btmRow, column] = widget
    else:
        wdgGrid[row:btmRow, column:rghtCol] = widget
    #widget
    #display(widget)


def guiRadioButton(options=[] , description='',  text='', preselected=None):
    if(preselected is None):
        preselected=options[0]
    if(len(description)==0):
        description=text
    return(widgets.RadioButtons(
        options=options,
        value=preselected, # Defaults to 'pineapple'
    #    layout={'width': 'max-content'}, # If the items' names are long
        description=description,
        disabled=False
    ))



def guiTextBox(frame, text='',  description='', width='18%',  height='20%',  fontName="Georgia",  fontSize=12, text_color="black"):
    box=widgets.Textarea(
        value=text,
        placeholder='___________________________________________________________________________________________________________________________________________',
        description=description,
        disabled=False
    )
    #box(layout=Layout(width=width, height=height))
    return(box)

#ge.guiTxtLabel(self.guiPg1TpLv, title, fontName=self.root.myFontFamily, fontSize=self.root.fsGIGANTIC, style="bold")
def guiTxtLabel(self, text,  anchor=None, fontName="Georgia",  fontSize=12,  width=None, description='' , style="normal"):
    placeholder='x'
    if(width is None):
        placeholder="123456789ABCDEF67890"
    else:
        for i in range(width):
            placeholder=placeholder+'x'
    return(widgets.Text(
    value=text,
    placeholder=placeholder,
    description=description,
    disabled=False   
    ))
