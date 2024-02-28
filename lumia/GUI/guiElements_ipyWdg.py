#!/usr/bin/env python3
import ipywidgets  as wdg
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
    def __init__(self):
        self.bg='cadet blue'
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
    return(wdg.Button(
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
    return(wdg.Checkbox(
        value=offvalue,
        description=text,
        disabled=disabled,
        indent=False
    ))

# guiDataEntry(self.guiPg1TpLv,textvariable=self.sStartDate, placeholder_text=txt, width=self.root.colWidth)
def guiDataEntry(canvas,textvariable='', placeholder_text='', width:int=40):
    return(wdg.Text(
        value=textvariable,
        placeholder=placeholder_text,
        description='',
        disabled=False   
    ))


                          
def guiFileDialog(filetypes='', title='Open', multiple=False):
    '''
    new lumiaGUI: ipywidgets can be a real pain in the butt. It seemed impossible to make execution wait until the user 
    has selected the input file using the fileUploader widget. And achieving this is indeed tricky and caused me lots of frustration 
    questioning my sanity. After lots of googling I found both an explanation why this is so hard to do and a solution to get 
    around this. Look at these resources to know more about it: 
    https://pypi.org/project/jupyter-ui-poll/  and  
    https://stackoverflow.com/questions/76564282/how-to-get-an-ipywidget-to-wait-for-user-input-then-continue-running-your-scrip 
    The current commit is the first test of getting this to work at all and is placed at the beginning of everything,
    just because it is WORKING :)
    Note that the first block is the original example and the second uses the fileUploader widget instead of a dropdown box.
    '''
    global button_clicked
    filename=None

    # Create the file selector widget
    upload_btn = wdg.FileUpload(accept=filetypes, multiple=False)

    # Create a function to continue the execution
    button_clicked = False
    def on_click(b):
        global button_clicked
        button_clicked = True
        #print('button clicked')
    
    # Create a button widget
    button = wdg.Button(description="Continue after file selection")
    button.layout = {"width":"150px"}
    button.on_click(on_click)
    
    # Display the widget and button
    display(upload_btn, button)
    
    # Wait for user to press the button
    with ui_events() as poll:
        while button_clicked is False:
            poll(10)          # React to UI events (upto 10 at a time)
            time.sleep(0.1)

    #filename =on_upload_btn_click
    fileInfo = upload_btn.value[0]
    filename=fileInfo['name']
    # filename returns a dictionary of the form (example):
    # {'name': 'lumia-config-v6-tr-co2.yml', 'type': 'application/x-yaml', 'size': 5808, 'content': <memory at 0x7f5ccc65f1c0>, 
    #    'last_modified': datetime.datetime(2024, 2, 27, 0, 12, 32, 459000, tzinfo=datetime.timezone.utc)}
    # print(f"Have uploaded {filename}. Continuing execution...")
    return filename


def guiOptionMenu(self, values:[], variable=int(0),  dropdown_fontName="Georgia",  dropdown_fontSize=12):
    return(wdg.Dropdown(
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
    return(wdg.RadioButtons(
        options=options,
        value=preselected, # Defaults to 'pineapple'
    #    layout={'width': 'max-content'}, # If the items' names are long
        description=description,
        disabled=False
    ))



def guiTextBox(frame, text='',  description='', width='18%',  height='20%',  fontName="Georgia",  fontSize=12, text_color="black"):
    box=wdg.Textarea(
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
    return(wdg.Text(
    value=text,
    placeholder=placeholder,
    description=description,
    disabled=False   
    ))
