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

class pseudoRootFrame:
    def __init__(self):
        self.bg='cadet blue'
        self.title='rootFrame'
        self.activeTextColor='gray10'
        self.inactiveTextColor='gray50'

class GridCTkCheckBox(wdg.Checkbox):
    def __init__(self, root, myGridID,  variable, text='',  *args, **kwargs):
        #ctk.CTkCheckBox.__init__(self, root, *args, **kwargs) 
        self.widgetGridID= myGridID
        wdg.Checkbox.__init__(self, 
            value=variable,
            description=text,
            disabled=False,
            indent=False
        )


class GridCTkLabel(wdg.Text):
    def __init__(self, root, myGridID, text='',  description='', *args, **kwargs):
        self.widgetGridID= myGridID
        sWdth="auto" # sWdth='55%%'            
        layout = widgets.Layout(width=sWdth)
        
        wdg.Text.__init__(self, 
        value=text, 
        layout = layout, 
        description=description,
        disabled=False   
        )



class GridCTkOptionMenu(wdg.Dropdown):
    def __init__(self, root, myGridID, values, *args, **kwargs):
        self.widgetGridID= myGridID
        #ctk.CTkOptionMenu.__init__(self, root, *args, **kwargs)
        wdg.Dropdown.__init__(self, options=values, description='', disabled=False) # value=0, 



def getVarValue(tkinterVar):
    return tkinterVar
    
    
def getWidgetValue(widget):
    return(widget.value)


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


def guiButton(master, text='Ok',  command=None,  fontName="Georgia",  fontSize=12, width=200):
    sWdth=f'{width}px'            
    layout = widgets.Layout(width=sWdth)
    return(wdg.Button(
    description=text,
    disabled=False,
    button_style='', # 'success', 'info', 'warning', 'danger' or ''
    tooltip='',
    layout = layout, 
    )
    )
    #icon='check' # (FontAwesome names without the `fa-` prefix)
    #return(ctk.CTkButton(master=master, command=command, font=(fontName, fontSize), text=text)


def   guiCheckBox(self,disabled=False, text='', fontName="Georgia", command=None, fontSize=12, variable=None, 
                            text_color='gray5',  text_color_disabled='gray70', onvalue=True, offvalue=False):
    # variable holds the initial state whether the CheckBox is selected (True) or not (False)
    return(wdg.Checkbox(
        value=variable,
        description=text,
        disabled=disabled,
        indent=False
    ))


def guiConfigureWdg(self, widget=None,  state=None,  disabled=None,  command=None,  text=None,  text_color=None, fg_color=None,  bg_color=None):
#                    ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID],  state=tk.DISABLED,  command=None,  text='On',  text_color='blue',  fg_color=None,  bg_color=None)
    if(widget is None):
        return
    if not (state is None):
        if (state):
            widget.disabled
    if not (disabled is None):
        if (disabled):
            widget.disabled=True
        else:
            widget.disabled=False
    if not (command is None):
        #widget.observe(value_changed, "value")
        # myWidgetSelect.configure(command=lambda widgetID=myWidgetSelect.widgetGridID : self.EvHdPg2myCheckboxEvent(myWidgetSelect.widgetGridID, obsDf)) 
        widget.configure(command=command)
    if not (text is None):
        widget.text=text
    if not (fg_color is None):
        widget.style.button_color = fg_color
    '''
    if not (text_color is None):
        widget.configure(text_color=text_color)
    if not (bg_color is None):
        widget.configure(bg_color=bg_color)
    '''
    widget



# guiDataEntry(self.guiPg1TpLv,textvariable=self.sStartDate, placeholder_text=txt, width=self.root.colWidth)
def guiDataEntry(canvas,textvariable='', placeholder_text='', width:int=40):
    return(wdg.Text(
        value=textvariable,
        placeholder=placeholder_text,
        description='',
        disabled=False   
    ))


                          
def guiFileDialogDoAll(filetypes='', title='Open', multiple=False,  width=240):
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
    #button.layout = {"width":"150px"}
    if(width is None):
        width=int(150)
    else:
        sWdth=f'{width}px'            
    myLayout = {"width":sWdth}
    button.layout = myLayout
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



def guiFileDialog(filetypes='', title='Open',  description="Select file", multiple=False,  width=240):
    # Create the file selector widget
    upload_btn = wdg.FileUpload(accept=filetypes,  description=description, multiple=False)
    #upload_btn.on_click(on_click)
    
    # Display the widget and button
    # display(upload_btn) # done by calling entity
    return(upload_btn)


def guiOptionMenu(self, values:[], variable=int(0),  dropdown_fontName="Georgia",  dropdown_fontSize=12):
    return(wdg.Dropdown(
        options=values,
        value=variable,
        description='',
        disabled=False,
    ))

def guiPlaceWidget(wdgGrid,  widget,  row=0, column=0, columnspan=1, rowspan=1,  width=240,  padx=10,  pady=10,  sticky="ew"):
    # widgets that do not support a width layout must set nCols=0
    '''
    nCols=0, 
    width=rowspan*width
    if(width is None):
        if(nCols>0):
            iRelWdth=int(100*(columnspan*1.0/nCols))  # width in percent
            sWdth=f'{iRelWdth}%%'
            myLayout = {"width":sWdth}
            widget.layout = myLayout
    else:
        sWdth=f'{width}px'            
        myLayout = {"width":sWdth}
        widget.layout = myLayout
    '''
    iHght=30*rowspan
    sHght=f"{iHght}px"
    myLayout = {"width":"auto",  "height":sHght,  "margin":"2px",  "padding":"2px"}
    widget.layout = myLayout
    # wdgGrid[1stRow:LstRow , 1stCol:LstCol] = widget  # with its layout setup beforehand
    rghtCol=column + columnspan
    btmRow=row+rowspan
    if(rowspan==6):
        wdgGrid[3:7, 4] = widget
    elif((columnspan==1) and (rowspan==1)):
        wdgGrid[row, column] = widget
    elif((columnspan > 1) and (rowspan==1)):
        wdgGrid[row, column:rghtCol] = widget
    elif((columnspan==1) and (rowspan > 1)):
        wdgGrid[row:btmRow, column] = widget
    else:
        wdgGrid[row:btmRow, column:rghtCol] = widget


def guiRadioButton(options=[] , description='',  text='', preselected=None):
    if(preselected is None):
        preselected=0
    if(len(description)==0):
        description=text
    return(wdg.RadioButtons(
        options=options,
        value=options[preselected],
    #    layout={'width': 'max-content'}, # If the items' names are long
        description=description,
        disabled=False
    ))


def guiSetCheckBox(myWidget, bSelected=False):
        myWidget.value=bSelected
        myWidget


def guiTextBox(frame, text='',  description='', width='18%',  height='20%',  fontName="Georgia",  fontSize=12, text_color="black"):
    box=wdg.Textarea(
        value=text,
        placeholder='___________________________________________________________________________________________________________________________________________',
        description=description,
        disabled=False
    )
    #box(layout=Layout(width=width, height=height))
    return(box)

def guiTxtLabel(self, text,  anchor=None, fontName="Georgia",  fontSize=12,  description='', placeholder='',  width=None, nCols=0,  
                            colwidth=1, style="normal"):
    if(nCols==0):
        return(wdg.Text(
        value=text,
        placeholder=placeholder,
        description=description,
        disabled=False   
        ))
    if(width is None):
        iRelWdth=int(100*(colwidth*1.0/nCols))  # width in percent
        sWdth=f'{iRelWdth}%%'
    else:
        sWdth=f'{width}px'            
    layout = widgets.Layout(width=sWdth)
    
    return(wdg.Text(
    value=text,
    placeholder=placeholder,
    description=description,
    layout = layout, 
    disabled=False   
    )) 
    #     label_layout =wdg.Layout(layout), 
    # width='100px',height='30px'

def guiSetWidgetWidth(widget,  width=200):
    if(width is None):
        width=int(240)
    else:
        sWdth=f'{width}px'            
    myLayout = {"width":sWdth}
    widget.layout = myLayout
    
    
def guiWidgetsThatWait4UserInput(watchedWidget=None,watchedWidget2=None, title='',  
            myDescription="Continue after user selection",  myDescription2="Cancel", width=240):
    '''
    new lumiaGUI: ipywidgets can be a real pain in the butt. It seemed impossible to make execution wait until the user 
    has selected the input file using the fileUploader widget. And achieving this is indeed tricky and caused me lots of frustration 
    questioning my sanity. After lots of googling I found both an explanation why this is so hard to do and a solution to get 
    around this. Look at these resources to know more about it: 
    https://pypi.org/project/jupyter-ui-poll/  and  
    https://stackoverflow.com/questions/76564282/how-to-get-an-ipywidget-to-wait-for-user-input-then-continue-running-your-scrip 
    '''
    global button_clicked
    button_clicked = False
    global whichButton
    whichButton=1 # which on was clicked?
    
    # Create a function to continue the execution
    def on_click(b):
        global button_clicked
        button_clicked = True
        on_click

    def on_cancel(b):
        print('Cancel pressed')
        global whichButton
        whichButton=2
        global button_clicked
        button_clicked = True
        on_cancel
    
    watchedWidget.on_click(on_click)
    if(watchedWidget2 is None):
        pass
    else:
        watchedWidget2.on_click(on_cancel)
    
    # Display the widget and button
    # display(watchedWidget)
    
    # Wait for user to press the button
    with ui_events() as poll:
        while button_clicked is False:
            poll(10)          # React to UI events (upto 10 at a time)
            time.sleep(0.1)

    #print(f'returning whichButton={whichButton}')
    return (whichButton) 


def guiWipeTextBox(txtBoxWidget, protect=True):
    txtBoxWidget.value = ""
    txtBoxWidget
    
def guiWriteIntoTextBox(txtBoxWidget, txt='', protect=True):    
    txtBoxWidget.value = txt
    txtBoxWidget
    
