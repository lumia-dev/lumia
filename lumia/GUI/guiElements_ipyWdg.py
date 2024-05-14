#!/usr/bin/env python3
import ipywidgets  as wdg
from loguru import logger
import time
from functools import partial
from jupyter_ui_poll import ui_events
#from ipywidgets import  Dropdown, Output, Button, FileUpload, SelectMultiple, Text, HBox, IntProgress
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
    #bObserve=False

    def __init__(self, parent,  root, myGridID, command, variable, text='',  *args, **kwargs):
        #ctk.CTkCheckBox.__init__(self, root, *args, **kwargs) 
        self.widgetGridID= myGridID
        self.command=parent.EvHdPg2myCheckboxEvent  
        self.parent=parent
        #print(f'parent={parent}, self.command={self.command},  self.parent.EvHdPg2myCheckboxEvent={self.parent.EvHdPg2myCheckboxEvent} ')
        #self.ptrToEvHdPg2myCheckboxEvent=lambda: command(self.widgetGridID)
        #self.eventHandlerFunction=lambda: command(self.widgetGridID)
        wdg.Checkbox.__init__(self, 
            value=variable,
            description=text,
            disabled=False,
            indent=False
        )
        
        def actOnCheckBoxChanges(change):
            try:
                chName=change['name']
                # we are only interested in events where there is a change in value selected/deselected True/False etc
                # changeEvent={'name': '_property_lock', 'old': {}, 'new': {'value': False}, 'owner': GridCTkCheckBox(value=True, description='JFJ', indent=False, layout=Layout(grid_area='widget011', height='30px', margin='2px', padding='2px', width='auto')), 'type': 'change'}                
                # changeEvent={'name': 'value', 'old': True, 'new': False, 'owner': GridCTkCheckBox(value=False, description='JFJ', indent=False, layout=Layout(grid_area='widget011', height='30px', margin='2px', padding='2px', width='auto')), 'type': 'change'}
                if('value' in chName):
                    value=True
                    description=change['owner'].description  # 'CH' 'JFJ' a country or station name code or empty if a Select button
                    try:
                        value=change['owner'].value  # True/False for check box now being selected or deselected
                    except:
                        logger.warning('GridCTkCheckBox.actOnCheckBoxChanges: Failed to extract the value from change[owner].value')
                    try:
                        wdgGridTxt=change['owner'].layout.grid_area
                        if not (wdgGridTxt is None):
                            #try:
                            #   print(f'CheckBox Change event with: self.widgetGridID={self.widgetGridID} wdgGridTxtID={wdgGridTxt},  value={value},  description={description}')
                            #   print('Calling self.parent.EvHdPg2myCheckboxEvent() with those parameters')
                            #except:
                            #    print('Error: GridCTkCheckBox.actOnCheckBoxChanges(): p112 failed')
                            self.parent.EvHdPg2myCheckboxEvent(gridID=self.widgetGridID, wdgGridTxt=wdgGridTxt,value=value,description=description )
                    except:
                        pass
            except:
                pass
            return True

        self.observe(actOnCheckBoxChanges)
        return


class GridCTkLabel(wdg.Text):
    def __init__(self, root, myGridID, text='',  description='', *args, **kwargs):
        self.widgetGridID= myGridID
        sWdth="auto" # sWdth='55%%'            
        layout = wdg.Layout(width=sWdth)
        
        wdg.Text.__init__(self, 
        value=text, 
        layout = layout, 
        description=description,
        disabled=False   
        )


class GridCTkOptionMenu(wdg.Dropdown):
    def __init__(self, parent, root, myGridID, command, values, *args, **kwargs):
        self.widgetGridID= myGridID
        self.command=command
        self.lumiaGuiApp=parent
        # command=self.EvHdPg2myOptionMenuEvent
        #ctk.CTkOptionMenu.__init__(self, root, *args, **kwargs)
        wdg.Dropdown.__init__(self, options=values, description='', disabled=False) # value=0, 

        def actOnOptionMenuChanges(change):
            try:
                chName=change['name']
                # we are only interested in events where there is a change in value selected/deselected True/False etc
                # changeEvent={'name': '_property_lock', 'old': {}, 'new': {'value': False}, 'owner': GridCTkCheckBox(value=True, description='JFJ', indent=False, layout=Layout(grid_area='widget011', height='30px', margin='2px', padding='2px', width='auto')), 'type': 'change'}                
                # changeEvent={'name': 'value', 'old': True, 'new': False, 'owner': GridCTkCheckBox(value=False, description='JFJ', indent=False, layout=Layout(grid_area='widget011', height='30px', margin='2px', padding='2px', width='auto')), 'type': 'change'}
                if('value' in chName):
                    value=True
                    #description=change['owner'].description  # 'CH' 'JFJ' a country or station name code or empty if a Select button
                    try:
                        value=change['owner'].value  # True/False for check box now being selected or deselected
                    except:
                        logger.warning('GridCTkOptionMenu.actOnOptionMenuChanges: Failed to extract the value from change[owner].value')
                    try:
                        wdgGridTxt=change['owner'].layout.grid_area
                        if not (wdgGridTxt is None):
                            #print(f'changeEvent={change}')

                            # print(f'Calling self.parent.EvHdPg2myCheckboxEvent(gridID=99999) with self.lumiaGuiApp={self.lumiaGuiApp.EvHdPg2myCheckboxEvent}')
                            # def EvHdPg2myCheckboxEvent(self, gridID=None,  wdgGridTxt='',  value=None,  description=''):

                            #if((self.widgetGridID==3) or (self.widgetGridID==203) or (self.widgetGridID==11803)):
                            #   print(f'OptionMenu Change event with: self.widgetGridID={self.widgetGridID},  value={value},  wdgGridTxt={wdgGridTxt},  sSamplingHeights={self.options}')
                            self.lumiaGuiApp.EvHdPg2myOptionMenuEvent(gridID=self.widgetGridID, sSamplingHeights=self.options, selectedValue=value)
                    except:
                        pass
            except:
                pass
            return True

        self.observe(actOnOptionMenuChanges)
        return


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
    layout = wdg.Layout(width=sWdth)
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
    return True



class guiCheckBox(wdg.Checkbox):
    def __init__(self, root=None, parent=None,  disabled=False, text='', fontName="Georgia", command=None, nameOfEvtHd=None, 
                             fontSize=12, variable=False, text_color='gray5',  text_color_disabled='gray70', onvalue=True, offvalue=False):
        self.command=command
        self.parent=parent
       #print(f'parent={parent}, self.command={self.command}')
        #@def   guiCheckBox:
        # variable holds the initial state whether the CheckBox is selected (True) or not (False)
        wdg.Checkbox.__init__(self, 
            value=variable,
            description=text,
            disabled=disabled,
            indent=False
        )

        def actOnCheckBoxChange(change):
            #print(f'Entering actOnCheckBoxChange.change:{change}')
            try:
                chName=change['name']
            except:
                chName=''
            #print(f'chName={chName}')
            #print(f'chName={chName}')
            # we are only interested in events where there is a change in value selected/deselected True/False etc
            # changeEvent={'name': 'value', 'old': True, 'new': False, 'owner': GridCTkCheckBox(value=False, description='JFJ', indent=False, layout=Layout(grid_area='widget011', height='30px', margin='2px', padding='2px', width='auto')), 'type': 'change'}

            if('value' in chName):
                value=True
                #print(f'actOnCheckBoxChange.change:{change}')
                #description=change['owner'].description  # 'CH' 'JFJ' a country or station name code or empty if a Select button
                try:
                    value=change['owner'].value  # True/False for check box now being selected or deselected
                    #print(f'change[owner].value={value}')
                except:
                    logger.warning('guiCheckBox.actOnCheckBoxChange: Failed to extract the value from change[owner].value')
                #print(f'actOnCheckBoxChange.command={self.command}')
                #print(f'command={self.command}')
                if(self.command is None):
                    pass
                else:
                    self.command()

        self.observe(actOnCheckBoxChange)
        return 

class guiRadioButton(wdg.RadioButtons):
    def __init__(self,  parent=None,  options=[] , description='',  text='', preselected=None,  command=None):
        self.command=command
        self.parent=parent
        if(preselected is None):
            preselected=0
        if((description is None) or (len(description)<1)):
            description=text
        myRb=wdg.RadioButtons.__init__(self, 
            options=options,
            value=options[preselected],
            description=description,
            disabled=False, 
            indent=False
        )

        #@def   guiRadioButton:
        # variable holds the initial state whether the CheckBox is selected (True) or not (False)
        def actOnRbChange(change):
            #print(f'changeEvent={change}')
            try:
                chName=change['name']
            except:
                chName=''
            #print(f'chName={chName}')
            # we are only interested in events where there is a change in the radiobutton index value
            # changeEvent={'name': 'index', 'old': 0, 'new': 1, 'owner': guiRadioButton(index=1, layout=Layout(grid_area='widget018', 
            # height='60px', margin='2px', padding='2px', width='auto'), options=('Any station', 'ICOS only'), value='ICOS only'), 'type': 'change'}
            if('index' in chName):
                #print(f'changeEvent={change}')
                value=999
                try:
                    value=change['owner'].index  # index of the presently selected radiobutton 0,1,2,...
                    # print(f'value=change[owner].index={value}')
                except:
                    logger.warning('guiRadioButton.actOnRbChange: Failed to extract the value from change[owner].value')
                self.command() #self.parent, value)
    
        self.observe(actOnRbChange)
        return(myRb)

        

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
    return True



# guiDataEntry(self.guiPg1TpLv,textvariable=self.sStartDate, placeholder_text=txt, width=self.root.colWidth)
def guiDataEntry(canvas,textvariable='', placeholder_text='', width:int=40):
    return(wdg.Text(
        value=textvariable,
        placeholder=placeholder_text,
        description='',
        disabled=False   
    ))
    return True


                          
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
    return True

def guiPlaceWidget(wdgGrid,  widget,  row=0, column=0, columnspan=1, rowspan=1, widgetID_LUT={},  width=240,  padx=10,  pady=10,  sticky="ew"):
    # widgets that do not support a width layout must set nCols=0
    iHght=30*rowspan
    sHght=f"{iHght}px"
    # It proved impossible to me to use our widget.gridID instead of the auto-generated one by the wdgGrid object
    # If I do change it, then wdgGrid cannot place the widget (or places is somwhere outside the visible screen)
    myLayout = {"width":"auto",  "height":sHght,  "margin":"2px",  "padding":"2px"}
    widget.layout = myLayout
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
    try:  
        # Let's create a lookup table to convert wdgGrid3.grid_area gridIdx (a string like 'widget001', 'widget488',..) info into our self.widgetGridID
        # Only the dynamically created widgets do have a gridID assigned - if it exists, then we use our ID
        # else it gets too complicated because the number of widgets per row varies (country checkbox present or not)
        # hence we don't want to use the autogenerated grid_area value. And we do need the gridID for the
        # checkbox event handler later on. Hence we create a LUT for later use.
            # wdgGrid[row, column].widgetGridID=widget.widgetGridID # we can do this, but it is to no use. A checkbox change event does not forward this info
        myGridID=widget.widgetGridID
        wdgGridTxtID=wdgGrid[row, column].layout.grid_area
        # print(f'PlaceWdg Adding wdgGrid[row={row}, column={column}].txtID={wdgGridTxtID} ; widgetGridID={myGridID} to self.widgetID_LUT')
        if (wdgGridTxtID not in widgetID_LUT):
            widgetID_LUT[wdgGridTxtID]=myGridID
    except:
        pass
    return True



def guiSetCheckBox(myWidget, bSelected=False):
    myWidget.value=bSelected
    myWidget
    return True


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
    layout = wdg.Layout(width=sWdth)
    
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
    return True
    
    
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
    whichButton=1 # which one was clicked?
    
    # Create a function to continue the execution
    def on_click(b):
        global button_clicked
        button_clicked = True
        on_click

    def on_cancel(b):
        #print('Cancel pressed')
        global whichButton
        whichButton=2
        global button_clicked
        button_clicked = True
        on_cancel
        #return True
        
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

    return (whichButton) 


def guiWipeTextBox(txtBoxWidget, protect=True):
    txtBoxWidget.value = ""
    txtBoxWidget
    return True
    
def guiWriteIntoTextBox(txtBoxWidget, txt='', protect=True):    
    txtBoxWidget.value = txt
    txtBoxWidget
    return True
    
