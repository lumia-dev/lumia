#!/usr/bin/env python3
import tkinter as tk    
import customtkinter as ctk
from tkinter import messagebox as mb
#from loguru import logger

'''
 This file guiElementsTk.py provides generig interfaces to tkinter and customtkinter GUI elements
'''

# LumiaGui Class =============================================================
class LumiaGui(ctk.CTk):
    def __init__(self): 
        ctk.set_appearance_mode("System")
        ctk.set_default_color_theme("/home/arndt/dev/lumia/lumiaDA/lumia/lumia/GUI/doc/lumia-dark-theme.json")
        ctk.CTk.__init__(self)
        # self.geometry(f"{maxW+1}x{maxH+1}")   is set later when we know about the screen dimensions
        self.title('LUMIA - the Lund University Modular Inversion Algorithm')
        self.grid_rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.activeTextColor='gray10'
        self.inactiveTextColor='gray50'

class LumiaTkFrame(tk.Frame):
    def __init__(self, parent, *args, **kwargs):
        tk.Frame.__init__(self, parent, *args, **kwargs)
        self.parent = parent


class GridOldCTkCheckBox(ctk.CTkCheckBox):
    def __init__(self, root, myGridID,  *args, **kwargs):
        ctk.CTkCheckBox.__init__(self, root, *args, **kwargs) 
        self.widgetGridID= myGridID

class GridCTkCheckBox(ctk.CTkCheckBox):
    def __init__(self, parent, root, myGridID, command=None,  *args, **kwargs):
        self.widgetGridID= myGridID
        #print(f'command={command},  self.widgetGridID={self.widgetGridID}')
        self.ptrToEvHdPg2myCheckboxEvent=lambda: command(self.widgetGridID)
        #print(f'self.ptrToEvHdPg2myCheckboxEvent={self.ptrToEvHdPg2myCheckboxEvent},  self.widgetGridID={self.widgetGridID}')
        ctk.CTkCheckBox.__init__(self, root, command=self.ptrToEvHdPg2myCheckboxEvent, *args, **kwargs) 


class GridCTkLabel(ctk.CTkLabel):
    def __init__(self, root, myGridID,  *args, **kwargs):
        ctk.CTkLabel.__init__(self, root, *args, **kwargs)
        self.widgetGridID= tk.IntVar(value=myGridID)

class GridCTkOptionMenu(ctk.CTkOptionMenu):
    def __init__(self, parent,  root, myGridID, command=None, values=None,  *args, **kwargs):
        self.widgetGridID= myGridID
        ptrToEvHdPg2myOptionMenuEvent=lambda: command(self.widgetGridID,  values)
        ctk.CTkOptionMenu.__init__(self, root, command=ptrToEvHdPg2myOptionMenuEvent, *args, **kwargs)

class oldGridCTkOptionMenu(ctk.CTkOptionMenu):
    def __init__(self, root, myGridID, *args, **kwargs):
            ctk.CTkOptionMenu.__init__(self, root, *args, **kwargs)
            self.widgetGridID= myGridID

def getVarValue(tkinterVar):
    return tkinterVar.get()
    
    
def getWidgetValue(widget):
    return(widget.get())
    

def guiBooleanVar(value):
    return(tk.BooleanVar(value=value))

def guiDoubleVar(value):
    return(tk.DoubleVar(value=value))

def guiIntVar(value):
    return(tk.IntVar(value=value))
    
def guiStringVar(value):
   return(tk.StringVar(value=value))

def guiAskyesno(title='', message='?'):
    return(mb.askyesno(title='Use previous list of obs data?',
                        message=message))

def guiAskOkCancel(title="Untitled",  message="Is it ok?"):
    # If you click the OK button, the function returns True. Else, if you click the Cancel button, the function returns False
    return(tk.messagebox.askokcancel(title="Quit", message="Is it OK to abort your Lumia run?",  icon=tk.messagebox.QUESTION))
    

def guiButton(master, text='Ok',  command=None,  fontName="Georgia",  fontSize=12, style="normal", width=200):
    return(ctk.CTkButton(master=master, command=command, font=(fontName, fontSize, style), text=text)
)


def   guiCheckBox(self, root=None, parent=None,disabled=False, text='', fontName="Georgia", command=None, nameOfEvtHd=None,
                                    fontSize=12, variable=None, text_color='gray5',  text_color_disabled='gray70', onvalue=True, offvalue=False):
    if(disabled):
        state=tk.DISABLED
    else:
        state=tk.NORMAL
    if((variable is None)):
        raise RuntimeError('Attempt to create a CheckBox failed. Required parameter >>variable<< is not valid.')
    if((text_color is None) and (text_color_disabled is None)):
        return(ctk.CTkCheckBox(self,state=state, text=text, font=(fontName, fontSize), variable=variable,   
                                        command=command,  onvalue=onvalue, offvalue=offvalue) )
    else:
        return(ctk.CTkCheckBox(self,state=state, text=text, font=(fontName, fontSize), variable=variable, text_color='gray5',  
                                        command=command, text_color_disabled='gray70', onvalue=onvalue, offvalue=offvalue) )


def guiConfigureWdg(self, widget=None,  state=None,  disabled=None,  command=None,  text=None,  text_color=None,  fg_color=None,  bg_color=None):
#                    ge.guiConfigureWdg(self, widget=self.widgetsLst[widgetID],  state=tk.DISABLED,  command=None,  text='On',  text_color='blue',  fg_color=None,  bg_color=None)
    if(widget is None):
        return
    if not (state is None):
        widget.configure(state=state)
    if not (disabled is None):
        if (disabled):
            widget.configure(state=tk.DISABLED)
        else:
            widget.configure(state=tk.NORMAL)
    if not (command is None):
        # myWidgetSelect.configure(command=lambda widgetID=myWidgetSelect.widgetGridID : self.EvHdPg2myCheckboxEvent(myWidgetSelect.widgetGridID, obsDf)) 
        widget.configure(command=command)
    if not (text is None):
        widget.configure(text=text)
    if not (text_color is None):
        widget.configure(text_color=text_color)
    if not (fg_color is None):
        widget.configure(fg_color=fg_color)
    if not (bg_color is None):
        widget.configure(bg_color=bg_color)

def guiDataEntry(self, textvariable='',  placeholder_text='', width:int=40):
    return(ctk.CTkEntry(self,textvariable=textvariable, placeholder_text=placeholder_text, width=width))

def guiFileDialog(filetypes=[("All", "*.yml")],  title='Open local file',  description="Select file", multiple=False,  width=240): 
    return(ctk.filedialog.askopenfilename(filetypes=filetypes,  title=title))
    
    

def guiOptionMenu(self, values:[], variable=None,  dropdown_fontName="Georgia",  dropdown_fontSize=12):
    if((variable is None) or (values is None)):
        raise RuntimeError('Attempt to create an OptionsMenu failed. At least one of the required parameters values or variable was not valid.')
    return(ctk.CTkOptionMenu(self, values=values, variable=variable, dropdown_font=(dropdown_fontName, dropdown_fontSize)))

def guiPlaceWidget(wdgGrid,  widget, row=0, column=0, columnspan=1, rowspan=1,  widgetID_LUT=None,  padx=10,  pady=10,  sticky="ew"):
    #try:
    #    print(f'widget={widget}')
    #    sGridID=str(widget.widgetGridID)
    #    print(f'sGridID={sGridID}')
    #except:
    #    pass

    widget.grid(row=row, column=column, columnspan=columnspan, rowspan=rowspan, padx=padx, pady=pady, sticky=sticky)

def guiRadioButton(rootFrame, text, fontName="Georgia",  fontSize=12, 
                                   variable=None,  value=None,  command=None,  nameOfEvtHd=None):
    if((variable is None) or (value is None) or (command is None)):
        raise RuntimeError('Attempt to create radiobutton failed. At least one of the required parameters variable, value or command was not valid.')
    return(ctk.CTkRadioButton(rootFrame, text=text,  font=(fontName, fontSize),  
                                   variable=variable,  value=value,  command=command))

def guiTextBox(self, text='', width=200,  height=100,  fontName="Georgia",  fontSize=12, text_color="black"):
    tbox=ctk.CTkTextbox(self, width=width, text_color=text_color, font=(fontName, fontSize),  height=height)
    if(len(text)>1):
        tbox.insert('0.0',text) # insert at line 0 character 0
    return(tbox)

def guiTxtLabel(self, text,  anchor=None, fontName="Georgia",  fontSize=12, placeholder='',  width=None, nCols=0,  
                            colwidth=1, style="normal"):
    if((anchor is None) and (width is None)):
        return(ctk.CTkLabel(self, text=text,  font=(fontName,  fontSize, style)))                                
    elif(anchor is None):
        return(ctk.CTkLabel(self, text=text,  width=width, font=(fontName,  fontSize, style)))                                
    elif(width is None):
        return(ctk.CTkLabel(self, text=text,  anchor=anchor, font=(fontName,  fontSize, style)))                                
    else:
        return(ctk.CTkLabel(self, text=text,  anchor=anchor, width=width, font=(fontName,  fontSize, style)))  
    

def guiToplevel(self,  bg="cadet blue"):
    return(tk.Toplevel(self,  bg=bg))


def guiSetCheckBox(myWidget, bSelected=False):
        if(bSelected):
            myWidget.select()
        else:
            myWidget.deselect()


def guiWipeTextBox(TxtBox,  protect=False):
    TxtBox.configure(state=tk.NORMAL)
    TxtBox.delete("0.0", "end")  # delete all text
    if(protect):
        TxtBox.configure(state=tk.DISABLED) 

def guiWriteIntoTextBox( TxtBox,  text,  protect=False):
    TxtBox.configure(state=tk.NORMAL)
    TxtBox.insert("0.0", text)
    if(protect):
        TxtBox.configure(state=tk.DISABLED) 

def  updateWidget(widget,  value, bTextvar=False,  bText_color=False,  bFg_color=False,  bBg_color=False):
    if(bTextvar):
        widget.configure(textvariable=value)
    elif(bText_color):  
        widget.configure(text_color=value)
    elif(bFg_color):   # in CTk this is the main button color (not the text color)
        widget.configure(fg_color=value)
    elif(bBg_color):   
        widget.configure(bg_color=value)


