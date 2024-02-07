
import tkinter as tk    
import customtkinter as ctk
#from loguru import logger

'''
 This file guiElementsTk.py provides generig interfaces to tkinter and customtkinter GUI elements
'''

class GridCTkCheckBox(ctk.CTkCheckBox):
    def __init__(self, root, myGridID,  *args, **kwargs):
        ctk.CTkCheckBox.__init__(self, root, *args, **kwargs) 
        self.widgetGridID= myGridID

class GridCTkLabel(ctk.CTkLabel):
    def __init__(self, root, myGridID,  *args, **kwargs):
        ctk.CTkLabel.__init__(self, root, *args, **kwargs)
        self.widgetGridID= tk.IntVar(value=myGridID)

class GridCTkOptionMenu(ctk.CTkOptionMenu):
    def __init__(self, root, myGridID, *args, **kwargs):
        ctk.CTkOptionMenu.__init__(self, root, *args, **kwargs)
        self.widgetGridID= myGridID

def guiBooleanVar(value):
    return(tk.BooleanVar(value=value))

def guiDoubleVar(value):
    return(tk.DoubleVar(value=value))

def guiIntVar(value):
    return(tk.IntVar(value=value))
    
def guiStringVar(value):
   return(tk.StringVar(value=value))


def guiAskOkCancel(title="Untitled",  message="Is it ok?"):
    # If you click the OK button, the function returns True. Else, if you click the Cancel button, the function returns False
    return(tk.messagebox.askokcancel(title="Quit", message="Is it OK to abort your Lumia run?",  icon=tk.messagebox.QUESTION))
    

def guiButton(master, text='Ok',  command=None,  fontName="Georgia",  fontSize=12, ):
    return(ctk.CTkButton(master=master, command=command, font=(fontName, fontSize), text=text)
)

def   guiCheckBox(self,state=tk.NORMAL, text='', fontName="Georgia",  fontSize=12, variable=None, 
                            text_color='gray5',  text_color_disabled='gray70', onvalue=True, offvalue=False):
    if((variable is None)):
        raise RuntimeError('Attempt to create a CheckBox failed. Required parameter >>variable<< is not valid.')
    if((text_color is None) and (text_color_disabled is None)):
        return(ctk.CTkCheckBox(self,state=state, text=text, font=(fontName, fontSize), variable=variable,   
                                        onvalue=onvalue, offvalue=offvalue) )
    else:
        return(ctk.CTkCheckBox(self,state=state, text=text, font=(fontName, fontSize), variable=variable, text_color='gray5',  
                                        text_color_disabled='gray70', onvalue=onvalue, offvalue=offvalue) )


def guiDataEntry(self, textvariable='',  placeholder_text='', width:int=40):
    return(ctk.CTkEntry(self,textvariable=textvariable, placeholder_text=placeholder_text, width=width))


def guiOptionMenu(self, values:[], variable=None,  dropdown_fontName="Georgia",  dropdown_fontSize=12):
    if((variable is None) or (values is None)):
        raise RuntimeError('Attempt to create an OptionsMenu failed. At least one of the required parameters values or variable was not valid.')
    return(ctk.CTkOptionMenu(self, values=values, variable=variable, dropdown_font=(dropdown_fontName, dropdown_fontSize)))

def guiRadioButton(rootFrame, text, fontName="Georgia",  fontSize=12, 
                                   variable=None,  value=None,  command=None):
    if((variable is None) or (value is None) or (command is None)):
        raise RuntimeError('Attempt to create radiobutton failed. At least one of the required parameters variable, value or command was not valid.')
    return(ctk.CTkRadioButton(rootFrame, text=text,  font=(fontName, fontSize),  
                                   variable=variable,  value=value,  command=command))

def guiTextBox(self, width=200,  height=100,  fontName="Georgia",  fontSize=12, text_color="black"):
    return(ctk.CTkTextbox(self, width=width, text_color=text_color, font=(fontName, fontSize),  height=height))

def guiTxtLabel(self, text,  anchor=None, fontName="Georgia",  fontSize=12,  width=None):
    if((anchor is None) and (width is None)):
        return(ctk.CTkLabel(self, text=text,  font=(fontName,  fontSize)))                                
    elif(anchor is None):
        return(ctk.CTkLabel(self, text=text,  width=width, font=(fontName,  fontSize)))                                
    elif(width is None):
        return(ctk.CTkLabel(self, text=text,  anchor=anchor, font=(fontName,  fontSize)))                                
    else:
        return(ctk.CTkLabel(self, text=text,  anchor=anchor, width=width, font=(fontName,  fontSize)))                                

