
import tkinter as tk    
import customtkinter as ctk

from loguru import logger

class GridCTkLabel(ctk.CTkLabel):
    def __init__(self, root, myGridID,  *args, **kwargs):
        ctk.CTkLabel.__init__(self, root, *args, **kwargs)
        self.widgetGridID= tk.IntVar(value=myGridID)

class GridCTkCheckBox(ctk.CTkCheckBox):
    def __init__(self, root, myGridID,  *args, **kwargs):
        ctk.CTkCheckBox.__init__(self, root, *args, **kwargs) 
        self.widgetGridID= myGridID


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


def guiDoubleVar(value):
    return(tk.DoubleVar(value=value))


def guiRadioButton(rootFrame, text, fontName="Georgia",  fontSize=12, 
                                   variable=None,  value=None,  command=None):
    if((variable is None) or (value is None) or (command is None)):
        raise RuntimeError('Attempt to create radiobutton failed. At least one of the required parameters variable, value or command was not valid.')
    return(ctk.CTkRadioButton(rootFrame, text=text,  font=(fontName, fontSize),  
                                   variable=variable,  value=value,  command=command))


def guiTxtLabel(self, text,  anchor=None, fontName="Georgia",  fontSize=12,  width=None):
    if((anchor is None) and (width is None)):
        return(ctk.CTkLabel(self, text=text,  font=(fontName,  fontSize)))                                
    elif(anchor is None):
        return(ctk.CTkLabel(self, text=text,  width=width, font=(fontName,  fontSize)))                                
    elif(width is None):
        return(ctk.CTkLabel(self, text=text,  anchor=anchor, font=(fontName,  fontSize)))                                
    else:
        return(ctk.CTkLabel(self, text=text,  anchor=anchor, width=width, font=(fontName,  fontSize)))                                

