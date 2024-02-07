

from ipywidgets import widgets 
from IPython.display import display


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

def guiTxtLabel(self, text,  anchor=None, fontName="Georgia",  fontSize=12,  width=None,  description=''):
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
