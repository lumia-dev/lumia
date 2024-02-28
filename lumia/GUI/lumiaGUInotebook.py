#!pip install jupyter_ui_poll
from importlib import reload
import time
from jupyter_ui_poll import ui_events
import time
import guiElements_ipyWdg as ge
from IPython.display import display, HTML,  clear_output
import ipywidgets as wdg



def doThis():
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
    filetypes="*.yml"
    ui_done = False

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
    print(f"Have uploaded {filename}. Continuing execution...")
    return filename

title='Open existing LUMIA configuration file:'
filetypes='*.yml'
filename =doThis()
#filename = guiFileDialog(filetypes=filetypes, title=title)
print('more stuff...') 



if "lumiaGUI" not in dir():
    %run ./lumiaGUI
else:
    reload(lumiaGUI)
