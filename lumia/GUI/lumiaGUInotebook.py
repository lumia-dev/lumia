#!pip install jupyter_ui_poll
from importlib import reload
import time
from jupyter_ui_poll import ui_events
import time
import guiElements_ipyWdg as ge
from IPython.display import display, HTML,  clear_output
import ipywidgets as wdg

if(1>0):
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
    # Create the widget
    widget = wdg.Dropdown(
        options=['a','b','c'],
        value=None,
        disabled=False)
    
    # Create a function to continue the execution
    button_clicked = False
    def on_click(b):
        global button_clicked
        button_clicked = True
        print('button clicked')
    
    # Create a button widget
    button = wdg.Button(description="Continue")
    button.on_click(on_click)
    
    # Display the widget and button
    display(widget, button)
    
    # Wait for user to press the button
    with ui_events() as poll:
        while button_clicked is False:
            poll(10)          # React to UI events (upto 10 at a time)
            time.sleep(0.1)
    
    # Get the selected value from the widget
    selected_value = widget.value
    print("Selected value:", selected_value)
    print("Continuing execution...")

    filename=None
    filetypes="*.yml"
    ui_done = False

    def on_upload_btn_click(change):
        uploaded_file = next(iter(upload_btn.value.values()))
        #content = uploaded_file['content']
        print(f"File {uploaded_file} uploaded successfully!")
        global ui_done
        ui_done = True
        #btn.description = 'ok'       
        return uploaded_file #content

    upload_btn = wdg.FileUpload(accept=filetypes, multiple=False)
    #upload_btn.on_click(on_upload_btn_click)
    #upload_btn.observe(on_upload_btn_click, names='value')
    #display(upload_btn)

    # Create a function to continue the execution
    button_clicked = False
    def on_click(b):
        global button_clicked
        button_clicked = True
        print('button clicked')
    
    # Create a button widget
    button = wdg.Button(description="Continue")
    button.on_click(on_click)
    
    # Display the widget and button
    display(upload_btn, button)
    
    # Wait for user to press the button
    with ui_events() as poll:
        while button_clicked is False:
            poll(10)          # React to UI events (upto 10 at a time)
            time.sleep(0.1)

    #filename =on_upload_btn_click
    filename = upload_btn.value[0]
    print(f"Have uploaded {filename}. Continuing execution...")
    



if "lumiaGUI" not in dir():
    %run ./lumiaGUI
else:
    reload(lumiaGUI)
