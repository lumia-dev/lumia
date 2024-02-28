#!pip install jupyter_ui_poll
from importlib import reload
import time
from jupyter_ui_poll import ui_events
import time
import guiElements_ipyWdg as ge
from IPython.display import display, HTML,  clear_output
import ipywidgets as wdg


if "lumiaGUI" not in dir():
    %run ./lumiaGUI
else:
    reload(lumiaGUI)
