#!pip install jupyter_ui_poll
from importlib import reload
import ipywidgets as wdg


if "lumiaGUI" not in dir():
    %run ./lumiaGUI
else:
    reload(lumiaGUI)
