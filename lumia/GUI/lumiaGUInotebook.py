#!pip install jupyter_ui_poll
#!pip install distro
#!pip install loguru
#!pip install screeninfo
#!pip install git+https://github.com/ecederstrand/exchangelib

from importlib import reload
import ipywidgets as wdg


if "lumiaGUI" not in dir():
    %run ./lumiaGUI
else:
    reload(lumiaGUI)
