#!/usr/bin/env python
import importlib
import glob, os

Interfaces = {}
for module in glob.glob(os.path.dirname(__file__)+'/*.py'):
    modname = os.path.basename(module).strip('*.py')
    if not modname.startswith('__'):
        mod = importlib.import_module('.'+modname, __name__)
        if hasattr(mod, 'invcontrol') and hasattr(mod, 'obsoperator'):
            Interfaces[mod.invcontrol, mod.obsoperator] = mod.Interface


class Interface:
    def __init__(self, invcontrol, obsop, *args, **kwargs):
        self._interface = Interfaces[(invcontrol, obsop)](*args, **kwargs)

    def __getattr__(self, item):
        return getattr(self._interface, item)