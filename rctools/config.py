#!/usr/bin/env python

from omegaconf import OmegaConf, DictConfig, errors, ListConfig
from typing import Union, List, IO, Any, Dict
from pathlib import Path
from rctools.utils import flatten, dict_syntax, rcf_legacy, splitkey
from rctools.resolvers import resolvers
import re
import yaml
import pprint
from loguru import logger
import inspect


class Config:
    def __init__(self, file_or_obj: Union[str, Path, dict, DictConfig, "Config"]):
        self._data = None
        if isinstance(file_or_obj, (Path, str)):
            self._data = self.load(file_or_obj)
        elif isinstance(file_or_obj, (DictConfig)):
            self._data = file_or_obj
        elif isinstance(file_or_obj, dict):
            self._data = OmegaConf.create(file_or_obj)
        elif isinstance(file_or_obj, Config):
            self._data = file_or_obj._data

        self.haskey = self.has
        self.getkey = self.get
        self.setkey = self.set

    def write(self, filename: Union[Path, str, IO[Any]], resolve: bool = False) -> Path:
        data = self

        if resolve:
            data = data.resolve()

        # Flatten to be able to loop on keys more easily
        data = data.flatten().as_dict()

        # Convert pathlib Paths to str
        # Ensure that there are no extra "'"
        keys_to_replace = {}
        for k, v in data.items():
            if isinstance(v, Path):
                data[k] = str(v)
            if "'" in k:
                keys_to_replace[k] = k.replace("'", '')
        for k, v in keys_to_replace.items():
            data[v] = data[k]
            del data[k]

        # write using the standard yaml library to have lists in compact format
        with open(filename, 'w') as fid:
            yaml.dump(data, fid, default_flow_style=None)

        return filename

    def load(self, file: Union[Path, str, IO[Any]], comment='#'):
        with open(file, 'r') as fid :
            lines = fid.readlines()

        # In any case, remove commented lines and empty lines:
        lines = [_ for _ in lines if not _.startswith(comment)]
        lines = [_ for _ in lines if _.strip()]

        # Is the file yaml or rc file? yaml files have lists organised in blocks over multiple lines that we need to fold into one
        # TODO: cannot handle yml list, i.e. {steps: \n -apri \n -apos} - instead of {steps: ['apri', 'apos']}
        # 1) place all list entries into the same line as the corresponding key
        # 2) drop all lines starting with ' - ' where the number of leading spaces may vary
        idxCurListLine=0
        junkLines=[]
        bAssemblingList=False
        for i, line in enumerate(lines):
            if(' - ' in line):
                if(bAssemblingList==False):
                    idxCurListLine=i-1
                    lines[idxCurListLine]=lines[idxCurListLine].strip("\n")
                    lines[idxCurListLine]+=' [\''
                else:
                    lines[idxCurListLine]+=', \''
                bAssemblingList=True
                (junk, payload)=line.split(" - ",1)
                lines[idxCurListLine]+=payload.strip("\n")+'\''
                junkLines.append(line)
            else:
                if(bAssemblingList==True):
                    lines[idxCurListLine]+=' ]\n'
                bAssemblingList=False        
        for line in junkLines:
            lines.remove(line)  # drop any processed line starting with a minus sign

        values = [_.split(':')[1].strip() for _ in lines]  
        if '' in values:
            return OmegaConf.load(file)
        else :
            # Otherwise, assume it's a rc-file (a.b : v), and convert it to dot-list (a.b=v) for import:
            dotlist = []
            for line in lines:
                key, value = line.split(comment)[0].split(':', maxsplit=1)
                dotlist.append(key.strip() + '=' + value.strip())
            return OmegaConf.from_dotlist(dotlist)

    def resolve(self) -> "Config":
        data = self._data.copy()
        OmegaConf.resolve(data)
        return Config(data)

    def flatten(self) -> "Config":
        return Config(flatten(self.as_dict()))

    def as_dict(self) -> dict:
        return OmegaConf.to_container(self._data)

    @dict_syntax
    @rcf_legacy
    def get(self, key: Union[str, List[str]], **kwargs) -> Any :
        try :
            value = self._data
            for kk in splitkey(key):
                value = value[kk]
        except (TypeError, errors.ConfigKeyError) as e:
            # We get TypeError when we are already at the lowest level of the hierarchy (e.g. trying to get a.b.c, but a key a.b: value exists)
            # and we get ConfigKeyError in other cases (e.g. a.b is a section but doesn't contain a key or subsection c).
            # TODO: Not working as described. If the key emissions.co2.resample_from is missing, no exception is triggered and 'None' instead of the default value is returned /cec-ami 2023-12-13
            parent = self._find_parent(key)
            if parent is not None :
                value = self.get(parent)
            elif 'fallback' in kwargs:
                value = self.get(kwargs['fallback'])
            elif 'default' in kwargs:
                value = kwargs['default']
            else :
                logger.critical(f'Key "{key}" not found!')
                raise e

        if isinstance(value, ListConfig):
            value = list(value)

        return value

    def checkDefault(self, keyval,  default) -> Any :
        if((keyval is None) or (keyval=='') or (keyval=='None')): # an alternative workaround for the default value being ignored in rcf.rcfGet()
            keyval=default
        return(keyval)

    def rcfGet(self, key: Union[str, List[str]], **kwargs) -> Any :
        try :
            value = self._data
            for kk in splitkey(key):
                value = value[kk]
        except (TypeError, errors.ConfigKeyError) as e:
            # We get TypeError when we are already at the lowest level of the hierarchy (e.g. trying to get a.b.c, but a key a.b: value exists)
            # and we get ConfigKeyError in other cases (e.g. a.b is a section but doesn't contain a key or subsection c).
            # TODO: Not working as described. If the key emissions.co2.resample_from is missing, no exception is triggered and 'None' instead of the default value is returned /cec-ami 2023-12-13
            parent = self._find_parent(key)
            if parent is not None :
                value = self.get(parent)
                # We have an issue if value is 'None' or None or ''
            elif 'fallback' in kwargs:
                value = self.get(kwargs['fallback'])
            elif 'default' in kwargs:
                value = kwargs['default']
            else :
                logger.critical(f'Key "{key}" not found!')
                raise e
            if((value is None) or (value=='None') or (value=='')):
                if 'fallback' in kwargs:
                    value = self.get(kwargs['fallback'])
                elif 'default' in kwargs:
                    value = kwargs['default']

        if isinstance(value, ListConfig):
            value = list(value)

        return value
        
        
    def getAlt(self, arg1,  arg2=None,  arg3=None,  arg4=None,  arg5=None,  default=None):
        # an alternative to the get() function that seems to have some intricate issues with the default value sent
        rVal=default
        try:
            if(arg2 is None):
                rVal=self.get['arg1']
            elif(arg3 is None):
                rVal=self.get['arg1']['arg2']
            elif(arg4 is None):
                rVal=self.get['arg1']['arg2']['arg3']
            elif(arg5 is None):
                rVal=self.get['arg1']['arg2']['arg3']['arg4']
            else:
                rVal=self.get['arg1']['arg2']['arg3']['arg4']['arg5']
        except:
            rVal=default
        return(rVal)
        

    def set(self, key: Union[str, List[str]], value):
        if type(value) in resolvers:
            value = resolvers[type(value)].reverse(value)
        key = splitkey(key)
        for kk in key[::-1]:
            value = {kk: value}
        self._data.merge_with(value)

    def has(self, key: Union[str, List[str]]) -> bool:
        try :
            value = self._data
            for kk in splitkey(key):
                value = value[kk]
            return True
        except errors.ConfigKeyError:
            return False
        

    def set_defaults(self, **defaults: Union[dict, DictConfig]):
        self._data = OmegaConf.merge(OmegaConf.create(defaults), self._data)

    def _find_parent(self, key) -> Union[str, None]:
        possible_matches = [k for k in self.flatten().keys() if '*' in k]
        matches = [k for k in possible_matches if re.match(k.replace('*', '\w+'), key)]
        while len(matches) > 1:
            # First, select the matches that branch the lowest:
            pos = [m.index('*') for m in matches]
            matches = [m for m in matches if m.index('*') == max(pos)]

            # Temporarily replace the first '*' by a '#' (to enable search to continue):
            matches = [m.replace('*', '#', 1) for m in matches]

        if len(matches) == 1 :
            return matches[0].replace('#', '*')
        else :
            return None

    def __contains__(self, item) -> bool:
        return self.has(item)

    def __getitem__(self, item):
        return self.get(item)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __repr__(self):
        return pprint.pformat(self.flatten().as_dict(), sort_dicts=False)

    def keys(self):
        return self._data.keys()

    def items(self):
        return self._data.items()

    def values(self):
        return self._data.values()
