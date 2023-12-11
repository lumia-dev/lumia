#!/usr/bin/env python

from functools import wraps
from typing import Union
from datetime import datetime
import warnings
from omegaconf import DictConfig
from loguru import logger
from typing import List


def splitkey(key: Union[str, List[str]]) -> List[str]:
    """
    Split a dot-list key ("a.b.c") in a proper list (["a","b","c"])
    """
    if isinstance(key, str):
        key = key.split('.')
    return key


def rcf_legacy(func):
    """
    Decorator to enable the use of legacy RcFile.get keyword arguments (tolist, todate, fmt, totype and convert) in Config.get.
    A deprecation warning will be raised if these keyword arguments are found, but they will be handled as before.

    :param func:
    :return:
    """

    def rcvalue_convert(val, tolist: Union[bool, str], todate: bool, fmt: str, totype):
        logger.debug(val, tolist, todate, fmt, totype)
        # Convert to a list if a comma is found, unless specifically requested to do otherwise
        if (tolist and ',' in val) or (tolist == 'force'):
            val = [z.strip() for z in val.split(',') if z.strip() != '']
            tolist = True
        else:
            tolist = False

        # Convert to date if requested:
        if todate:
            val = datetime.strptime(val, fmt)
            return val

        # Try converting to int, bool or real
        if not tolist:
            val = [val]

        for iv in range(len(val)):
            try:
                # Try integer first
                val[iv] = int(val[iv])
            except ValueError:
                try:
                    # Then try float
                    val[iv] = float(val[iv])
                except ValueError:
                    # Finally, try logical:
                    if val[iv] in ['T', 'True']:
                        val[iv] = True
                    elif val[iv] in ['F', 'False']:
                        val[iv] = False

            if totype is not None:
                val[iv] = totype(val[iv])

        if not tolist and len(val) == 1:
            val = val[0]
        return val

    @wraps(func)
    def inner(self, *args, **kwargs):

        # arguments specific to RcFile.get
        rc_kwargs = {'tolist': True, 'convert': True, 'todate': False, 'fmt': None, 'totype': None}

        # Has any RcFile keyword been requested?
        convert = any(set(kwargs.keys()).intersection(rc_kwargs.keys()))
        for arg in rc_kwargs.keys():
            if arg in kwargs:
                warnings.warn(f"'{arg}' argument passed to get method of {self.__class__} is deprecated and will be removed in a future version")

        # Remove RcFile arguments from kwargs, if they exist:
        for k, v in rc_kwargs.items():
            rc_kwargs[k] = kwargs.pop(k, v)

        # Call the function normally
        val = func(self, *args, **kwargs)

        # Return if no RcFile argument has been specified:
        if not convert:
            return val

        # If RcFile arguments have been specified, raise a deprecation message and
        # apply the old methods:
        if rc_kwargs['convert'] and type(val) == str:
            return rcvalue_convert(val, rc_kwargs['tolist'], rc_kwargs['todate'], rc_kwargs['fmt'], rc_kwargs['totype'])

        logger.error("Something went wrong when parsing the rc-file")
        logger.error(f"key: {args}")
        logger.error(f"options: {rc_kwargs}")

    return inner


def dict_syntax(func):
    """
    Decorator that enables using the python dictionary-like syntax (obj.get(key, default_value)) with Config objects,
    which normally require the more explicit obj.get(key, default=default_value, **kwargs) syntax.
    """

    @wraps(func)
    def inner(self, *args, **kwargs):
        # Classic dict-sytle syntax:
        if len(args) == 2 and not kwargs:
            return func(self, args[0], default=args[1])

        # Otherwise, just pass the arguments as they are
        return func(self, *args, **kwargs)

    return inner


def flatten(data: dict, prefix='', separator='.') -> dict:
    """
    Flatten an OmegaConf.DictConfig structure: the hierarchy of keys is flattened, and the name of the keys
    is modified to reflect the original hierarchy:
    conf = {'level1': {
                {level2 :
                    {'key1' : value, 'key2': value},
                'keyA': value},
            'toplevelkey': value
            }
    will become:
    conf = {
        level1.level2.key1 : value,
        level1.level2.key2 : value,
        level1.keyA : value,
        toplevelkey : value
    }
    """
    if isinstance(data, (dict, DictConfig)):
        dd = {}
        for kk, vv in data.items():
            for k, v in flatten(vv, kk, separator).items():
                dd[prefix + separator + k if prefix else k] = v
        return dd
        # return OmegaConf.create(dd)
    else:
        return {prefix: data} if prefix else data
        # return OmegaConf.create({ prefix : data })
