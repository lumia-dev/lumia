from loguru import logger
from functools import wraps
import time
from .system import colorize


def logged(func):
    """
    Decorator to display when a function is called, and with what arguments.
    Usage: just add "@logged" as a decorator.
    
    For example, consider the following function:
    
    ```
        from lumia.utils.debug import logged
        
        @logged
        def my_func(a, b, c=None):
            ...
    ```

    Then, calling `my_func(1, 2, False)` will (in addition to whatever the function is doing) send the following messages to the logger:
    ```
    my_func called with positional arguments:
    - 1
    - 2
    and with optional arguments:
    - c = False
    ```
    
    The messages are logged at the "debug" level, so won't be displayed unless the logging level is set to DEBUG.
    """
    @wraps(func)
    def inner(*args, **kwargs):
        txt = colorize(f"\n<u>{func.__module__}.{func.__name__}</u> called ")
        if len(args) > 0 :
            txt += f"with positional arguments:\n"
            for a in args:
                txt += f'- {a}\n'
            
            #logger.opt(ansi=True).debug(f"<i><u>{func.__module__}.{func.__name__}</></> called with positional arguments:")
            #[logger.debug(f'    {a}') for a in args]
        if kwargs :
            if len(args) > 0 :
                txt += 'and '
                #logger.debug("and keyword arguments:")
                txt += f"with optional arguments:\n"
                #logger.opt(ansi=True).debug(f"<i><u>{func.__module__}.{func.__name__}</></> called with keyword arguments:")
            for k, v in kwargs.items():
                txt += f'- {k} = {v}\n'
                #logger.debug(f"    {k} : {v}")
        logger.debug(txt.rstrip())
        return func(*args, **kwargs)
    return inner


def timer(func):
    """
    
    """
    @wraps(func)
    def inner(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        logger.opt(ansi=True).debug(f"<i><u>{func.__module__}.{func.__name__}</></> ran in {end - start} seconds")
        return result
    return inner