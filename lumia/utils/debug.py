from loguru import logger
from functools import wraps
import time
from .system import colorize
import inspect


def trace_args(*parameters):
    """
    Decorator that shows a log message when a function is called, and, possibly, the value of (some of) its arguments
    
    Usage example:
    
    ```
    from lumia.utils import debug
    
    @debug.trace_args("param2", "option_x")
    def my_func(param1, param2, option_a = 1, option_2 = None, option_x = False):
        ...
    ```
    
    Calling "my_func(1, 2, option_2 = 3)" will show a debug message indicating that the function has been called, and that the value of "param2" was "2" and that the value of "option_x" was "False".
    """
    def outer(func):
        def inner(*args, **kwargs):
            # Create a dict that contains both positional and keyword arguments, if they are in the "parameters" list
            diag = {}
            for (par, value) in zip(inspect.signature(func).parameters.items(), args):
                if par[1].default == inspect.Parameter.empty and par[1].kind != inspect.Parameter.VAR_POSITIONAL:
                    if par[0] in parameters :
                        diag[par[0]] = value
            for k, v in kwargs.items():
                if k in parameters:
                    diag[k] = v

            msg = f"<y>{func.__module__}.{func.__name__}</> ({inspect.getfile(func)})"
            if diag:
                msg += f' called with arguments <y>{diag}</y>'
            
            # Log
            logger.opt(colors=True, depth=1, capture=True).debug(msg)
            #logger.opt(colors=True, depth=1).debug(f"Function <u>{modname}.{funcname}</> called by <u>{callmodname}{callfuncname}</> ({filename}line {frame.lineno}), with arguments {diag}")
            x = func(*args, **kwargs)
            return x
        return inner
    return outer
            

def trace_call(func):
    """
    Simple decorator that shows a log message when a function is called
    
    Usage example
    ```
    from lumia.utils import debug
    
    @debug.trace_call
    def my_func(*args, **kwargs):
        ...
    ```
    
    Calling "my_func" will lead to a debug logging message
    """
    @wraps(func)
    def inner(*args, **kwargs):
        frame = inspect.stack()[1]
        
        # Ensure that the "<" are escaped
        modname = func.__module__.replace('<', '\<')#.replace('>', '\>')
        funcname = func.__name__.replace('<', '\<')
        if inspect.getmodule(frame.frame):
            callmodname = inspect.getmodule(frame.frame).__name__.replace('<', '\<') + '.'
        else :
            callmodname = '__main__.'
        callfuncname = frame.function.replace('<', '\<')
        filename = f'file {frame.filename}, '
        if frame.filename == '<string>':
            filename = ''
    
        # Log
        logger.opt(colors=True).debug(f"{modname}.{funcname} called by <u>{callmodname}{callfuncname}</> ({filename}line {frame.lineno})")
        
        # Return the actual function result
        return func(*args, **kwargs)
    return inner


def logged(func, *wrapper_args):
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
    #@wraps(func)
    def inner(*args, **kwargs):
        txt = colorize(f"\n<u>{func.__module__}.{func.__name__}</u> called ")
        if not wrapper_args :
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
        else :
            txt += 'with optional arguments:\n'
            for k in wrapper_args:
                txt += f'- {k} = {kwargs[k]}\n'
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
        logger.opt(colors=True).debug(f"<y>{func.__module__}.{func.__name__}</> ran in <r>{end - start}</r> seconds")
        return result
    return inner