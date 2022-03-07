import os
import sys
import re
import inspect
from datetime import datetime
from numpy import ndarray
from loguru import logger


class RcFile:
    def __init__(self, file=None):
        self.keys = {}
        self.includes = []
        self.info = {}
        self.filename = file
        if file is not None :
            self.dirname = os.path.dirname(file)
        else :
            self.dirname = os.getcwd()
        if file is not None :
            self.read(file)
            
    def read(self, file, comment='!', level=0):
        #self.filename = os.path.basename(file)
        #self.dirname = os.path.dirname(file)
        includes = []
        
        with open(os.path.join(file), 'r') as f1 :
            lines = f1.readlines()
        
        # remove comments and white lines:
        lines = [(il, l.strip()) for (il, l) in enumerate(lines) if l.strip() != '']
        lines = [(il, l) for (il, l) in lines if not l.startswith(comment)]
        lines = [(il, l.split(comment)[0]) for (il, l) in lines]
        
        # Separate line numbers and content
        ln = [l[0] for l in lines]
        lines = [l[1].strip() for l in lines]
        
        # parse:
        for il, l in zip(ln, lines):
            if l.startswith('#include'):
                includes.append(l.split()[1])
                #self.append(RcFile(os.path.join(self.dirname, val)))
            else :
                self.parse(l, il)

        # Finally, go through the "#include":
        for incl in includes :
            self.read(os.path.join(self.dirname, self.matchval(incl)), level=level+1)

    def parse(self, line, ln):
        try :
            line = line.split(':')
            key = line[0]
            val = ':'.join([x for x in line[1:]])
            val = val.strip()
            key = key.strip()
            if key in self.keys:
                logger.info(f"redefining value of key {key} from {self.keys[key]} to {val}")
            self.keys[key] = val
        except Exception as e:
            logger.error(f"error parsing line {ln +1}: {self.lines[ln]}")
            logger.exception(e)
            sys.exit(1)

    def setkey(self, key, val):
        self.keys[key] = val
            
    def matchval(self, val):
        matches = re.findall(r'\${[^}]+}', val)
        if matches :
            for match in matches:
                matched = self.get(match.replace('${', '').replace('}', ''), convert=False)
                if type(matched) != str: 
                    matched = str(matched).strip()
                out = val.replace(match, matched)
            return self.matchval(out)
        else :
            return val
    
    def get(self, key, tolist=True, convert=True, default=None, todate=False, fmt=None, totype=None, info=None):
        key = self.matchval(key)
        
        # Check if the key is there
        if key in self.keys:
            val = self.keys[key]
            
        # Otherwise, check if it has a default value
        elif default is not None :
            val = default
            
        # Otherwise, check if a parent key exists
        elif self.findParent(key) is not None :
            val = self.get(self.findParent(key))
            
        # Finally, look for matching environment variables
        elif key in os.environ :
            val = os.environ[key]
            
        else :
            logger.error(f"key {key} not found in rc-file {os.path.join(self.dirname, self.filename)}")
            sys.exit(1)
        
        if info is not None :
            if isinstance(info, dict):
                info = info[key]
            self.setInfo(key, info)

        if convert and type(val) == str :
            # We don't want to call this for keys that are called internally (by
            # matchval), or on keys that have been set by setkey (which can contain
            # anything)
            val = self.convert(val, tolist, todate, fmt, totype)
        return val

    def convert(self, val, tolist, todate, fmt, totype):
        val = self.matchval(val)

        # Convert to a list if a comma is found, unless specifically requested to do otherwise
        if (tolist and ',' in val) or (tolist == 'force') :
            val = [z.strip() for z in val.split(',') if z.strip() != '']
            tolist = True
        else :
            tolist = False

        # Convert to date if requested:
        if todate :
            val = datetime.strptime(val, fmt)
            return val

        # Try converting to int, bool or real
        if not tolist :
            val = [val]

        for iv in range(len(val)):
            try :
                # Try integer first
                val[iv] = int(val[iv])
            except ValueError :
                try :
                    # Then try float
                    val[iv] = float(val[iv])
                except ValueError :
                    # Finally, try logical:
                    if val[iv] in ['T', 'True'] : 
                        val[iv] = True
                    elif val[iv] in ['F', 'False'] :
                        val[iv] = False

            if totype is not None :
                val[iv] = totype(val[iv])

        if not tolist and len(val) == 1 :
            val = val[0]
        return val

    def findParent(self, key):
        key_elements = key.split('.')
        key_elements = [x for x in key_elements if x != '*']
        if len(key_elements) > 0 :
            # 1st, try replacing 1 element:
            combinations = []
            for kk in key_elements:
                combinations.append(key.replace(kk, '*'))
                if combinations[-1] in self.keys: 
                    return combinations[-1]
            # 2nd, try replacing one more element:
            for cc in combinations:
                res = self.findParent(cc)
                if res is not None:
                    return res
        else :
            return None
        return None
    
    def setInfo(self, key, info):
        stack = inspect.stack()[2]
        file = stack.filename
        if os.path.isfile(file):
            module = inspect.getmodule(stack[0]).__name__
            module = f'{module}.' if module != '__main__' else ''
        else :
            module = ''
        cls = '' if 'self' not in stack[0].f_locals else f'{stack[0].f_locals["self"].__class__.__name__}.'
        function = stack.function if stack.function != '<module>' else '' 
        infodic = {'info':info, 'file':stack.filename, 'line':stack.lineno, 'method': f'{module}{cls}{function}'}
        if key not in self.info :
            self.info[key] = [infodic]
        else :
            self.info[key].append(infodic)
            
    def printInfo(self):
        for key in self.info :
            for info in self.info[key]:
                print(f'{key} : {self.get(key)} : {info}')

    def write(self, dest):
        logger.info(f"Writing rc-file {dest}")
        if not os.path.exists(os.path.dirname(dest)):
            os.makedirs(os.path.dirname(dest))

        rcfout = open(dest, 'w')

        maxkeylen = max([len(key) for key in self.keys])
        fmt = '%%-%is : %%s\n' % maxkeylen
        oldprefix = None
        for key in sorted(self.keys):
            # print a blank line between different sections of the rc-file
            prefix = key.split('.')[0]
            if oldprefix is None : 
                oldprefix = prefix
            if prefix != oldprefix :
                rcfout.write('\n')
                oldprefix = prefix
            # retrieve the value of the key and print it
            val = self.get(key)
            if isinstance(val, datetime): 
                val = val.strftime('%Y%m%d%H%M%S')
            if isinstance(val, (list, tuple, ndarray)):
                val = ', '.join([str(x) for x in val])
            rcfout.write(fmt % (key, val))
        rcfout.close()
        return dest
