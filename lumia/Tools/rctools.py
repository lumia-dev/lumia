#!/usr/bin/env python
import re
from numpy import unique
import datetime
import os
import sys
import logging
from numpy import ndarray

logger = logging.getLogger(__name__)

class rc:
    def __init__(self, file=None, verbosity=1):
        self.verbosity=verbosity
        self.keys = {}
        self.includes = []
        if file != None: self.read(file)

    def setTime(self, attr, key, fail=False, fmt='%Y%m%d%H%M%S'):
        if self.haskey(key) :
            val = self.get(key)
            if type(val) == int : val = str(val)
            if type(val) == str :
                val = datetime.datetime.strptime(val, fmt)
            else :
                val = datetime.datetime(*val)
            self.setkey(key, val)
            setattr(self, attr, self.get(key))
        else :
            if fail :
                self.message('Missing key [%s], time attribute "%s" could not be set'%(key, attr))
                
    def haskey(self, key):
        return key in self.keys

    def read(self, file, comment='!'):
        self.filename = os.path.basename(file)
        self.dirname = os.path.dirname(file)
        self.curpath = os.getcwd()
        if self.dirname == '': self.dirname = '.'
        if self.curpath == '': self.curpath = '.'
        # save the current directory and change to the directory where the rc-file is located (to avoid errors with relative path of included rc-files)
        os.chdir(self.dirname)
        self.message('Parse rc-file %s'%os.path.join(self.dirname, self.filename))
        try :
            f1 = open(self.filename, 'r')
        except IOError :
            self.fail("Rc-file <p:%s> could not be opened. Quitting ..."%self.filename)
        lines = f1.readlines()
        self.lines = lines
        f1.close()
        # remove comments and white lines:
        lines = [(il, l.strip()) for (il, l) in enumerate(lines) if l.strip() != '']
        lines = [(il, l) for (il, l) in lines if not l.startswith(comment)]
        lines = [(il, l.split(comment)[0]) for (il, l) in lines]
        ln = [l[0] for l in lines]
        lines = [l[1].strip() for l in lines]
        # parse:
        for il, l in zip(ln, lines):
            if l.startswith('#'):
                if l.startswith('#include'):
                    val = self.matchval(l.split()[1])
                    self.append(rc(val))
                    # self.append(rc(l.split()[1]))
            else:
                self.parse(l, il)
        # go back to the original directory
        os.chdir(self.curpath)

    def message(self, msg):
        if self.verbosity > 0 :
            logger.info(msg)

    def fail(self, msg, err=RuntimeError):
        logger.critical(msg)
        sys.exit()

    def matchval(self, val):
        matches = re.findall(r'\${[^}]+}', val)
        for match in matches:
            matched = self.get(match.replace('${', '').replace('}', ''), tolist=False, convert=False)
            if type(matched) != str: matched = str(matched).strip()
            val = val.replace(match, matched)
        return val

    def append(self, rcobj):
        if type(rcobj) == str : rcobj = rc(file=rcobj)
        for key in rcobj.keys:
            self.keys[key] = rcobj.keys[key]
            #self.setkey(key, rcobj.keys[key])  # don't use setkey here as we want to preserve the original keys!

    def getall(self):
        for key in self.keys: setattr(self, key, self.get(key))

    def get(self, key, tolist=True, convert=True, default=None, todate=False, fmt=None, totype=None):
        try:
            val = self.keys[key]
        except:
            if default != None:
                return default
            if self.findParent(key) != None :
                val = self.get(self.findParent(key))
            else:
                import os
                if key in os.environ:
                    return os.environ[key]
                else:
                    self.message('ERROR: key "%s" not found' % key)
                    raise
        if type(val) == str:
            val = self.matchval(val)
            # matches = re.findall(r'\${[^}]+}',val)
            # for match in matches :
            #	matched = self.get(match.replace('${','').replace('}',''),tolist=False,convert=False)
            #        if type(matched) != str : matched = str(matched).strip()
            #	val = val.replace(match, matched)
            # self.keys[key] = val
            if tolist and ',' in val:
                val = [z.strip() for z in val.split(',')]
            else:
                tolist = False
            # try converting the value to a integer, logical or real type:
            if todate:
                val = datetime.datetime.strptime(val, fmt)
                return val
            if convert and not tolist:
                # first, try converting to integer
                try:
                    val = int(val)
                except ValueError:
                    # then to real
                    try:
                        val = float(val)
                    except ValueError:
                        # finally, to logical
                        if val == 'T': val = True
                        if val == 'F': val = False
                    except:
                        raise
            elif convert and tolist:
                for iv in range(len(val)):
                    # first, try converting to integer
                    try:
                        val[iv] = int(val[iv])
                    # then to real
                    except ValueError:
                        try:
                            val[iv] = float(val[iv])
                        except ValueError:
                            # finally, to logical
                            if val[iv] in ['T','True']: val[iv] = True
                            if val[iv] in ['F','False']: val[iv] = False
        if totype == bool:
            val = {'T':True, 'F':False, 1:True, 0:False, 'True':True, 'False':False}[val]
        return val

    def setkey(self, key, val):
        self.keys[key] = val

    def addInclude(self, file_to_include, parse=False):
        if parse :
            self.append(rc(file_to_include))
        else :
            self.includes.append(file_to_include)

    def findParent(self, key):
        key_elements = key.split('.')
        key_elements = [x for x in key_elements if x != '*']
        if len(key_elements) > 0 :
            # 1st, try replacing 1 element:
            combinations = []
            for kk in key_elements:
                combinations.append(key.replace(kk, '*'))
                if self.haskey(combinations[-1]): return combinations[-1]
            # 2nd, try replacing one more element:
            for cc in combinations:
                res = self.findParent(cc)
                if res != None: return res
        else :
            return None
        return None

    def parse(self, line, ln):
        try:
            line = line.split(':')
            key = line[0]
            val = ':'.join([x for x in line[1:]])
            val = val.strip()
            key = key.strip()
            #converted = False
            #if not converted:
            #    try:
            #        val = int(val)
            #    except ValueError:
            #        try:
            #            val = float(val)
            #        except ValueError:
            #            pass
            if key in self.keys:
                self.message("redefining value of key %s:" % key)
                self.message("%s --> %s" % (self.keys[key], val))
            self.keys[key] = val
        except:
            self.message("error parsing line %i :" % (ln + 1))
            self.message(self.lines[ln])
            raise RuntimeError

    def write(self, path):
        self.message('Writing rc-file %s'%path)
        rcfout = open(path, 'w')

        for incl in self.includes :
            rcfout.write("#include %s\n"%incl)

        maxkeylen = max([len(key) for key in self.keys])
        fmt = '%%-%is : %%s\n' % maxkeylen
        oldprefix = None
        for key in sorted(self.keys):
            # print a blank line between different sections of the rc-file
            prefix = key.split('.')[0]
            if oldprefix == None : oldprefix = prefix
            if prefix != oldprefix :
                rcfout.write('\n')
                oldprefix = prefix
            # retrieve the value of the key and print it
            val = self.get(key)
            if type(val) == datetime.datetime: val = val.strftime('%Y%m%d%H%M%S')
            if isinstance(val, (list, tuple, ndarray)):
                val = ', '.join([str(x) for x in val])
            rcfout.write(fmt % (key, val))
        rcfout.close()
        return path
