import os
import logging
import subprocess
logger = logging.getLogger(__name__)


class RcloneArchive:
    def __init__(self, remote, ignore_existing=True):
        remote = remote.replace('rclone:', '')
        if ':' in remote :
            remote, path = remote.split(':')
        else :
            remote, path = remote, ''
        self.remote = f'{remote}:{path}'
        self.ignore_existing = ignore_existing
        self.remote_structure = {}
        
    def get(self, file, dest='.'):
        source, filename = os.path.split(file)
        
        # Don't copy an existing file, unless explicitly asked to do otherwise
        # even if the files are different (no check!)
        outfile = os.path.join(dest, filename)
        if os.path.exists(outfile) and self.ignore_existing :
            return True
        
        success = False
        # Check the content of the remote folder:
        if self.checkFile(file):
            cmd = ['rclone', 'copy', f'{self.remote}/{file}', dest]
            logger.info(' '.join(cmd))
            _ = subprocess.check_output(cmd)
            success = os.path.exists(outfile)
            
        return success
    
    def checkFile(self, filename):
        # Get the list of files in the rclone repo, in the same folder (only if we are in a new folder)
        path, file = os.path.split(filename)
        if path not in self.remote_structure :
            logger.info("Retrieving list of files in rclone folder {self.remote:path})")
            self.remote_structure[path] = subprocess.check_output(['rclone', 'lsf', f'{self.remote}/{path}'], universal_newlines=True).split('\n')
        return file in self.remote_structure[path]
    
    
class LocalArchive:
    def __init__(self, remote, ignore_existing=True, max_attempts=3):
        self.remote = remote
        self.ignore_existing = ignore_existing
        self.max_attempts = max_attempts
    
    def get(self, file, dest='.'):
        logger.debug(f"Try copying file {file} from path {self.remote}" )
        if not file.startswith('/'):
            file = os.path.join(self.remote, file)
        
        # If the file doesn't exist on the archive, stop here
        if not os.path.exists(file):
            logger.debug(f"File {file} not found on archive {self.remote}")
            return False
        
        # If the file already exists in the destination, don't copy it (unless ignore_existing is
        # set to False)
        source, filename = os.path.split(file)
        outfile = os.path.join(dest, filename)
        if os.path.exists(outfile) and self.ignore_existing :
            logger.debug(f"File {file} already present locally and will not be re-downloaded")
            return True
        
        # If the file needs to be retrieved, attempt it
        cmd = ['rsync', '-ah', f"{file}", f"{dest}/"]
        _ = subprocess.check_output(cmd)
        success = os.path.exists(outfile)
        
        return success
            
            
class Archive:
    def __init__(self, key, parent=None, *attrs, **kwattrs):
        self.key = key
        if parent is not None :
            if parent.key is not None :
                self.parent = parent 
        if key is not None:
            if os.path.isdir(key):
                self.archive = LocalArchive(key, *attrs, **kwattrs)
            elif key.startswith('rclone:'):
                self.archive = RcloneArchive(key, *attrs, **kwattrs)
            else : 
                logger.error(f"Un-recognized meteo archive: {key}")
                raise RuntimeError
    
    def get(self, file, dest='.', fail=True):
        success = self.archive.get(file, dest)
        if not success :
            if hasattr(self, 'parent'):
                logger.info(f"File {file} couldn't be downloaded from archive {self.key}. Trying archive {self.parent.key}")
                success = self.parent.get(file, dest)
            else :
                msg = f"File {file} could not be downloaded from archive {self.key}."
                if fail: 
                    logger.error(msg)
                    raise RuntimeError
                else :
                    logger.info(msg)
        return success