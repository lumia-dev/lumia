import os
import subprocess
from loguru import logger


class RcloneArchive:
    def __init__(self, remote, ignore_existing=True):
        remote = remote.replace('rclone:', '')
        if ':' in remote :
            remote, path = remote.split(':')
        else :
            remote, path = remote, ''
        self.remote = f'{remote}:{path}'
        self.remote_root = remote
        self.remote_path = path
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

    def put(self, file, destpath, destfname):
        destpath = os.path.join(self.remote_path, destpath)
        cmd = ['rclone', 'copyto', file, f'{self.remote_root}:{destpath}/{destfname}']
        logger.info(' '.join(cmd))
        _ = subprocess.check_output(cmd)
        try :
            cmd = ['rclone', 'lsf', f'{self.remote_root}:{destpath}/{destfname}']
            logger.info(' '.join(cmd))
            subprocess.check_output(cmd)
            return True
        except subprocess.CalledProcessError :
            logger.warning(f"Copy of file {file} to {self.remote_root}:{destpath}/{destfname} failed.")
            return False
    
    def checkFile(self, filename):
        # Get the list of files in the rclone repo, in the same folder (only if we are in a new folder)
        path, file = os.path.split(filename)
        if path not in self.remote_structure :
            logger.info(f"Retrieving list of files in rclone folder {self.remote}:{path})")
            cmd = ['rclone', 'lsf', f'{self.remote}/{path}']
            logger.info(' '.join([c for c in cmd]))
            self.remote_structure[path] = subprocess.check_output(cmd, universal_newlines=True).split('\n')
        return file in self.remote_structure[path]
    
    
class LocalArchive:
    def __init__(self, local, ignore_existing=True, max_attempts=3, mkdir=False):
        local = local.replace('local:', '')
        if not os.path.exists(local) and mkdir :
            try :
                logger.info(f"Creating archive folder {local}")
                os.makedirs(local)
            except PermissionError:
                logger.error(f"Insufficient permissions to create path '{local}'")
                raise PermissionError
        self.remote = local
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

    def put(self, file, destpath, destfname):
        if not os.path.exists(destpath):
            os.makedirs(destpath)
        
        cmd = ['rsync', '-ah', f"{file}", os.path.join(destpath, destfname)]
        _ = subprocess.check_output(cmd)
        success = os.path.exists(os.path.join(destpath, destfname))
        
        return success
            
            
class Archive:
    def __init__(self, key, parent=None, *args, **kwargs):
        logger.debug(key)
        logger.debug(parent)
        self.key = key
        if parent is not None :
            if parent.key is not None :
                self.parent = parent 
        if key is not None:
            if key.startswith('rclone:'):
                self.archive = RcloneArchive(key, *args, **kwargs)
            elif key.startswith('local:'):
                self.archive = LocalArchive(key, *args, **kwargs)
            elif os.path.isdir(key):
                self.archive = LocalArchive(key, *args, **kwargs)
            else : 
                logger.error(f"Un-recognized archive. Does the path '{key}' exist?")
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

    def put(self, file, dest='', destfname=None):
        if destfname is None :
            fname = os.path.basename(file)
        #dest = os.path.join(self.key, dest)
        success = self.archive.put(file, dest, fname)
        return success


from dataclasses import dataclass

@dataclass
class Rclone:
    path: str
    protocol: str='rclone'
    remote: str=None

    def __post_init__(self):
        """
        The default way to instantiate the Rclone archive is to pass a path, with the format: "rclone:remote:path". In that case, __post_init__ will then split this into three attributes: protocol, remote and path.
        """
        if self.path is not None and self.remote is None :
            self.protocol, self.remote, self.path = self.path.split(':')

    def download(self, remotepath: str, localpath: str) -> None:
        if self.path is None :
            return
        cmd = [self.protocol, 'copy', f'{self.remote}:{remotepath}', localpath]
        _ = subprocess.check_output(cmd)

    def get(self, filepath: str) -> bool:
        """
        If the file given by the "filepath" path is not already on disk, try to retrieve it from the archive.
        """
        if not os.path.exists(filepath):
            localpath, filename = os.path.split(filepath)
            remotepath = os.path.join(self.path, filename)
            self.download(remotepath, localpath)
        return os.path.exists(filepath)