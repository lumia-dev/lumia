import os
import subprocess
from loguru import logger
from dataclasses import dataclass
from typing import List


@dataclass
class Rclone:
    path: str
    protocol: str = 'rclone'
    remote: str = None

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
        try :
            _ = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            logger.exception(f"File {remotepath} not found on archive {self.protocol}:{self.remote}", traceback=False)

    def lsf(self) -> List[str]:
        """
        Return a list of the files present on the archive
        """
        return subprocess.check_output(['rclone', 'lsf', f'{self.remote}:{self.path}']).decode().split()

    def get(self, filepath: str) -> bool:
        """
        If the file given by the "filepath" path is not already on disk, try to retrieve it from the archive.
        """
        if os.path.dirname(filepath) == '':
            filepath = os.path.join('.', filepath)
        
        logger.info(f"Getting file {filepath}")
        if self.path is not None:
            if not os.path.exists(filepath):
                localpath, filename = os.path.split(filepath)
                remotepath = os.path.join(self.path, filename)
                logger.info(f"File not found. Try downlaod it from {self.protocol}:{self.remote}:{remotepath}")
                self.download(remotepath, localpath)
        return os.path.exists(filepath)

    def put(self, filename : str, destpath : str = None, destname : str = None) -> bool:
        """
        Upload the file given by the "filename" argument to the archive
        Optionally, a specific destination folder and/or filename can be requested, using the "destpath" and "destname" keywords
        Return: True if successful, False otherwise
        """
        if destname is None :
            # if destname is none, use the same filename as the original file
            destname = os.path.basename(filename)

        if destname != os.path.basename(destname) and destpath is None :
            # If not destpath has been provided and destname is a path, then split destname in destpath + destname
            destpath, destname = os.path.split(destname)

        if destpath is None :
            # Finally, if destpath is still None, then set it to an empty string
            destpath = ''

        # Upload the file
        destpath = os.path.join(self.path, destpath)
        cmd = ['rclone', 'copyto', filename, f'{self.remote}:{destpath}/{destname}']
        logger.info(' '.join(cmd))

        try :
            logger.info(' '.join(cmd))
            _ = subprocess.check_output(cmd)
            subprocess.check_output(cmd)
            return True

        except subprocess.CalledProcessError :
            logger.warning(f"Copy of file {filename} to {self.remote}:{destpath}/{destname} failed.")
            return False
