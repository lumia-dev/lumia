import os
import subprocess
from loguru import logger
from dataclasses import dataclass
from typing import List

def runSysCmd(sCmd,  ignoreError=False):
    try:
        os.system(sCmd)
    except:
        if(ignoreError==False):
            logger.error(f"Error: Failed to execute system command >>{sCmd}<<. Please check your write permissions and possibly disk space etc.")
        return False
    return True

@dataclass
class Rclone:
    path: str
    protocol: str = 'rclone'
    remote: str = None
    # format of 'archive' key in config.yaml::: localFolder: protocol (allows to add options for swestore tokens): remoteEntity: remotePath
    # The motives for the changes to the original class Rclone implementation are these: 
    # The protocol (rclone) and target folder had been presumed to have the same name in the original archive.py.  That is an issue
    # if one needs to send a token for Swestore access with the rclone command. Hence, I addeed an additional key to the beginning
    # of the archive key in the control_inversion.yaml file so that the protocol can be accompanied by the tokens and the target folder 
    # can have a normal name. However, we maintain backward compatibility if only 3 instead of the now expected 4 arguments are
    # found in the yaml key $machine.archive
    def __post_init__(self):
        """
        The default way to instantiate the Rclone archive is to pass a path, with the format: "rclone:remote:path". In that case, __post_init__ will then split this into three attributes: protocol, remote and path.
        """
        if self.path is not None and self.remote is None :
            lst = self.path.split(':')
            paramsCount = len(lst)
            if(paramsCount<4): # old behaviour that assumes that the local directory is named 'rclone' like the protocol
                self.protocol, self.remote, self.remotePath = self.path.split(':')
                self.localPth=self.protocol
            else:
                # More important than allowing a different local folder name is the ability to send options with the protocol like an authentication token for swestore.
                self.localPath, self.protocol, self.remote, self.remotePath = self.path.split(':')
            logger.debug(f'archive.rclone.__post_init__: self.protocol={self.protocol}')

    def download(self, remotepath: str, localpath: str) -> None:
        if self.path is None :
            return
        #cmd = [self.protocol, 'copy', f'{self.remote}:{remotepath}', localpath]
        cmd = f'{self.protocol} copy {self.remote}:{self.remotePath} {self.localPath}'
        try :
            # Allow for commandline options like swestore access tokens be passed
            rStr=subprocess.check_output(cmd, text=True, shell=True)
            return(rStr.splitlines())
        except subprocess.CalledProcessError as e:
            logger.exception(f"File {remotepath} not found on archive {self.protocol}:{self.remote}", traceback=False)
            output = e.output.decode()
            logger.exception(f' {str(output)}' )
        except Exception as e:
            # check_call can raise other exceptions, such as FileNotFoundError
            logger.exception(f' {str(e)}' )

    def lsf(self) -> List[str]:
        """
        Return a list of the files present on the archive
        """
        #return subprocess.check_output(['rclone', 'lsf', f'{self.remote}:{self.path}']).decode().split()
        #cmd = [self.protocol, 'lsf', f'{self.remote}:{self.path}']
        cmd = f'{self.protocol} lsf {self.remote}:{self.remotePath}'
        # print(f'cmd={cmd}') cmd=rclone --client-cert=/tmp/x509up_u1001 --client-key=/tmp/x509up_u1001 lsf swestore:/snic/tmflex/LUMIA/fluxes/nc/eurocom025x025/H
        try :
            # Allow for commandline options like swestore access tokens be passed
            rStr=subprocess.check_output(cmd, text=True, shell=True)
            return(rStr.splitlines())
        except subprocess.CalledProcessError as e:
            logger.exception(f"File {self.remote}:{self.remotePath} not found on archive {self.protocol}:{self.remote}", traceback=False)
            output = e.output.decode()
            logger.exception(f' {str(output)}' )
        except Exception as e:
            # check_call can raise other exceptions, such as FileNotFoundError
            logger.exception(f' {str(e)}' )

    def get(self, filepath: str) -> bool:
        """
        If the file given by the "filepath" path is not already on disk, try to retrieve it from the archive.
        """
        logger.debug(f'archive.get(): )filepath={filepath}')
        if os.path.dirname(filepath) == '':
            filepath = os.path.join('.', filepath)
        logger.debug(f'Rclone.get(): Reading input file={filepath}')
        
        if self.path is not None:
            if(os.path.exists(filepath)):
                logger.info(f'File {filepath} found locally.')
            else:
                localpath, filename = os.path.split(filepath)
                remotepath = os.path.join(self.remotePath, filename)
                logger.info(f"Requested input file does not exist locally. Attempting to download it with {self.protocol}:{self.remote}:{remotepath}")
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
