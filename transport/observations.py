#!/usr/bin/env python

import os
import shutil
from pandas import DataFrame, read_hdf, isnull,  tseries,  offsets,  Period,  to_datetime
from numpy import array, nan
from datetime import datetime
import numpy as np
from tqdm import tqdm
from loguru import logger
from transport.core.model import FootprintFile
from typing import Type


def check_migrate(source, dest):
    if os.path.exists(dest):
        return True
    elif os.path.exists(source):
        shutil.copy(source, dest)
        return True
    return False


class Observations(DataFrame):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    @classmethod
    def read(cls, filename: str) -> "Observations":
        return cls(read_hdf(filename))

    @property
    def _constructor(self):
        """
        Ensures that the parent's (DataFrame) methods return an instance of Observations and not DataFrame
        """
        return Observations

    def write(self, filename: str) -> None:
        logger.debug(f'observations.write filename={filename} via self.to_hdf(filename, key=observations)')
        self.to_hdf(filename, key='observations')

    def gen_filenames(self) -> None:
        # h=self['height'].astype(float)  # the height column might be in Text format, which causes havoc....
        # fnames = self.code.str.lower() + h.map('.{:.0f}m.'.format) + self.time.dt.strftime('%Y-%m.hdf')
        fnames = self.code.str.lower() + self.height.map('.{:.0f}m.'.format) + self.time.dt.strftime('%Y-%m.hdf')
        self.loc[:, 'footprint'] = fnames


    def identifyMissingFootprints(self,  missingFootprintsRaw, sOutpPrfx=''):
        # missingFootprintsRaw is a pandas dataframe with obs entries that have no existing footprint file
        if (missingFootprintsRaw.empty == True):
            logger.info('We have all the footprints that are required. That is good.')
            return(missingFootprintsRaw,None)
        else:
            #missingFootprintsRaw.to_csv(sOutpPrfx+"missing-footprint-files-raw.csv")
            availColNames = list(missingFootprintsRaw.columns.values)
            if not (any('code' in entry for entry in availColNames)):    #  
                missingFootprintsRaw.rename(columns={'site':'code'}, inplace=True)
            missingFootprintsTmp=missingFootprintsRaw[[ 'time','lat', 'lon', 'alt', 'height', 'code', 'footprint']].copy()
            myMissingFootprints=missingFootprintsTmp.copy() # for all these entries we need to create footprints
            
            # What follows is a brief log file so we see quickly which sites and months are affected
            missingFootprintsTmp.rename(columns={'footprint':'missingFootprintFilename'}, inplace=True)
            #missingFootprintsTmp.to_csv(sOutpPrfx+"missing-footprint-files-tmp.csv")
            missingFootprintsTmp.drop_duplicates(subset=['missingFootprintFilename'], keep='last',inplace=True, ignore_index=True) 
            fObsWithoutFootprints=sOutpPrfx+"obs-without-footprints.csv"
            try:
                myMissingFootprints.to_csv(fObsWithoutFootprints)
                missingFootprintsTmp.to_csv(sOutpPrfx+"missing-footprint-files-tmp.csv")
            except:
                pass
        missingFootprints=DataFrame(np.repeat(missingFootprintsTmp.values, 2, axis=0))
        missingFootprints.rename(columns={0:'time',1:'lat', 2:'lon', 3:'alt', 4:'height', 5:'code',6:'missingFootprintFilename' }, inplace=True)
        #print(missingFootprints)
        missingFootprints['dtTime']= to_datetime(missingFootprints['time'])
        missingFootprints['month_start_date'] = to_datetime(missingFootprints['time']).apply(lambda x: x.replace(day=1, hour=0 ))
        missingFootprints['month_end_date']= to_datetime(missingFootprints['time'])
        missingFootprints['month_end_date']=missingFootprints['month_end_date']+  tseries.offsets.MonthEnd(0)
        missingFootprints['month_end_date'] = missingFootprints['month_end_date'].apply(lambda x: x.replace(hour=23 ))
        missingFootprints['dtTime'].apply(lambda d: Period(d, freq='M').end_time.date())

        i=0
        imax=len(missingFootprints)
        while(i<imax):
            missingFootprints.iloc[i, 7]=missingFootprints.iloc[i, 8]
            i+=1
            missingFootprints.iloc[i, 7]=missingFootprints.iloc[i, 9]
            i+=1
        missingFootprints['time'] = missingFootprints['dtTime']
        missingFootprints.drop(columns='dtTime', inplace=True)
        # create a missing-footprint-files.csv file that can be handed to runflex to create these missing footprints
        missingFootprints.to_csv(sOutpPrfx+"missing-footprint-files.csv")
        return(myMissingFootprints,  fObsWithoutFootprints)

    
    def find_footprint_files(self, archive: str, local: str=None, sOutpPrfx='./') -> None:
        # 2) Append file names, both for local and archive
        if local is None:
            local = archive
        fnames_archive = archive + '/' + self.footprint
        fnames_local = local + '/' + self.footprint
        #logger.info(f"Hunting for these footprints,  either archived: \n{fnames_archive} \n or local:\n{fnames_local}")
        # 3) retrieve the files from archive if needed:
        exists = array([check_migrate(arc, loc) for (arc, loc) in tqdm(zip(fnames_archive, fnames_local), desc='Migrate footprint files', total=len(fnames_local), leave=False)])
        self.loc[:, 'footprint'] = fnames_local
        self.loc[:, 'ftprintExists'] = exists

        if not exists.any():
            logger.error("No valid footprints found. Exiting ...")
            raise RuntimeError("No valid footprints found")

        missingFootprintsRaw= self[self['ftprintExists'] == False]
        (missingFootprints,  fObsWithoutFootprints)=self.identifyMissingFootprints(missingFootprintsRaw,sOutpPrfx )
        if(missingFootprints.empty==True):
            logger.info('We have all the footprints we need. That is good')
        else:
            logger.error(f'We lack at least some footprints that are needed. Please call runflex with the option --obs={fObsWithoutFootprints}')
            
        self.drop(columns='ftprintExists', inplace=True)
        self.loc[~exists, 'footprint'] = nan
        return
        
    def gen_obsid(self, sOutpPrfx) -> None:
        # Construct the obs ids:
        # TODO: obsids are no longer unique if footprints from multiple(!) heights are available for the same site.
        # assuming all traditional obsids are <=999,999.0, we could make them unique by replacing that 
        # index with newObsidx=obsidx+int(1e7*height); e.g. 50m: 12228 => 50012228
        # self.to_csv("self.csv")
        valid=self.loc[~isnull(self.footprint)]
        # valid.to_csv("valid.csv")
        # h=valid['height'].astype(float)  # the height column might be in Text format, which causes havoc....
        # obsids = valid.code + h.map('.{:.0f}m.'.format) + valid.time.dt.strftime('%Y%m%d-%H%M%S')
        obsids = valid.code + valid.height.map('.{:.0f}m.'.format) + valid.time.dt.strftime('%Y%m%d-%H%M%S')
        #obsids=obsids.drop_duplicates()
        # logger.info(f"Dbg: obsids=\n{obsids}")
        try:
            if(sOutpPrfx is None):
                sOutpPrfx='./output'
            obsids.to_csv(sOutpPrfx+'obsids.csv', sep=',', mode='w')  # , encoding='utf-8'
        except:
            logger.error('obsids.to_csv(sOutpPrfx+obsids.csv) failed.')
        self.loc[~isnull(self.footprint), 'obsid'] = obsids


    def check_footprint_files(self, cls: Type[FootprintFile],sOutpPrfx ) -> None:
        # Check in the files which footprints are actually present:
        fnames = self.footprint.drop_duplicates().dropna()
        footprints = []

        #print('checking for footprint files')         
        for fname in fnames:
            #print(f'{fname}')
            with cls(fname) as fpf:
                footprints.extend(fpf.footprints)
        #with open("requested-footprint-files.txt", "w") as output:
        #    for fname in fnames:
        #        output.write(fname+'\n')
        self.loc[~self.obsid.isin(footprints), 'footprint'] = nan


    def check_footprints(self, archive: str, cls: Type[FootprintFile], local: str=None,  sOutpPrfx='') -> None:
        """
        Search for/lumia/transport/multitracer.py the footprint corresponding to the observations
        """
        # 1) Create the footprint file names
        self.gen_filenames()
        self.find_footprint_files(archive, local,sOutpPrfx)
        self.gen_obsid(sOutpPrfx)
        self.check_footprint_files(cls,sOutpPrfx )
        # self.to_csv("self2.csv")
