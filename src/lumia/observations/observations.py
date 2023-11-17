#!/usr/bin/env python
import shutil
from dataclasses import dataclass, field
from pandas import DataFrame, Timestamp, read_csv, read_hdf
from pathlib import Path
from omegaconf import DictConfig, OmegaConf
from typing import Dict, List
import tarfile
from loguru import logger
import tempfile
import os
from numpy.typing import NDArray
from datetime import datetime
from lumia.utils import debug


@dataclass
class Settings :
    err_min : float = 0
    err_freq : str = '7D'
    err_fac : float = 1
    field_err_obs : str = 'err_obs'

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if k in self.__dataclass_fields__:
                setattr(self, k, v)


@dataclass
class Observations:
    sites : DataFrame = None
    observations : DataFrame = None
    start : Timestamp = None
    end : Timestamp = None
    settings : Settings = field(default_factory=Settings)
    
    def __post_init__(self):
        if self.sites is None :
            self.sites = DataFrame(columns=['code', 'name', 'lat', 'lon', 'alt', 'height'])
        if self.observations is None:
            self.observations = DataFrame(columns=['time', 'site'])
        self.observations.loc[:, 'time'] = self.observations.time.astype('datetime64[ns]')
        self.start = Timestamp(self.start) if self.start is not None else None
        self.end = Timestamp(self.end) if self.end is not None else None

    def to_hdf(self, filename: Path | str) -> Path :
        logger.info(f"Writing observation database to {filename}")
        df = self.to_dataframe()
        # TODO: ad-hoc fix to convert "object" bool to standard bool. Need to make sure these don't be created in the 1st place
        for col in df.columns:
            if df.loc[:, col].dtype == 'O' and isinstance(df.loc[:, col].iloc[0], bool):
                logger.warning(f"Converting column {col} from {df.loc[:, col].dtype} to {bool}")
                df.loc[:, col] = df.loc[:, col].astype(bool)
        df.to_hdf(filename, key='observations')
        return Path(filename)

    def save_tar(self, filename: Path | str) -> Path :
        filename = Path(filename)

        logger.info(f"Writing observation database to {filename}")

        # Create a unique temporary directory, save the current directory
        filename.parent.mkdir(exist_ok=True, parents=True)
        tmpdir = Path(tempfile.mkdtemp(dir=filename.parent))
        curdir = os.getcwd()
        os.chdir(tmpdir)

        # Create a tar file (and the intermediate files that go in the tar) in that temporary directory
        with tarfile.open(filename.name, 'w:gz') as tar:
            self.observations.to_csv('observations.csv', date_format='%Y%m%d%H%M%S', encoding='utf8')
            self.sites.to_csv('sites.csv', encoding='utf-8')
            tar.add('observations.csv')
            tar.add('sites.csv')

        # Move back to the original directory, and move the tarfile in it
        os.chdir(curdir)
        os.rename(tmpdir / filename.name, filename)

        # Delete the temporary directory
        shutil.rmtree(tmpdir)
        return Path(filename)

    def to_dataframe(self) -> DataFrame:
        """
        Combine the "sites" and "observations" dataframes in a single dataframe
        The columns of the "sites" dataframe are added to the "observations" dataframe if they are not already present
        """
        obs = self.observations.copy()
        for site in self.sites.itertuples():
            for field in set(self.sites.columns) - set(self.observations.columns):
                try:
                    obs.loc[self.observations.site == site.Index, field] = getattr(site, field)
                except AttributeError as e:
                    logger.error(f"Column {field} not found in sites DataFrame")
                    import pdb; pdb.set_trace()
        return obs

    @classmethod
    def from_hdf(cls, filename: Path | str) -> "Observations":
        df = read_hdf(filename, key='observations')
        return cls.from_dataframe(df)

    @classmethod
    def from_dconf(cls, dconf: Dict | DictConfig) -> "Observations":
        ...

    @classmethod
    def from_tar(cls, filename: Path | str) -> "Observations":
        with tarfile.open(filename, 'r:gz') as tar:
            observations = read_csv(tar.extractfile('observations.csv'), index_col=0, parse_dates=['time'], date_format='%Y%m%d%H%M%S')
            sites = read_csv(tar.extractfile('sites.csv'), index_col=0)
        return cls(sites, observations)

    @classmethod
    def from_dataframe(cls, df: DataFrame) -> "Observations":
        obs = cls()
        for site in df.site.drop_duplicates():
            dfs = df.loc[df.site == site]
            site = {}
            for col in dfs.columns:
                values = dfs.loc[:, col].drop_duplicates().values
                if len(values) == 1:
                    site[col] = values[0]
            obs.sites.loc[site['site']] = site

        # Remove columns that have been transferred to "sites", except for the "site" column, which is used for establishing correspondance
        obs.observations = df.loc[:, ['site'] + list(set(df.columns) - set(obs.sites.columns))]

        return obs

    @property
    def mismatch(self) -> NDArray:
        return self.observations.dropna(subset=['mismatch', 'sigma']).mismatch.values

    @property
    def sigma(self) -> NDArray:
        return self.observations.dropna(subset=['mismatch', 'sigma']).sigma.values

    def select_times(self, tmin : Timestamp | datetime | str = None, tmax : datetime | Timestamp | str = None, inplace : bool = True) -> "Observations":
        tmin = self.start if tmin is None else tmin
        tmax = self.end if tmax is None else tmax
        tmin = self.observations.time.min() if tmin is None else tmin
        tmax = self.observations.time.max() if tmax is None else tmax
        observations = self.observations.loc[(
                (self.observations.time >= tmin) &
                (self.observations.time <= tmax)
        )]
        sites = self.sites.loc[self.observations.site.drop_duplicates(), :]
        if not inplace:
            return Observations(
                sites=sites,
                observations=observations,
                start=tmin,
                end=tmax
            )
        else:
            self.observations = observations
            self.sites = sites
            return self

    def select_sites(self, sites : List[str], inplace : bool = True) -> "Observations":
        observations = self.observations.loc[self.observations.site.isin(sites)]
        sites_table = self.sites.loc[sites]
        if not inplace :
            return Observations(sites=sites_table, observations=observations, start=self.start, end=self.end)
        else :
            self.observations = observations
            self.sites = sites_table
            return self
        
    @debug.trace_args()
    def calc_uncertainties(self, step: str = None):
        
        # Ensure that all observations have measurement error:
        sel = self.observations.loc[:, self.settings.field_err_obs] <= 0
        self.observations.loc[sel, self.settings.field_err_obs] = self.settings.err_min

        for code in self.observations.code.drop_duplicates():
            
            # 1) Select the data:
            mix = self.observations.loc[self.observations.code == code].loc[:, ['time', 'obs', f'mix_{step}', self.settings.field_err_obs]].set_index('time').sort_index()
            
             # 2) Calculate weekly moving average and residuals from it
            #trend = mix.rolling(freq).mean()
            #resid = mix - trend
            
            # Use a weighted rolling average, to avoid giving too much weight to the uncertain obs:
            weights = 1. / mix.loc[:, self.settings.field_err_obs] ** 2
            total_weight = weights.rolling(self.settings.err_freq).sum()   # sum of weights in a week (for normalization)
            obs_weighted = mix.obs * weights
            mod_weighted = mix.loc[:, f'mix_{step}'] * weights
            obs_averaged = obs_weighted.rolling(self.settings.err_freq).sum() / total_weight
            mod_averaged = mod_weighted.rolling(self.settings.err_freq).sum() / total_weight
            resid_obs = mix.obs - obs_averaged
            resid_mod = mix.loc[:, f'mix_{step}'] - mod_averaged
            
            # 3) Calculate the standard deviation of the residuals model-data mismatches. Store it in sites dataframe for info.
            sigma = (resid_obs - resid_mod).dropna().values.std()
            self.sites.loc[self.sites.code == code, 'err'] = sigma
            logger.info(f'Model uncertainty for site {code} set to {sigma:.2f}')
            
            # 4) Get the measurement uncertainties and calculate the error inflation
#            s_obs = self.observations.loc[:, self.settings.field_err_obs].values
#            nobs = len(s_obs)
#            s_mod = sqrt((nobs * sigma**2 - (s_obs**2).sum()) / nobs)

            # 5) Store the inflated errors:
            self.observations.loc[self.observations.code == code, 'err'] = (
                self.observations.loc[self.observations.code == code, self.settings.field_err_obs] ** 2 + sigma ** 2).values ** .5
            self.observations.loc[self.observations.code == code, 'resid_obs'] = resid_obs.values
            self.observations.loc[self.observations.code == code, 'resid_mod'] = resid_mod.values
            self.observations.loc[self.observations.code == code, 'obs_detrended'] = obs_averaged.values
            self.observations.loc[self.observations.code == code, 'mod_detrended'] = mod_averaged.values
        
        # Apply global scaling factor if needed:
        self.observations.loc[:, 'err'] *= self.settings.err_fac