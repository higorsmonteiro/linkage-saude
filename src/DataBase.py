'''
    Base class to define the specific of each database.
'''
import os
import pandas as pd
import numpy as np
from src.CustomExceptions import *

import lib.database_utils as db_utils


class DataBase:
    def __init__(self, data) -> None:
        '''
        
        '''
        # --> Simple validation on the type of database
        self.data_source = db_utils.identify_source(data)
        if self.data_source!=self.db_type:
            raise DataSourceNotMatched(f"Data source defined used does not match with the source of the data parsed: {self.data_source} -> {self.db_type}")
        
        self.raw_data = data.copy()
        self.data = None

class DataSinan(DataBase):
    db_type = "SINAN"

    def create_id(self):
        '''
        
        '''
        # --> Create ID GEO
        self.raw_data["NU_NOTIFIC"] = self.raw_data["NU_NOTIFIC"].astype(int).apply(lambda x: f"{x:7.0f}".replace(" ", "0"))
        self.raw_data["DT_NOTIFIC_FMT"] = self.raw_data["DT_NOTIFIC"].apply(lambda x: f"{x.day:2.0f}{x.month:2.0f}{x.year}".replace(" ", "0"))
        self.raw_data["ID_MUNICIP"] = self.raw_data["ID_MUNICIP"].apply(lambda x: f"{x}")
        self.raw_data["ID_GEO"] = self.raw_data["ID_AGRAVO"]+self.raw_data["NU_NOTIFIC"]+self.raw_data["ID_MUNICIP"]+self.raw_data["DT_NOTIFIC_FMT"]
        self.data = pd.DataFrame(self.raw_data)
        self.raw_data = self.raw_data.drop("DT_NOTIFIC_FMT", axis=1)

class DataSivep(DataBase):
    db_type = "SIVEP"

class DataSim(DataBase):
    db_type = "SIM"

class DataSinasc(DataBase):
    db_type = "SINASC"

class DataExtra(DataBase):
    db_type = ""
