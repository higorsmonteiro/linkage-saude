'''
    NOTE: Maybe 'ProcessBase' does not need children for specific behavior of the type of database.
'''
from src.ReadBase import *
from src.DataBase import *
from src.CustomExceptions import *

import numpy as np
from pandera import Column, DataFrameSchema
import lib.database_utils as utils

class ProcessBase:
    def __init__(self, data) -> None:
        '''
        
        '''
        self._raw_data = data.copy()
        self.data = None
        self.data_object = None

    @property
    def raw_data(self):
        '''
            Getter of raw_data
        '''
        if self.data_object is not None:
            self._raw_data = self.data_object.raw_data
        return self._raw_data

    @raw_data.setter
    def raw_data(self, value):
        '''
            Setter of raw_data (not mutable from outside class)
        '''
        raise AttributeError("No possible to change this attribute.")

    @property
    def fmt_data(self):
        '''
            Getter of formatted data
        '''
        if self.data_object is None:
            return self.data
        return self.data_object.data

    # --> To test if **kwargs works (it does!)
    #def read_file(self, **kwargs):
    #    '''
    #        Arguments parsed are the same of the base function used 
    #        depending on the file extension:
    #            - Excel: pandas.read_excel
    #            - DBF: simpledbf.Dbf5
    #            - CSV: pandas.read_csv
    #    '''
    #    self._raw_data = self.reader.read_file(**kwargs)

    def export(self, **kwargs):
        '''
        
        '''
        pass

    def initilize(self, DBTYPE=DataExtra):
        # 'DataExtra' has no specific behaviour, but we can create on the fly for any new behavior
        self.data_object = DBTYPE(self._raw_data)


# ----------- SPECIAL INITIALIZATION: DATA SOURCE DEPENDENT -----------
class ProcessSinan(ProcessBase):
    def initilize(self):
        self.data_object = DataSinan(self._raw_data)

    def data_validation(self):
        '''
            All validations regarding SINAN.
        '''
        if self._raw_data is None:
            raise NoDataLoaded("There is no data found within the class.")

        schema = DataFrameSchema(
            {
                "NU_NOTIFIC": Column(str, required=True),
                "ID_AGRAVO": Column(str, required=True),
                "ID_MUNICIP": Column(str, required=True),
                "DT_NOTIFIC": Column(str, required=True),
            },
            strict=False, coerce=False
        )

        # --> VALIDATIONS
        schema.validate(self._raw_data)
        

# ----------------------------------------------------------------------
class ProcessSivep(ProcessBase):
    def initilize(self):
        '''
        
        '''
        self.data_object = DataSivep(self._raw_data)

class ProcessSim(ProcessBase):
    def initilize(self):
        '''
        
        '''
        self.data_object = DataSim(self._raw_data)

class ProcessSinasc(ProcessBase):
    def initilize(self):
        '''
        
        '''
        self.data_object = DataSinasc(self._raw_data)

# --> Every time we deal with a new data source, we create a new class for initialization
class ProcessExtra(ProcessBase):
    pass


