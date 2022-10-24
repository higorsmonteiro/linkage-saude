'''
    Base class for file reading  
'''
import os
import pandas as pd
from simpledbf import Dbf5

class ReadBase:
    def __init__(self, path_data, filename) -> None:
        '''
        
        '''
        # Identifica extensão do arquivo -> self.ext é definido pela classe herdeira.
        fname_dummy, file_extension = os.path.splitext(filename)
        if file_extension not in self.extension:
            raise TypeError # modify later 

        self.path_data = path_data
        self.filename = filename

class ReadExcel(ReadBase):
    '''
    
    '''
    extension = [".xlsx", ".xls"]
    
    def read_file(self, **kwargs):
        return pd.read_excel(os.path.join(self.path_data, self.filename), **kwargs)

class ReadDBF(ReadBase):
    '''
    
    '''
    extension = [".dbf", ".DBF"]

    def read_file(self, **kwargs):
        return Dbf5(os.path.join(self.path_data, self.filename), **kwargs).to_dataframe()

class ReadCSV(ReadBase):
    extension = [".csv", ".CSV"]

    def read_file(self, **kwargs):
        return pd.read_csv(os.path.join(self.path_data, self.filename), **kwargs)