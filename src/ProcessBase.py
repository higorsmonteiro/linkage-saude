'''
    NOTE: Maybe 'ProcessBase' does not need children for specific behavior of the type of database.
'''
from src.ReadBase import *
from src.DataBase import *
from src.CustomExceptions import *

import lib.database_utils as utils

class ProcessBase:
    def __init__(self, path_data, filename) -> None:
        '''
        
        '''
        self.path_data = path_data
        self.filename = filename
        self.data_object = None
        self._raw_data = None
        self.data = None
        self.reader = None

        # Create reader object independent of the file extension (unless the extension is not recognized)
        try:
            self.reader = ReadExcel(self.path_data, self.filename)
        except FileExtensionError:
            try:
                self.reader = ReadDBF(self.path_data, self.filename)
            except FileExtensionError:
                self.reader = ReadCSV(self.path_data, self.filename)

    @property
    def raw_data(self):
        """Getter of raw_data"""
        if self.data_object is not None:
            self._raw_data = self.data_object.raw_data
        return self._raw_data

    @raw_data.setter
    def raw_data(self, value):
        raise AttributeError("No possible to change this attribute.")

    @property
    def fmt_data(self):
        """Getter of formatted data"""
        if self.data_object is None:
            return self.data
        return self.data_object.data

    # --> To test if **kwargs works (it does!)
    def read_file(self, **kwargs):
        '''
            Arguments parsed are the same of the base function used 
            depending on the file extension:
                - Excel: pandas.read_excel
                - DBF: simpledbf.Dbf5
                - CSV: pandas.read_csv
        '''
        self._raw_data = self.reader.read_file(**kwargs)

    def initilize(self, DBtype=DataExtra):
        if self._raw_data is None:
            raise NoDataLoaded("There is no data found within the class.")
        # 'DataExtra' has no specific behaviour, but we can create on the fly for any new behavior
        self.data_object = DBtype(self._raw_data)


# ----------- SPECIAL INITIALIZATION: DATA SOURCE DEPENDENT -----------
class ProcessSinan(ProcessBase):
    def initilize(self):
        if self.raw_data is None:
            raise NoDataLoaded("There is no data found within the class.")
        self.data_object = DataSinan(self.raw_data)

class ProcessSivep(ProcessBase):
    def initilize(self):
        if self.raw_data is None:
            raise NoDataLoaded("There is no data found within the class.")
        self.data_object = DataSivep(self.raw_data)

class ProcessSim(ProcessBase):
    def initilize(self):
        if self.raw_data is None:
            raise NoDataLoaded("There is no data found within the class.")
        self.data_object = DataSim(self.raw_data)

class ProcessSinasc(ProcessBase):
    def initilize(self):
        if self.raw_data is None:
            raise NoDataLoaded("There is no data found within the class.")
        self.data_object = DataSinasc(self.raw_data)

# Every time we deal with a new data source, we create a new class for initialization
class ProcessExtra(ProcessBase):
    pass


