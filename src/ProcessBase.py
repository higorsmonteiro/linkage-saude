'''

'''
from src.ReadBase import *

class ProcessBase:
    def __init__(self, path_data, filename) -> None:
        '''
        
        '''
        self.path_data = path_data
        self.filename = filename
        self.data = None
        self.reader = None

        ''' Cria objeto de leitura independente da extensão dada (exceto se 
            a extensão não é conhecida '''
        try:
            self.reader = ReadExcel(self.path_data, self.filename)
        except TypeError:
            self.reader = ReadDBF(self.path_data, self.filename)
        except TypeError:
            self.reader = ReadCSV(self.path_data, self.filename)
        except TypeError:
            pass

    # --> To test if **kwargs works (it does!)
    def read_file(self, **kwargs):
        '''
            Arguments parsed are the same of the base function used 
            depending on the file extension:
                - Excel: pandas.read_excel
                - DBF: simpledbf.Dbf5
                - CSV: pandas.read_csv
        '''
        self.data = self.reader.read_file(**kwargs)
