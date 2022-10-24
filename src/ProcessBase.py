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

        ''' Cria objeto de leitura independente do extensão dada (exceto se 
            a extensão não é conhecida '''
        try:
            self.reader = ReadExcel(self.path_data, self.filename)
        except Exception:
            self.reader = ReadDBF(self.path_data, self.filename)
        except Exception:
            pass

    # --> To test if **kwargs works
    def read_file(self, **kwargs):
        '''
        
        '''
        self.data = self.reader.read_file(**kwargs)
