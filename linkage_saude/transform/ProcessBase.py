# -*- coding: utf-8 -*- 

'''
    Create the general class able to handle universal processing tasks. 
'''

from linkage_saude.exceptions import *

class ProcessBase:
    def __init__(self, DataObject) -> None:
        '''
        
        '''
        self.data_object = DataObject

        # -- Check validation and ID of the data object parsed.
        if not self.data_object.has_id or not self.data_object.validated:
            raise UniqueIdentifierMissing("No Unique Identifier and/or Data not validated.")

        self._raw_data = self.data_object._raw_data
        self._data = self.data_object._data

    @property
    def raw_data(self):
        return self._raw_data

    @raw_data.setter
    def raw_data(self, x):
        raise AttributeError("Not possible to change this attribute.")

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, x):
        raise AttributeError("Not possible to change this attribute.")

    def process(self):
        '''
            Method to handle transformation steps specific to the database.
        '''
        pass