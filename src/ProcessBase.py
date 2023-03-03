# -*- coding: utf-8 -*- 
# Author: Higor S. Monteiro

'''
    Create the general class able to handle universal processing tasks. 

    Objective: 
        Define I/O tasks to this general class. Thereafter, we should define 
        specific children classes for any disease of interest, such as: DENGUE,
        CHIKUNGUNYA, SARAMPO, etc. Children classes should be created and updated
        as long as demands are generated (or outbreaks of new diseases occur). 

    Specific DEV tasks:
        1 - Robust reading methods for the common supported extensions. DBFs files
            are harder to deal with in some circumstances (e. g. Congenital Syphilis). 
        2 - Robust writing methods to stable formats. Writing methods should be 
            available during different stages of processing.
        3 - Decide which processing tasks should be either in the parent or child classes.   
'''

import numpy as np
import pandas as pd
import lib.utils as utils
from src.CustomExceptions import *

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