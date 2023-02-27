# -*- coding: utf-8 -*- 
# Author: Higor S. Monteiro

'''
    Create the general class able to handle general tasks on GAL database. 

    Objective: 
        Define transformation tasks to this general class.

    Specific DEV tasks: 
        1 - Robust writing methods to stable formats. Writing methods should be 
            available during different stages of processing.
        2 - Decide which processing tasks should be either in the parent or child 
            classes.   
'''

import numpy as np
import pandas as pd
import lib.utils as utils

class ProcessGAL:
    def __init__(self, DataGal) -> None:
        '''
        
        '''
        self.data_object = DataGal

        # -- Check validation and ID of the data object parsed
        if not self.data_object.has_id or not self.data_object.validated:
            raise ValueError("Data object parsed has no unique ID associated or it is not validated.")

        # -- To be immutable
        self._raw_data = self.data_object._raw_data
        # -- Data for Linkage-Deduplication (Starts with only the ID)
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
        
        '''
        self._data["nome"] = self._raw_data["PACIENTE"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan)
        self._data["sexo"] = self._raw_data["SEXO"].fillna("I").map({"MASCULINO": "M", "FEMININO": "F", "IGNORADO": "I"})
        self._data["cns"] = self._raw_data["CNS DO PACIENTE"].copy()

        self._data["nome_mae"] = self._raw_data["NOME DA MÃE"].apply(lambda x: utils.replace_string(x, sep=" ") if pd.notna(x) else np.nan)
        self._data["nome"] = self._data["nome"].apply(lambda x: utils.replace_string(x, sep=" ") if pd.notna(x) else np.nan)

        self._data["nascimento_dia"] = self._raw_data["DATA DE NASCIMENTO"].apply(lambda x: x.day if pd.notna(x) else np.nan)
        self._data["nascimento_mes"] = self._raw_data["DATA DE NASCIMENTO"].apply(lambda x: x.month if pd.notna(x) else np.nan)
        self._data["nascimento_ano"] = self._raw_data["DATA DE NASCIMENTO"].apply(lambda x: x.year if pd.notna(x) else np.nan)

        self._data["primeiro_nome"] = self._data["nome"].apply(lambda x: x.split(" ")[0] if pd.notna(x) else np.nan )
        self._data["segundo_nome"] = self._data["nome"].apply(lambda x: x.split(" ")[1] if pd.notna(x) and len(x.split(" "))>1 else np.nan )
        self._data["complemento_nome"] = self._data["nome"].apply(lambda x: ' '.join(x.split(" ")[2:]) if pd.notna(x) and len(x.split(" "))>2 else np.nan )

        self._data["primeiro_nome_mae"] = self._data["nome_mae"].apply(lambda x: x.split(" ")[0] if pd.notna(x) else np.nan )
        self._data["segundo_nome_mae"] = self._data["nome_mae"].apply(lambda x: x.split(" ")[1] if pd.notna(x) and len(x.split(" "))>1 else np.nan )
        self._data["complemento_nome_mae"] = self._data["nome_mae"].apply(lambda x: ' '.join(x.split(" ")[2:]) if pd.notna(x) and len(x.split(" "))>2 else np.nan )

        self._data["FONETICA_N"] = self._data["nome"].apply(lambda x: f"{x.split(' ')[0]}{x.split(' ')[-1]}" if pd.notna(x) else np.nan)