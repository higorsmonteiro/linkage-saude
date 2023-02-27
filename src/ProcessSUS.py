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
from src.ProcessBase import ProcessBase

'''
    -------> SINAN
'''
class ProcessSinan(ProcessBase):
    db_type = "SINAN"
    
    def process(self):
        '''
        
        '''
        self._data["nome"] = self._raw_data["NM_PACIENT"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan)
        self._data["nome_mae"] = self._raw_data["NM_MAE_PAC"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan)
        
        self._data["sexo"] = self._raw_data["CS_SEXO"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan)
        self._data["dt_notific"] = pd.to_datetime(self._raw_data["DT_NOTIFIC"], format="%Y-%m-%d", errors="coerce")
        self._data["dt_nasc"] = self._raw_data["DT_NASC"].apply(lambda x: pd.to_datetime(x, format="%Y-%m-%d", errors="coerce"))
        self._data["uni_notific"] = self._raw_data["ID_UNIDADE"].apply(lambda x: x if pd.notna(x) else np.nan)

        self._data["nascimento"] = self._data["dt_nasc"].apply(lambda x: f"{x.day:2.0f}/{x.month:2.0f}/{x.year:2.0f}".replace(" ", "0") if pd.notna(x) else np.nan)
        self._data["notificacao"] = self._data["dt_notific"].apply(lambda x: f"{x.day:2.0f}/{x.month:2.0f}/{x.year:2.0f}".replace(" ", "0") if pd.notna(x) else np.nan)
        
        self._data["nascimento_dia"] = self._data["dt_nasc"].apply(lambda x: x.day if pd.notna(x) else np.nan)
        self._data["nascimento_mes"] = self._data["dt_nasc"].apply(lambda x: x.month if pd.notna(x) else np.nan)
        self._data["nascimento_ano"] = self._data["dt_nasc"].apply(lambda x: x.year if pd.notna(x) else np.nan)
        self._data["notific_dia"] = self._data["dt_notific"].apply(lambda x: x.day if pd.notna(x) else np.nan)
        self._data["notific_mes"] = self._data["dt_notific"].apply(lambda x: x.month if pd.notna(x) else np.nan)
        self._data["notific_ano"] = self._data["dt_notific"].apply(lambda x: x.year if pd.notna(x) else np.nan)
        self._data["cns"] = self._raw_data["ID_CNS_SUS"].apply(lambda x: x if pd.notna(x) else np.nan)
        self._data["cep"] = self._raw_data["NU_CEP"].apply(lambda x: x if pd.notna(x) else np.nan)

        self._data["primeiro_nome_mae"] = self._data["nome_mae"].apply(lambda x: x.split(" ")[0] if pd.notna(x) else np.nan )
        self._data["segundo_nome_mae"] = self._data["nome_mae"].apply(lambda x: x.split(" ")[1] if pd.notna(x) and len(x.split(" "))>1 else np.nan )
        self._data["complemento_nome_mae"] = self._data["nome_mae"].apply(lambda x: ' '.join(x.split(" ")[2:]) if pd.notna(x) and len(x.split(" "))>2 else np.nan )

        self._data["primeiro_nome"] = self._data["nome"].apply(lambda x: x.split(" ")[0] if pd.notna(x) else np.nan )
        self._data["segundo_nome"] = self._data["nome"].apply(lambda x: x.split(" ")[1] if pd.notna(x) and len(x.split(" "))>1 else np.nan )
        self._data["complemento_nome"] = self._data["nome"].apply(lambda x: ' '.join(x.split(" ")[2:]) if pd.notna(x) and len(x.split(" "))>2 else np.nan )
 
        # --> Consolidate BAIRROS
        self._data["bairro"] = self._raw_data["NM_BAIRRO"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan)
        self._data["bairro"] = self._data["bairro"].apply(lambda x: utils.replace_string(x, sep=" ") if pd.notna(x) else np.nan)

        self._data = self._data.drop(["dt_nasc", "dt_notific"], axis=1)
        self._data["evolucao"] = self._raw_data["EVOLUCAO"].copy()
        self._data["classi_fin"] = self._raw_data["CLASSI_FIN"].copy()
        ## --> FONETICA for blocking
        self._data["FONETICA_N"] = self._data["nome"].apply(lambda x: f"{x.split(' ')[0]}{x.split(' ')[-1]}" if pd.notna(x) else np.nan)


'''
    ------> GAL
'''
class ProcessGAL(ProcessBase):
    db_type = "GAL"

    def process(self):
        '''
        
        '''
        self._data["nome"] = self._raw_data["PACIENTE"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan)
        self._data["nome_mae"] = self._raw_data["NOME DA MÃE"].apply(lambda x: utils.replace_string(x, sep=" ") if pd.notna(x) else np.nan)
        self._data["sexo"] = self._raw_data["SEXO"].fillna("I").map({"MASCULINO": "M", "FEMININO": "F", "IGNORADO": "I"})
        self._data["cns"] = self._raw_data["CNS DO PACIENTE"].copy()
        self._data["nascimento"] = self._raw_data["DATA DE NASCIMENTO"].apply(lambda x: f"{x.day:2.0f}/{x.month:2.0f}/{x.year:2.0f}".replace(" ", "0") if pd.notna(x) else np.nan)
        self._data["solicitacao"] = self._raw_data["DATA DA SOLICITAÇÃO"].apply(lambda x: f"{x.day:2.0f}/{x.month:2.0f}/{x.year:2.0f}".replace(" ", "0") if pd.notna(x) else np.nan)
        
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

        self._data["bairro"] = self._raw_data["BAIRRO"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan)

        self._data["FONETICA_N"] = self._data["nome"].apply(lambda x: f"{x.split(' ')[0]}{x.split(' ')[-1]}" if pd.notna(x) else np.nan)