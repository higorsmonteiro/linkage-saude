import os
import pandas as pd

def identifiers_columns():
    '''
    
    '''
    id_cols = {
        "SINAN": ["NU_NOTIFIC", "ID_AGRAVO", "ID_MUNICIP", "DT_NOTIFIC"],
        "SIVEP": ["NU_NOTIFIC", "DT_NOTIFIC", "DT_INTERNA", "HOSPITAL"],
        "SIM": ["NUMERODO"],
        "SINASC": ["NUMERODN"]
    }
    return id_cols

def identify_source(data):
    '''
        Identify the source of the data parsed.

        Options included so far:
            - SINAN (Sistema de Informação de Notificação de Agravos N)
            - SIVEP - Not included yet
            - SIM - Not included yet
            - SINASC - Not included yet

        Args:
            data:
                pandas.DataFrame.
    '''
    columns = [ col.upper() for col in data.columns ]
    id_cols = identifiers_columns()

    # --> Columns to identify the database
    sinan_cols = id_cols["SINAN"]
    sivep_cols = id_cols["SIVEP"]
    sim_cols = id_cols["SIM"]
    sinasc_cols = id_cols["SINASC"]

    # --> Verification
    if set(sinan_cols).issubset(set(columns)):
        return "SINAN"
    elif set(sivep_cols).issubset(set(columns)):
        return "SIVEP"
    elif set(sim_cols).issubset(set(columns)):
        return "SIM"
    elif set(sinasc_cols).issubset(set(columns)):
        return "SINASC"
    else:
        return ""
    

