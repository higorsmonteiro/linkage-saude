# -*- coding: utf-8 -*- 
'''
    For each specific SUS database, define a method to standardize the data for deduplication and linkage tasks.

    Done for now:
        - SINAN;
        - SIVEP-Gripe;
'''

import re
import numpy as np
import pandas as pd

from linkage_saude.exceptions import *
import linkage_saude.utils.general as general_utils
from linkage_saude.transform.ProcessBase import ProcessBase

class ProcessSinan_v2(ProcessBase):
    '''
    
    '''
    db_type = "SINAN"

    def matching_standard(self, sific=False):
        '''
        
        '''
        self._data["nome"] = self._raw_data["NM_PACIENT"].apply(lambda x: general_utils.uniformize_name(x.upper().strip(), sep=" ") if pd.notna(x) else np.nan).apply(lambda x: re.sub(' {2,}', ' ', x) if pd.notna(x) else np.nan)
        self._data["nome_mae"] = self._raw_data["NM_MAE_PAC"].apply(lambda x: general_utils.uniformize_name(x.upper().strip(), sep=" ") if pd.notna(x) else np.nan).apply(lambda x: re.sub(' {2,}', ' ', x) if pd.notna(x) else np.nan)
        self._data["sexo"] = self._raw_data["CS_SEXO"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan)
        self._data["dt_nasc"] = self._raw_data["DT_NASC"].apply(lambda x: pd.to_datetime(x, format="%d/%m/%Y", errors="coerce") if not hasattr(x, 'year') and pd.notna(x) else x)
        self._data["cns"] = self._raw_data["ID_CNS_SUS"].apply(lambda x: x if isinstance(x, str) and general_utils.cns_is_valid(x) and pd.notna(x) else ( f"{x:13.0f}".replace(" ", "0") if not isinstance(x, str) and pd.notna(x) else np.nan))
        self._data["cep"] = self._raw_data["NU_CEP"].apply(lambda x: x if pd.notna(x) else np.nan)
        self._data["bairro"] = self._raw_data["NM_BAIRRO"].apply(lambda x: general_utils.uniformize_name(x.upper().strip(), sep=" ") if pd.notna(x) else np.nan)
        self._data["cod_unidade"] = self._raw_data["ID_UNIDADE"].apply(lambda x: x if pd.notna(x) else np.nan)

        self._data["FONETICA_N"] = self._data["nome"].apply(lambda x: f"{x.split(' ')[0]}{x.split(' ')[-1]}" if pd.notna(x) else np.nan)
        if sific:
            self._data["FONETICA_N"] = self._data["nome_mae"].apply(lambda x: f"{x.split(' ')[0]}{x.split(' ')[-1]}" if pd.notna(x) else np.nan)
            self._data["classif_nascido"] = self._data[["nome", "nome_mae"]].apply(lambda x: x["nome"].replace(x["nome_mae"], '') if pd.notna(x["nome"]) and pd.notna(x["nome_mae"]) and x["nome_mae"] in x["nome"] else x["nome"], axis=1) 
            self._data["classif_nascido"] = self._data["classif_nascido"].apply(lambda x: x.strip().replace(" DE", ""))

        self._data["nascimento_dia"] = self._data["dt_nasc"].apply(lambda x: x.day if hasattr(x, 'day') and pd.notna(x) else np.nan)
        self._data["nascimento_mes"] = self._data["dt_nasc"].apply(lambda x: x.month if hasattr(x, 'day') and pd.notna(x) else np.nan)
        self._data["nascimento_ano"] = self._data["dt_nasc"].apply(lambda x: x.year if hasattr(x, 'day') and pd.notna(x) else np.nan)

        self._data["primeiro_nome"] = self._data["nome"].apply(lambda x: x.split(" ")[0] if pd.notna(x) else np.nan )
        #self._data["segundo_nome"] = self._data["nome"].apply(lambda x: x.split(" ")[1] if pd.notna(x) and len(x.split(" "))>1 else np.nan )
        self._data["complemento_nome"] = self._data["nome"].apply(lambda x: ' '.join(x.split(" ")[1:]) if pd.notna(x) and len(x.split(" "))>1 else np.nan )
        if sific:
            self._data["primeiro_nome"] = self._data["classif_nascido"].apply(lambda x: x.split(" ")[0] if pd.notna(x) else np.nan )
            self._data["complemento_nome"] = self._data["classif_nascido"].apply(lambda x: ' '.join(x.split(" ")[2:]) if pd.notna(x) and len(x.split(" "))>2 else np.nan )
        
        self._data["primeiro_nome_mae"] = self._data["nome_mae"].apply(lambda x: x.split(" ")[0] if pd.notna(x) else np.nan )
        self._data["segundo_nome_mae"] = self._data["nome_mae"].apply(lambda x: x.split(" ")[1] if pd.notna(x) and len(x.split(" "))>1 else np.nan )
        self._data["complemento_nome_mae"] = self._data["nome_mae"].apply(lambda x: ' '.join(x.split(" ")[2:]) if pd.notna(x) and len(x.split(" "))>2 else np.nan )

    def define_duplicate(self, persons_pairs, delta_notific=None, delta_sintomas=None, same_unit=None):
        '''
            Description.

            Args:
            -----
                persons_pairs:
                    List.
                delta_notific:
                    Integer.
                delta_sintomas:
                    Integer.
                same_unit:
                    Boolean.

            Return:
            -------
                positives:
                    List of 2-tuples.
                negatives:
                    List of 2-tuples.
        '''
        temp_index = self.raw_data.set_index("ID_GEO")

        positives = []
        negatives = []

        for pair in persons_pairs:
            left, right = pair[0], pair[1]
            
            dt_not_left, dt_not_right = temp_index["DT_NOTIFIC"].loc[left], temp_index["DT_NOTIFIC"].loc[right]
            dt_sin_left, dt_sin_right = temp_index["DT_SIN_PRI"].loc[left], temp_index["DT_SIN_PRI"].loc[right]
            id_not_left, id_not_right = temp_index["ID_UNIDADE"].loc[left], temp_index["ID_UNIDADE"].loc[right]

            cur_delta_not = abs((dt_not_left-dt_not_right).days)
            cur_delta_sin = abs((dt_sin_left-dt_sin_right).days)

            if cur_delta_not<=delta_notific and cur_delta_not>=0 and cur_delta_sin<=delta_sintomas:
                if same_unit and id_not_left==id_not_right:
                    positives.append(pair)
                elif same_unit and id_not_left!=id_not_right:
                    negatives.append(pair)
                else:
                    positives.append(pair)
            else:
                negatives.append(pair)
        return positives, negatives


class ProcessSivep_v2(ProcessBase):
    '''
    
    '''
    db_type = "SIVEP-GRIPE"

    def matching_standard(self):
        '''
        
        '''
        self._data["nome"] = self._raw_data["NM_PACIENT"].apply(lambda x: general_utils.uniformize_name(x.upper().strip(), sep=" ") if pd.notna(x) else np.nan).apply(lambda x: re.sub(' {2,}', ' ', x) if pd.notna(x) else np.nan)
        self._data["nome_mae"] = self._raw_data["NM_MAE_PAC"].apply(lambda x: general_utils.uniformize_name(x.upper().strip(), sep=" ") if pd.notna(x) else np.nan).apply(lambda x: re.sub(' {2,}', ' ', x) if pd.notna(x) else np.nan)
        self._data["sexo"] = self._raw_data["CS_SEXO"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan)
        self._data["dt_nasc"] = self._raw_data["DT_NASC"].apply(lambda x: pd.to_datetime(x, format="%d/%m/%Y", errors="coerce") if not hasattr(x, 'year') and pd.notna(x) else x)
        self._data["cpf"] = self._raw_data["NU_CPF"].apply(lambda x: f"{x:11.0f}".replace(" ", "0") if not isinstance(x, str) and pd.notna(x) else x)
        self._data["cns"] = self._raw_data["NU_CNS"].apply(lambda x: x if isinstance(x, str) and general_utils.cns_is_valid(x) and pd.notna(x) else ( f"{x:13.0f}".replace(" ", "0") if not isinstance(x, str) and pd.notna(x) else np.nan))
        self._data["cep"] = self._raw_data["NU_CEP"].apply(lambda x: x if isinstance(x, str) and pd.notna(x) else ( f"{x:8.0f}".replace(" ", "0") if not isinstance(x, str) and pd.notna(x) else np.nan))
        self._data["bairro"] = self._raw_data["NM_BAIRRO"].apply(lambda x: general_utils.uniformize_name(x.upper().strip(), sep=" ") if pd.notna(x) else np.nan)
        self._data["cod_unidade"] = self._raw_data["CO_UNI_NOT"].apply(lambda x: x if pd.notna(x) else np.nan)

        self._data["FONETICA_N"] = self._data["nome"].apply(lambda x: f"{x.split(' ')[0]}{x.split(' ')[-1]}" if pd.notna(x) else np.nan)

        self._data["nascimento_dia"] = self._data["dt_nasc"].apply(lambda x: x.day if hasattr(x, 'day') and pd.notna(x) else np.nan)
        self._data["nascimento_mes"] = self._data["dt_nasc"].apply(lambda x: x.month if hasattr(x, 'day') and pd.notna(x) else np.nan)
        self._data["nascimento_ano"] = self._data["dt_nasc"].apply(lambda x: x.year if hasattr(x, 'day') and pd.notna(x) else np.nan)

        self._data["primeiro_nome"] = self._data["nome"].apply(lambda x: x.split(" ")[0] if pd.notna(x) else np.nan )
        #self._data["segundo_nome"] = self._data["nome"].apply(lambda x: x.split(" ")[1] if pd.notna(x) and len(x.split(" "))>1 else np.nan )
        self._data["complemento_nome"] = self._data["nome"].apply(lambda x: ' '.join(x.split(" ")[1:]) if pd.notna(x) and len(x.split(" "))>1 else np.nan )
        
        self._data["primeiro_nome_mae"] = self._data["nome_mae"].apply(lambda x: x.split(" ")[0] if pd.notna(x) else np.nan )
        self._data["segundo_nome_mae"] = self._data["nome_mae"].apply(lambda x: x.split(" ")[1] if pd.notna(x) and len(x.split(" "))>1 else np.nan )
        self._data["complemento_nome_mae"] = self._data["nome_mae"].apply(lambda x: ' '.join(x.split(" ")[2:]) if pd.notna(x) and len(x.split(" "))>2 else np.nan )

    def define_duplicate(self, persons_pairs, delta_notific=None, delta_sintomas=None, same_unit=None):
        '''
            Description.

            Args:
            -----
                persons_pairs:
                    List.
                delta_notific:
                    Integer.
                delta_sintomas:
                    Integer.
                same_unit:
                    Boolean.

            Return:
            -------
                positives:
                    List of 2-tuples.
                negatives:
                    List of 2-tuples.
        '''
        temp_index = self.raw_data.set_index("ID_SIVEP")

        positives = []
        negatives = []

        for pair in persons_pairs:
            left, right = pair[0], pair[1]
            
            dt_not_left, dt_not_right = temp_index["DT_NOTIFIC"].loc[left], temp_index["DT_NOTIFIC"].loc[right]
            dt_sin_left, dt_sin_right = temp_index["DT_SIN_PRI"].loc[left], temp_index["DT_SIN_PRI"].loc[right]
            id_not_left, id_not_right = temp_index["CO_UNI_NOT"].loc[left], temp_index["CO_UNI_NOT"].loc[right]

            cur_delta_not = abs((dt_not_left-dt_not_right).days)
            cur_delta_sin = abs((dt_sin_left-dt_sin_right).days)

            if cur_delta_not<=delta_notific and cur_delta_not>=0 and cur_delta_sin<=delta_sintomas:
                if same_unit and id_not_left==id_not_right:
                    positives.append(pair)
                elif same_unit and id_not_left!=id_not_right:
                    negatives.append(pair)
                else:
                    positives.append(pair)
            else:
                negatives.append(pair)
        return positives, negatives


'''
    -------> SINAN
'''
class ProcessSinan(ProcessBase):
    db_type = "SINAN"
    
    def process(self):
        '''
        
        '''
        self._data["nome"] = self._raw_data["NM_PACIENT"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan).apply(lambda x: re.sub(' {2,}', ' ', x) if pd.notna(x) else np.nan)
        self._data["nome_mae"] = self._raw_data["NM_MAE_PAC"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan).apply(lambda x: re.sub(' {2,}', ' ', x) if pd.notna(x) else np.nan)
        
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
        self._data["bairro"] = self._data["bairro"].apply(lambda x: general_utils.uniformize_name(x, sep=" ") if pd.notna(x) else np.nan)

        self._data = self._data.drop(["dt_nasc", "dt_notific"], axis=1)
        self._data["evolucao"] = self._raw_data["EVOLUCAO"].copy()
        self._data["classi_fin"] = self._raw_data["CLASSI_FIN"].copy()
        ## --> FONETICA for blocking
        self._data["FONETICA_N"] = self._data["nome"].apply(lambda x: f"{x.split(' ')[0]}{x.split(' ')[-1]}" if pd.notna(x) else np.nan)

'''
    ------> GAL
'''
class ProcessGal(ProcessBase):
    db_type = "GAL"

    def process(self):
        '''
        
        '''
        self._data["nome"] = self._raw_data["PACIENTE"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan)
        self._data["nome_mae"] = self._raw_data["NOME DA MÃE"].apply(lambda x: general_utils.uniformize_name(x, sep=" ") if pd.notna(x) else np.nan)
        self._data["sexo"] = self._raw_data["SEXO"].fillna("I").map({"MASCULINO": "M", "FEMININO": "F", "IGNORADO": "I"})
        self._data["cns"] = self._raw_data["CNS DO PACIENTE"].copy()
        self._data["nascimento"] = self._raw_data["DATA DE NASCIMENTO"].apply(lambda x: f"{x.day:2.0f}/{x.month:2.0f}/{x.year:2.0f}".replace(" ", "0") if pd.notna(x) else np.nan)
        self._data["solicitacao"] = self._raw_data["DATA DA SOLICITAÇÃO"].apply(lambda x: f"{x.day:2.0f}/{x.month:2.0f}/{x.year:2.0f}".replace(" ", "0") if pd.notna(x) else np.nan)
        
        self._data["nome"] = self._data["nome"].apply(lambda x: general_utils.uniformize_name(x, sep=" ") if pd.notna(x) else np.nan)

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


class ProcessSivep(ProcessBase):
    db_type = "SIVEP"

    def process(self):
        '''
        
        '''
        # --> Generate variables for LINKAGE
        self._data["nome"] = self._raw_data["NM_PACIENT"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan).apply(lambda x: re.sub(' {2,}', ' ', x) if pd.notna(x) else np.nan)
        self._data["nome_mae"] = self._raw_data["NM_MAE_PAC"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan).apply(lambda x: re.sub(' {2,}', ' ', x) if pd.notna(x) else np.nan)
        self._data["nome_mae"] = self._data["nome_mae"].apply(lambda x: general_utils.uniformize_name(x, sep=" ") if pd.notna(x) else np.nan)
        self._data["nome"] = self._data["nome"].apply(lambda x: general_utils.uniformize_name(x, sep=" ") if pd.notna(x) else np.nan)

        self._data["dt_notific"] = pd.to_datetime(self._raw_data["DT_NOTIFIC"], format="%Y-%m-%d", errors="coerce")
        self._data["dt_sin_pri"] = pd.to_datetime(self._raw_data["DT_SIN_PRI"], format="%Y-%m-%d", errors="coerce")
        self._data["dt_nasc"] = self._raw_data["DT_NASC"].apply(lambda x: pd.to_datetime(x, format="%Y-%m-%d", errors="coerce"))
        self._data["uni_notific"] = self._raw_data["ID_UNIDADE"].apply(lambda x: x if pd.notna(x) else np.nan)

        self._data["nascimento"] = self._data["dt_nasc"].apply(lambda x: f"{x.day:2.0f}/{x.month:2.0f}/{x.year:2.0f}".replace(" ", "0") if pd.notna(x) else np.nan)
        self._data["notificacao"] = self._data["dt_notific"].apply(lambda x: f"{x.day:2.0f}/{x.month:2.0f}/{x.year:2.0f}".replace(" ", "0") if pd.notna(x) else np.nan)
        self._data["sint_pri"] = self._data["dt_sin_pri"].apply(lambda x: f"{x.day:2.0f}/{x.month:2.0f}/{x.year:2.0f}".replace(" ", "0") if pd.notna(x) else np.nan)

        self._data["sexo"] = self._raw_data["CS_SEXO"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan)
        self._data["cpf"] = self._raw_data["NU_CPF"].apply(lambda x: x if pd.notna(x) else np.nan)
        self._data["cep"] = self._raw_data["NU_CEP"].apply(lambda x: x if pd.notna(x) else np.nan)
        self._data["cns"] = self._raw_data["NU_CNS"].apply(lambda x: x if pd.notna(x) and x!='000000000000000' and x!='999999999999999' else np.nan)

        self._data["nascimento_dia"] = self._data["dt_nasc"].apply(lambda x: x.day if pd.notna(x) else np.nan)
        self._data["nascimento_mes"] = self._data["dt_nasc"].apply(lambda x: x.month if pd.notna(x) else np.nan)
        self._data["nascimento_ano"] = self._data["dt_nasc"].apply(lambda x: x.year if pd.notna(x) else np.nan)
        self._data["notific_dia"] = self._data["dt_notific"].apply(lambda x: x.day if pd.notna(x) else np.nan)
        self._data["notific_mes"] = self._data["dt_notific"].apply(lambda x: x.month if pd.notna(x) else np.nan)
        self._data["notific_ano"] = self._data["dt_notific"].apply(lambda x: x.year if pd.notna(x) else np.nan)

        self._data["primeiro_nome"] = self._data["nome"].apply(lambda x: x.split(" ")[0] if pd.notna(x) else np.nan )
        self._data["segundo_nome"] = self._data["nome"].apply(lambda x: x.split(" ")[1] if pd.notna(x) and len(x.split(" "))>1 else np.nan )
        self._data["complemento_nome"] = self._data["nome"].apply(lambda x: ' '.join(x.split(" ")[2:]) if pd.notna(x) and len(x.split(" "))>2 else np.nan )

        self._data["primeiro_nome_mae"] = self._data["nome_mae"].apply(lambda x: x.split(" ")[0] if pd.notna(x) else np.nan )
        self._data["segundo_nome_mae"] = self._data["nome_mae"].apply(lambda x: x.split(" ")[1] if pd.notna(x) and len(x.split(" "))>1 else np.nan )
        self._data["complemento_nome_mae"] = self._data["nome_mae"].apply(lambda x: ' '.join(x.split(" ")[2:]) if pd.notna(x) and len(x.split(" "))>2 else np.nan )

        self._data["bairro"] = self._raw_data["NM_BAIRRO"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan)
        self._data["bairro"] = self._data["bairro"].apply(lambda x: general_utils.uniformize_name(x, sep=" ") if pd.notna(x) else np.nan)

        self._data["evolucao"] = self._raw_data["EVOLUCAO"].copy()
        self._data["classi_fin"] = self._raw_data["CLASSI_FIN"].copy()

        self._data["FONETICA_N"] = self._data["nome"].apply(lambda x: f"{x.split(' ')[0]}{x.split(' ')[-1]}" if pd.notna(x) else np.nan)