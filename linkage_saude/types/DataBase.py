# -*- coding: utf-8 -*- 

'''
    Define the behavior of each database type considering their inherent
    details.

    Common behaviors:
        - Validate database. 
        - Create a unique identifier (ID).
        - Provide a dictionary to the codification.
''' 

import pandera
import pandas as pd
from collections import defaultdict
from pandera import DataFrameSchema, Column

# ----> custom
from linkage_saude.exceptions import *

# --> class definitions
class DataBase:
    def __init__(self, data) -> None:
        self._raw_data = data.copy()
        self._data = None

        # -- Get empty columns
        cols_temp = self._raw_data.dropna(axis=1, how='all').columns
        self.empty_columns = [ x for x in self._raw_data if x not in cols_temp ]

        # -- Id created
        self.has_id = False
        self.validated = False

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


'''
    # ----- SINAN data object ----- #
'''
class DataSinan(DataBase):
    db_type = "SINAN"

    def validate_schema(self):
        '''Define all validations for the SINAN database.

            Args:
            --------
                None.

        '''
        # -- Columns to not validate because they are empty
        dont_validate = defaultdict(lambda: True, zip(self.empty_columns, [False for n in self.empty_columns]))

        # Create validation patterns 
        schema_dates_1 = DataFrameSchema(
            {
                'DT_NOTIFIC': Column('datetime64[ns]', coerce=False, nullable=True),
                'DT_SIN_PRI': Column('datetime64[ns]', coerce=False, nullable=True),
                'DT_NASC': Column('datetime64[ns]', coerce=False, nullable=True),
            }, strict=False, coerce=False
        )
        schema_dates_2 = DataFrameSchema(
            {
                'DT_NOTIFIC': Column(object, coerce=False, nullable=True),
                'DT_SIN_PRI': Column(object, coerce=False, nullable=True),
                'DT_NASC': Column(object, coerce=False, nullable=True),
            }, strict=False, coerce=False
        )
        
        schema_object = DataFrameSchema(
            {
                'NU_NOTIFIC': Column(object, nullable=True, required=True),
                'TP_NOT': Column(object, nullable=True, required=dont_validate['TP_NOT']),
                'ID_AGRAVO': Column(object, nullable=True, required=True),
                'NU_ANO': Column(object, nullable=True, required=dont_validate['NU_ANO']),
                'SG_UF_NOT': Column(object, nullable=True, required=dont_validate['SG_UF_NOT']),
                'ID_MUNICIP': Column(object, nullable=True, required=True),
                'ID_REGIONA': Column(object, nullable=True, required=dont_validate['ID_REGIONA']),
                'NM_PACIENT': Column(object, nullable=True, required=dont_validate['NM_PACIENT']),
                'NU_IDADE_N': Column(object, nullable=True, required=dont_validate['NU_IDADE_N']),
                'CS_SEXO': Column(object, nullable=True, required=dont_validate['CS_SEXO']),
                'CS_GESTANT': Column(object, nullable=True, required=dont_validate['CS_GESTANT']),
                'CS_RACA': Column(object, nullable=True, required=dont_validate['CS_RACA']),
                'CS_ESCOL_N': Column(object, nullable=True, required=dont_validate['CS_ESCOL_N']),
                'ID_CNS_SUS': Column(object, nullable=True, required=dont_validate['ID_CNS_SUS']),
                'NM_MAE_PAC': Column(object, nullable=True, required=dont_validate['NM_MAE_PAC']),
                'SG_UF': Column(object, nullable=True, required=dont_validate['SG_UF']),
                'ID_MN_RESI': Column(object, nullable=True, required=dont_validate['ID_MN_RESI']),
                'ID_RG_RESI': Column(object, nullable=True, required=dont_validate['ID_RG_RESI']),
                'ID_DISTRIT': Column(object, nullable=True, required=dont_validate['NM_BAIRRO']),
                'NM_BAIRRO': Column(object, nullable=True, required=dont_validate['NM_BAIRRO']),
                'NM_LOGRADO': Column(object, nullable=True, required=dont_validate['NM_LOGRADO']),
                'NU_NUMERO': Column(object, nullable=True, required=dont_validate['NU_NUMERO']),
                'NM_COMPLEM': Column(object, nullable=True, required=dont_validate['NM_COMPLEM']),
                'NM_REFEREN': Column(object, nullable=True, required=dont_validate['NM_REFEREN']),
                'NU_CEP': Column(object, nullable=True, required=dont_validate['NU_CEP']),
                'NU_DDD_TEL': Column(object, nullable=True, required=dont_validate['NU_DDD_TEL']),
                'NU_TELEFON': Column(object, nullable=True, required=dont_validate['NU_TELEFON']),
                'CS_ZONA': Column(object, nullable=True, required=dont_validate['CS_ZONA']),
                'ID_PAIS': Column(object, nullable=True, required=dont_validate['ID_PAIS']),
            }, strict=False, coerce=False
        )
        
        # Validations
        # -- Validation 1: Date columns
        try:
            schema_dates_1.validate(self._raw_data)
            self.validated = True
        except (pandera.errors.SchemaError, pandera.errors.SchemaErrors):
            try:
                schema_dates_2.validate(self._raw_data)
                # -- Covert object columns
                self._raw_data["DT_NOTIFIC"] = pd.to_datetime(self._raw_data["DT_NOTIFIC"])
                self._raw_data["DT_SIN_PRI"] = pd.to_datetime(self._raw_data["DT_SIN_PRI"])
                self._raw_data["DT_NASC"] = pd.to_datetime(self._raw_data["DT_NASC"])
                self.validated = True
            except (pandera.errors.SchemaError, pandera.errors.SchemaErrors):
                pandera.errors.SchemaError("Essential date columns are neither date nor object format")

        # -- Validation 2: Essential SINAN columns (original data must be preserved => object columns)
        try:
            schema_object.validate(self._raw_data.dropna(axis=1, how='all'))
            self.validated = True
        except (pandera.errors.SchemaError, pandera.errors.SchemaErrors) as err:
            print(err.args)


    def create_id(self):
        '''
        
        '''
        # --> Create 'ID_GEO'
        self._raw_data["DT_NOTIFIC_FMT"] = self._raw_data["DT_NOTIFIC"].apply(lambda x: f"{x.day:2.0f}{x.month:2.0f}{x.year}".replace(" ", "0"))
        self._raw_data["ID_MUNICIP"] = self._raw_data["ID_MUNICIP"].apply(lambda x: f"{x}")
        self._raw_data["ID_GEO"] = self._raw_data["ID_AGRAVO"]+self._raw_data["NU_NOTIFIC"]+self._raw_data["ID_MUNICIP"]+self._raw_data["DT_NOTIFIC_FMT"]
        
        self._data = pd.DataFrame(self._raw_data["ID_GEO"])
        self._raw_data = self._raw_data.drop("DT_NOTIFIC_FMT", axis=1)
        self.has_id = True


'''
    # ----- GAL data object ----- #
'''
class DataGal(DataBase):
    db_type="GAL"

    def validate_schema(self):
        '''Define the validations for the GAL database.

            Args:
            --------
                None.

        '''
        # -- Columns to not validate because they are empty
        dont_validate = defaultdict(lambda: True, zip(self.empty_columns, [ False for n in self.empty_columns]))
        # -- All columns set to uppercase.
        self._raw_data.columns = [ name.upper() for name in self._raw_data.columns ]
        
        # Create validation patterns 
        schema_dates_1 = DataFrameSchema(
            {
                'DATA DE NASCIMENTO': Column('datetime64[ns]', coerce=False, nullable=True),
                'DATA DE CADASTRO': Column('datetime64[ns]', coerce=False, nullable=True),
                'DATA DA SOLICITAÇÃO': Column('datetime64[ns]', coerce=False, nullable=True),
            }, strict=False, coerce=False
        )
        schema_dates_2 = DataFrameSchema(
            {
                'DATA DE NASCIMENTO': Column(object, coerce=False, nullable=True),
                'DATA DE CADASTRO': Column(object, coerce=False, nullable=True),
                'DATA DA SOLICITAÇÃO': Column(object, coerce=False, nullable=True),
            }, strict=False, coerce=False
        )
        
        schema_object = DataFrameSchema(
            {
                'REQUISIÇÃO': Column(object, nullable=True, required=True),
                'CNES LABORATÓRIO DE CADASTRO': Column(object, nullable=True, required=True),
                'CNES UNIDADE SOLICITANTE': Column(object, nullable=True, required=True),
                'IBGE MUNICÍPIO SOLICITANTE': Column(object, nullable=True, required=True),
                'MUNICIPIO DE RESIDÊNCIA': Column(object, nullable=True, required=True),
                'REQUISIÇÃO CORRELATIVO (S/N)': Column(object, nullable=True, required=dont_validate['REQUISIÇÃO CORRELATIVO (S/N)']),
                'PACIENTE': Column(object, nullable=True, required=dont_validate['PACIENTE']),
                'NOME DA MÃE': Column(object, nullable=True, required=dont_validate['NOME DA MÃE']),
                'SEXO': Column(object, nullable=True, required=dont_validate['SEXO']),
            }, strict=False, coerce=False
        )
        
        # Validations
        # -- Validation 1: Date columns
        try:
            schema_dates_1.validate(self._raw_data)
            self.validated = True
        except (pandera.errors.SchemaError, pandera.errors.SchemaErrors):
            try:
                schema_dates_2.validate(self._raw_data)
                # -- Covert object columns
                self._raw_data["DATA DE NASCIMENTO"] = pd.to_datetime(self._raw_data["DATA DE NASCIMENTO"])
                self._raw_data["DATA DE CADASTRO"] = pd.to_datetime(self._raw_data["DATA DE CADASTRO"])
                self._raw_data["DATA DA SOLICITAÇÃO"] = pd.to_datetime(self._raw_data["DATA DA SOLICITAÇÃO"])
                self.validated = True
            except (pandera.errors.SchemaError, pandera.errors.SchemaErrors):
                pandera.errors.SchemaError("Essential date columns are neither date nor object format")

        # -- Validation 2: Essential GAL columns (original data must be preserved => object columns)
        try:
            schema_object.validate(self._raw_data.dropna(axis=1, how='all'))
            self.validated = True
        except (pandera.errors.SchemaError, pandera.errors.SchemaErrors) as err:
            print(err.args)

    def create_id(self):
        '''
            For GAL database, an unique ID generated from the original fields is not straightforward due to the 
            nature of the notification process in the system. An exam requisition (one single number) can trigger
            several samples and exams for a single person. Therefore, we create two IDs, one ('GAL_ID') to highlight 
            the notifications inside the whole database, the other ('UNIQUE_ID') to single out each notifications 
            existent in the specific file provided for analysis.

            Args:
            -----
                None.
        '''
        self._raw_data["DATA DA SOLICITAÇÃO_FMT"] = self._raw_data["DATA DA SOLICITAÇÃO"].apply(lambda x: f"{x.day:2.0f}{x.month:2.0f}{x.year}".replace(" ", "0"))
        self._raw_data["IBGE MUNICÍPIO SOLICITANTE"] = self._raw_data["IBGE MUNICÍPIO SOLICITANTE"].apply(lambda x: f"{x}")
        self._raw_data["GAL_ID"] = self._raw_data["REQUISIÇÃO"]+self._raw_data["DATA DA SOLICITAÇÃO_FMT"]+self._raw_data["CNES UNIDADE SOLICITANTE"]+\
                                   self._raw_data["IBGE MUNICÍPIO SOLICITANTE"]
        self._raw_data["UNIQUE_ID"] = self._raw_data["GAL_ID"]+[ f"{n:8.0f}".replace(" ", "0") for n in range(self._raw_data.shape[0]) ] 

        self._data = self._raw_data[["GAL_ID", "UNIQUE_ID"]].copy()
        self._raw_data = self._raw_data.drop("DATA DA SOLICITAÇÃO_FMT", axis=1)
        self.has_id = True

'''
    # ----- CUSTOM data object ----- #
'''
class DataExtra(DataBase):
    db_type = ""
