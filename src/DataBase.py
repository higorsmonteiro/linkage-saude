'''
    Define common behavior of each database type considering the inherent
    specifics for each particular database.

    Common behaviors:
        - Validate database. 
        - Create unique identifier (ID).
        - Provide a dictionary to the codification. 
'''
import pandera
import pandas as pd
from collections import defaultdict
from pandera import DataFrameSchema, Column

from src.CustomExceptions import *
import lib.database_utils as db_utils

class DataBase:
    def __init__(self, data) -> None:
        self._raw_data = data.copy()
        self._data = None

        # -- Get empty columns
        cols_temp = self._raw_data.dropna(axis=1, how='all').columns
        self.empty_columns = [ x for x in self._raw_data if x not in cols_temp ]

    @property
    def raw_data(self):
        return self._raw_data

    @raw_data.setter
    def raw_data(self, x):
        raise AttributeError("No possible to change this attribute.")

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, x):
        raise AttributeError("No possible to change this attribute.")

class DataSinan(DataBase):
    db_type = "SINAN"

    def validate_schema(self):
        '''Define all validations for the SINAN database.

            Args:
            --------
                None.

        '''
        # -- Columns to not validate because they are empty
        not_validate = defaultdict(lambda: True, zip(self.empty_columns, [False for n in self.empty_columns]))

        # Create validation patterns 
        schema_dates_1 = DataFrameSchema(
            {
                'DT_NOTIFIC': Column('datetime64[ns]', coerce=True, nullable=True),
                'DT_SIN_PRI': Column('datetime64[ns]', coerce=True, nullable=True),
                'DT_NASC': Column('datetime64[ns]', coerce=True, nullable=True),
            }, strict=False, coerce=False
        )
        schema_dates_2 = DataFrameSchema(
            {
                'DT_NOTIFIC': Column(object, coerce=True, nullable=True),
                'DT_SIN_PRI': Column(object, coerce=True, nullable=True),
                'DT_NASC': Column(object, coerce=True, nullable=True),
            }, strict=False, coerce=False
        )
        
        schema_object = DataFrameSchema(
            {
                'NU_NOTIFIC': Column(object, nullable=True, required=True),
                'TP_NOT': Column(object, nullable=True, required=not_validate['TP_NOT']),
                'ID_AGRAVO': Column(object, nullable=True, required=True),
                'SEM_NOT': Column(object, nullable=True, required=not_validate['SEM_NOT']),
                'NU_ANO': Column(object, nullable=True, required=not_validate['NU_ANO']),
                'SG_UF_NOT': Column(object, nullable=True, required=not_validate['SG_UF_NOT']),
                'ID_MUNICIP': Column(object, nullable=True, required=True),
                'ID_REGIONA': Column(object, nullable=True, required=not_validate['ID_REGIONA']),
                'SEM_PRI': Column(object, nullable=True, required=not_validate['SEM_PRI']),
                'NM_PACIENT': Column(object, nullable=True, required=not_validate['NM_PACIENT']),
                'NU_IDADE_N': Column(object, nullable=True, required=not_validate['NU_IDADE_N']),
                'CS_SEXO': Column(object, nullable=True, required=not_validate['CS_SEXO']),
                'CS_GESTANT': Column(object, nullable=True, required=not_validate['CS_GESTANT']),
                'CS_RACA': Column(object, nullable=True, required=not_validate['CS_RACA']),
                'CS_ESCOL_N': Column(object, nullable=True, required=not_validate['CS_ESCOL_N']),
                'ID_CNS_SUS': Column(object, nullable=True, required=not_validate['ID_CNS_SUS']),
                'NM_MAE_PAC': Column(object, nullable=True, required=not_validate['NM_MAE_PAC']),
                'SG_UF': Column(object, nullable=True, required=not_validate['SG_UF']),
                'ID_MN_RESI': Column(object, nullable=True, required=not_validate['ID_MN_RESI']),
                'ID_RG_RESI': Column(object, nullable=True, required=not_validate['ID_RG_RESI']),
                'ID_DISTRIT': Column(object, nullable=True, required=not_validate['NM_BAIRRO']),
                'NM_BAIRRO': Column(object, nullable=True, required=not_validate['NM_BAIRRO']),
                'NM_LOGRADO': Column(object, nullable=True, required=not_validate['NM_LOGRADO']),
                'NU_NUMERO': Column(object, nullable=True, required=not_validate['NU_NUMERO']),
                'NM_COMPLEM': Column(object, nullable=True, required=not_validate['NM_COMPLEM']),
                'NM_REFEREN': Column(object, nullable=True, required=not_validate['NM_REFEREN']),
                'NU_CEP': Column(object, nullable=True, required=not_validate['NU_CEP']),
                'NU_DDD_TEL': Column(object, nullable=True, required=not_validate['NU_DDD_TEL']),
                'NU_TELEFON': Column(object, nullable=True, required=not_validate['NU_TELEFON']),
                'CS_ZONA': Column(object, nullable=True, required=not_validate['CS_ZONA']),
                'ID_PAIS': Column(object, nullable=True, required=not_validate['ID_PAIS']),
            }, strict=False, coerce=False
        )
        
        # Validations
        # -- Validation 1: Date columns
        try:
            schema_dates_1.validate(self._raw_data)
        except pandera.errors.SchemaErrors:
            try:
                schema_dates_2.validate(self._raw_data)
                # -- Covert object columns
                self._raw_data["DT_NOTIFIC"] = pd.to_datetime(self._raw_data["DT_NOTIFIC"])

            except pandera.errors.SchemaErrors:
                pandera.errors.SchemaError("Essential date columns are neither of date or object format")

        # -- Validation 2: Essential SINAN columns (original data must be preserved => object columns)
        schema_object.validate(self._raw_data.dropna(axis=1, how='all'))
        print("Basic validation complete.")


    def create_id(self):
        '''
        
        '''
        # --> Create 'ID_GEO'
        self._raw_data["DT_NOTIFIC_FMT"] = self._raw_data["DT_NOTIFIC"].apply(lambda x: f"{x.day:2.0f}{x.month:2.0f}{x.year}".replace(" ", "0"))
        self._raw_data["ID_MUNICIP"] = self._raw_data["ID_MUNICIP"].apply(lambda x: f"{x}")
        self._raw_data["ID_GEO"] = self._raw_data["ID_AGRAVO"]+self._raw_data["NU_NOTIFIC"]+self._raw_data["ID_MUNICIP"]+self._raw_data["DT_NOTIFIC_FMT"]
        
        self._data = pd.DataFrame(self._raw_data["ID_GEO"])
        self._raw_data = self._raw_data.drop("DT_NOTIFIC_FMT", axis=1)


class DataSivep(DataBase):
    db_type = "SIVEP"

class DataSim(DataBase):
    db_type = "SIM"

class DataSinasc(DataBase):
    db_type = "SINASC"

class DataExtra(DataBase):
    db_type = ""
