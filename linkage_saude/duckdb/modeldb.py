import os
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import Sequence, Integer, String, Numeric, DateTime, ForeignKey

class DataModel:
    def __init__(self) -> None:
        '''
        
        '''
        self._metadata = MetaData()
        
        self._datasus_records = Table(
            'datasus_records', self._metadata,
            Column("ID_UNICO", String, primary_key=True),
            Column("FONTE_DATASUS", String, nullable=False),
            Column("NOME_PACIENTE", String),
            Column("SEXO", String(1)),
            Column("DATA_NASCIMENTO", DateTime, nullable=True),
            Column("RACA_COR", String),
            Column("NOME_MAE", String),
            Column("CPF", String),
            Column("CNS", String),
            Column("BAIRRO", String),
            Column("LOGRADOURO", String),
            Column("CEP", String),
            Column('CRIADO_EM', DateTime, default=datetime.now)
        )


        self._datasus_matching = Table(
            'datasus_matching', self._metadata,
            Column("ID_1", ForeignKey('datasus_records.ID_UNICO')),
            Column("ID_2", ForeignKey('datasus_records.ID_UNICO')),
            Column("PRIMEIRO_NOME", Numeric(6,5)),
            Column("COMPLEMENTO_NOME", Numeric(6,5)),
            Column("SEXO", Integer),
            Column("NASCIMENTO_DIA", Integer), 
            Column("NASCIMENTO_MES", Integer), 
            Column("NASCIMENTO_ANO", Integer), 
            Column("RACA_COR", Integer),
            Column("PRIMEIRO_NOME_MAE", Numeric(6,5)),
            Column("SEGUNDO_NOME_MAE", Numeric(6,5)),
            Column("COMPLEMENTO_NOME_MAE", Numeric(6,5)),
            Column("CPF", Numeric(6,5)),
            Column("CNS", Numeric(6,5)),
            Column("BAIRRO", Numeric(6,5)),
            Column("LOGRADOURO", Numeric(6,5)),
            Column("CEP", Numeric(6,5)),
            Column('CRIADO_EM', DateTime, default=datetime.now)
        )

    @property
    def datasus_records(self):
        return self._datasus_records
    
    @datasus_records.setter
    def datasus_records(self, x):
        raise AttributeError()

    @property
    def datasus_matching(self):
        return self._datasus_matching
    
    @datasus_matching.setter
    def datasus_matching(self, x):
        raise AttributeError()
    
    def create_engine(self, url, **kwargs):
        self.engine = create_engine(url, **kwargs)
        self._metadata.create_all(self.engine)

