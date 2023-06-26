import pandas as pd
import os
import io
from io import StringIO
from utils.Utilities import Utilities as u

import sqlalchemy as db
from sqlalchemy import text

class Load():

    def __init__(self) -> None:
        self.process = 'Load Process'

    # Escritura en base de datos MySQL
    # def load_to_mysql(self, _name, _df):
    def load_to_mysql(self, _db_name, _tablename, _df):  
#       engine = db.create_engine("mysql://root:root@192.168.3.60:3310/retail_db")
#       conn = engine.connect()
#        conn = db.create_engine("mysql://root:root@192.168.3.60:3310/test")
#        _df.to_sql(con=conn, name=_name,if_exists='append',index=False)
        conn   = u.get_client_mysql(_db_name)
#        conn.execute(f'TRUNCATE TABLE {_tablename}')
        _df.to_sql(con=conn, name=_tablename,if_exists='replace',index=False)
        return None

    # Escritura en Cloud Storage GCP
    def load_to_cloud_storage(self, _bucketname, _fileName, _df):
        client          = u.get_client_cloud_storage()
        bucket          = client.get_bucket(f'{_bucketname}')
        bucket.blob(_fileName).upload_from_string(_df.to_csv(encoding = "utf-8", index=False), 'text/csv')
        
    """"
    # Escritura en Azure Datalake
    def read_adls(self, _containerName, _fileName, _df):
        container_client = u.get_client_azure_storage(_containerName)
        output = io.StringIO()
        output = _df.to_csv(encoding = "utf-8", index=False)
        container_client.upload_blob(_fileName, output, overwrite=True, encoding='utf-8')
    """