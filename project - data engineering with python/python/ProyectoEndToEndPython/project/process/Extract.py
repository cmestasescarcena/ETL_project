import sqlalchemy as db
from sqlalchemy import text
import pandas as pd

from google.cloud.storage import Client
import os
from io import StringIO

from utils.Utilities import Utilities as u

from azure.storage.blob import ContainerClient

from pandas import DataFrame

class Extract():

    def __init__(self) -> None:
        self.process = 'Extract Process'

    # Lectura de base de datos MySQL
    # def read_mysql(self,  _username, _password, _ip, _port, _db_name, _table_name):
    def read_mysql(self, _db_name, _table_name):
        #engine = db.create_engine(f"mysql://{_username}:{_password}@{_ip}:{_port}/{_db_name}")
        #conn   = engine.connect()
        conn   = u.get_client_mysql(_db_name)
        df     = pd.read_sql_query(text(f'SELECT * FROM {_table_name}'), con=conn)        
        return df

    # Lectura de Cloud Storage GCP
    def read_cloud_storage(self, _bucketname, _fileName):
        client          = u.get_client_cloud_storage()
        bucket          = client.get_bucket(f'{_bucketname}')
        blob            = bucket.get_blob(f'{_fileName}')
        downloaded_file = blob.download_as_text(encoding="utf-8")
        df              = pd.read_csv(StringIO(downloaded_file))
        return df
        
    """
    # Lectura de Cloud Storage GCP v2
    def read_cloud_storageB(self, _bucketname, _colleciontName):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/user/app/ProyectoEndToEndPython/Proyecto/credenciales/cmestas-datadep12-821a37dff56f.json"
        client          = Client() 
        bucket          = client.get_bucket(f'{_bucketname}')
        blob            = bucket.get_blob(f'{_colleciontName}')
        downloaded_file = blob.download_as_text(encoding="utf-8")
        df              = pd.read_csv(StringIO(downloaded_file))

        return df
    """ 

    """"
    # Lectura Azure Datalake
    def read_adls(self, _containerName, _fileName):
        container_client = u.get_client_azure_storage(_containerName)
        downloaded_blob  = container_client.download_blob(_fileName)
        df               = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
        return df
    """
    
    # Lectura MongoDB
    def read_mongo_db(self, _databaseName, _collectionName):
        dbname           = u.get_client_mongo_db(_databaseName)
        collection_name  = dbname[_collectionName]
        collection       = collection_name.find({})
        df               = DataFrame(collection)
        df               = df.drop(['_id'], axis=1)
        return df

    



    
        
    