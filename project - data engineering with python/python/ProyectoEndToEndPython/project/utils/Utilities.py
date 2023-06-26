from google.cloud.storage import Client
from azure.storage.blob import ContainerClient
from pymongo import MongoClient
import os
import yaml
from io import StringIO

import sqlalchemy as db
from sqlalchemy import text

with open('/user/app/ProyectoEndToEndPython/project/config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

class Utilities():
    
    # Cliente para Cloud Storage GCP
    def get_client_cloud_storage():        
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["cloud_storage"]["path"]
        client = Client()                 
        return client

    """
    # Cliente para Azure Datalake
    @staticmethod
    def get_client_azure_storage(_containerName):
        conn_str  = "BlobEndpoint=https://adlsdatapath.blob.core.windows.net/;QueueEndpoint=https://adlsdatapath.queue.core.windows.net/;FileEndpoint=https://adlsdatapath.file.core.windows.net/;TableEndpoint=https://adlsdatapath.table.core.windows.net/;SharedAccessSignature=sv=2022-11-02&ss=bfqt&srt=co&sp=rwdlacupyx&se=2023-06-07T09:05:36Z&st=2023-06-07T01:05:36Z&spr=https,http&sig=RjAPfpvS6QLpsp5gGKqFyGqi4hW7BJ5C5rPC1n7RwVE%3D"
        container_client = ContainerClient.from_connection_string(
            conn_str       = conn_str, 
            container_name = _containerName
        )       
        return container_client
    """
    
    # Cliente para MongoDB
    def get_client_mongo_db(_databaseName):   
        CONNECTION_STRING = config["mongo_db"]["connection_string"]
        client            = MongoClient(CONNECTION_STRING)
        return client[_databaseName]

    # Cliente para MySQL
    def get_client_mysql(_databaseName):
        engine = db.create_engine(f'mysql://{config["mysql"]["user"]}:{config["mysql"]["pass"]}@{config["mysql"]["host"]}:{config["mysql"]["port"]}/{_databaseName}')
        conn = engine.connect()
        return conn



