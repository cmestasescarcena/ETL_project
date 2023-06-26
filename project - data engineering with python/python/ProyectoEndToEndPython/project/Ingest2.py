from process.Extract import Extract
from process.Load import Load
import pandas as pd
import sys

extract = Extract()
load    = Load()

funcName     = sys.argv[1]
sourceA      = sys.argv[2]
bucketNameB  = sys.argv[3]
sourceObject = sys.argv[4]
fileNameB    = sys.argv[5]


def mysql(_database, _tablename):
    df = extract.read_mysql(_database, _tablename)
    return df

def mongo_db(_database, _tablename):
    df = extract.read_mongo_db(_database, _tablename)
    return df

def cloud_storage(_bucketNameA, _fileNameA):
    df = extract.read_cloud_storage(_bucketNameA, _fileNameA)
    return df

func_dict = {
    "mysql": mysql,
    "mongo_db": mongo_db,
    "cloud_storage": cloud_storage    
}

df = func_dict[funcName](sourceA, sourceObject)

load.load_to_cloud_storage(bucketNameB,fileNameB,df)




#extract.read_cloud_storage(bucketNameA,fileNameA)
#customers_df   = extract.read_cloud_storage('data-engineering-python','retail/customers')

#load.load_to_cloud_storage(bucketNameB,fileNameB,df)
#load.load_to_cloud_storage('data-engineering-python','landing/customers',customers_df)


"""
categories_df  = extract.read_mysql('retail_db', 'categories')
customers_df   = extract.read_cloud_storage('data-engineering-python','retail/customers')
departments_df = extract.read_cloud_storage('data-engineering-python','retail/departments')
order_items_df = extract.read_cloud_storage('data-engineering-python','retail/order_items')
orders_df      = extract.read_cloud_storage('data-engineering-python','retail/orders')
products_df    = extract.read_mongo_db('retail_db','products')
"""





