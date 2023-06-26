from process.Extract import Extract
from process.Load import Load
#from process.Transform import Trans
#form from process.Load import Load
import pandas as pd

extract = Extract()
load    = Load()

print("INICIA EXTRACCION")

categories_df  = extract.read_mysql('retail_db', 'categories')
customers_df   = extract.read_cloud_storage('data-engineering-python','retail/customers')
departments_df = extract.read_cloud_storage('data-engineering-python','retail/departments')
order_items_df = extract.read_cloud_storage('data-engineering-python','retail/order_items')
orders_df      = extract.read_cloud_storage('data-engineering-python','retail/orders')
products_df    = extract.read_mongo_db('retail_db','products')

print("FIN EXTRACCION")

print("INICIA CARGA A CAPA LANDING")

load.load_to_cloud_storage('data-engineering-python','landing/categories',categories_df)
load.load_to_cloud_storage('data-engineering-python','landing/customers',customers_df)
load.load_to_cloud_storage('data-engineering-python','landing/departments',departments_df)
load.load_to_cloud_storage('data-engineering-python','landing/order_items',order_items_df)
load.load_to_cloud_storage('data-engineering-python','landing/orders',orders_df)
load.load_to_cloud_storage('data-engineering-python','landing/products',products_df)

print("FIN CARGA A CAPA LANDING")




