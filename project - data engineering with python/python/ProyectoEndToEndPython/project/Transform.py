from process.Extract import Extract
from process.Load import Load
from process.Transform import Transform
import pandas as pd



extract = Extract()
load    = Load()
transform = Transform()

print("INICIA CARGA DE CAPA LANDING")

categories_df  = extract.read_cloud_storage('data-engineering-python','landing/categories')
customers_df   = extract.read_cloud_storage('data-engineering-python','landing/customers')
departments_df = extract.read_cloud_storage('data-engineering-python','landing/departments')
order_items_df = extract.read_cloud_storage('data-engineering-python','landing/order_items')
orders_df      = extract.read_cloud_storage('data-engineering-python','landing/orders')
products_df    = extract.read_cloud_storage('data-engineering-python','landing/products')

print("FIN CARGA DE CAPA LANDING")

print("INICIA TRANSFORMACIONES")

df1 = orders_df.copy()
df2 = order_items_df.copy()
df_enunciado1    = transform.enunciado1(df1, df2)

df3 = customers_df.copy()
df4 = orders_df.copy()
df5 = order_items_df.copy()
df_enunciado2    = transform.enunciado2(customers_df, orders_df, order_items_df)

print("FIN TRANSFORMACIONES")

print("INICIA CARGA A CLOUD STORAGE CAPA GOLD")

load.load_to_cloud_storage('data-engineering-python','gold/df_enunciado1',df_enunciado1)
load.load_to_cloud_storage('data-engineering-python','gold/df_enunciado2',df_enunciado2)

print("FIN CARGA A CLOUD STORAGE CAPA GOLD")

print("INICIA CARGA MYSQL")

load.load_to_mysql('gold', 'enunciado1', df_enunciado1)
load.load_to_mysql('gold', 'enunciado2', df_enunciado2)

print("FIN CARGA MYSQL")
