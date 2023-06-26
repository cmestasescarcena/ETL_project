from process.Extract import Extract
from process.Load import Load
from process.Transform import Transform
import pandas as pd



extract = Extract()
load    = Load()
transform = Transform()

customers_df   = extract.read_cloud_storage('data-engineering-python','landing/customers')
orders_df      = extract.read_cloud_storage('data-engineering-python','landing/orders')
order_items_df = extract.read_cloud_storage('data-engineering-python','landing/order_items')

df3 = customers_df.copy()
df4 = orders_df.copy()
df5 = order_items_df.copy()
df_enunciado2    = transform.enunciado2(customers_df, orders_df, order_items_df)

load.load_to_cloud_storage('data-engineering-python','gold/df_enunciado2',df_enunciado2)

