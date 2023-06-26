from process.Extract import Extract
from process.Load import Load
from process.Transform import Transform
import pandas as pd



extract = Extract()
load    = Load()
transform = Transform()

order_items_df = extract.read_cloud_storage('data-engineering-python','landing/order_items')
orders_df      = extract.read_cloud_storage('data-engineering-python','landing/orders')

df1 = orders_df.copy()
df2 = order_items_df.copy()
df_enunciado1    = transform.enunciado1(df1, df2)

load.load_to_cloud_storage('data-engineering-python','gold/df_enunciado1',df_enunciado1)

