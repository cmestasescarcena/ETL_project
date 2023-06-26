from process.Extract import Extract
from process.Load import Load
from process.Transform import Transform
import pandas as pd



extract = Extract()
load    = Load()
transform = Transform()

df1 = orders_df.copy()
df2 = order_items_df.copy()
df_enunciado1    = transform.enunciado1(df1, df2)