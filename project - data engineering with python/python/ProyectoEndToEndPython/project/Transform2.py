from process.Extract import Extract
from process.Load import Load
from process.Transform import Transform
import pandas as pd
import sys

extract = Extract()
load    = Load()
transform = Transform()

statment     = sys.argv[1]

def statment01():
    order_items_df = extract.read_cloud_storage('data-engineering-python','landing/order_items')
    orders_df      = extract.read_cloud_storage('data-engineering-python','landing/orders')    
    df1 = orders_df.copy()
    df2 = order_items_df.copy()
    df_enunciado1    = transform.enunciado1(df1, df2)    
    load.load_to_cloud_storage('data-engineering-python','gold/df_enunciado1',df_enunciado1)    
    pass
    
def statment02():
    customers_df   = extract.read_cloud_storage('data-engineering-python','landing/customers')
    orders_df      = extract.read_cloud_storage('data-engineering-python','landing/orders')
    order_items_df = extract.read_cloud_storage('data-engineering-python','landing/order_items')
    df3 = customers_df.copy()
    df4 = orders_df.copy()
    df5 = order_items_df.copy()
    df_enunciado2    = transform.enunciado2(customers_df, orders_df, order_items_df)
    load.load_to_cloud_storage('data-engineering-python','gold/df_enunciado2',df_enunciado2)
    pass

func_dict = {
    "statment01": statment01,
    "statment02": statment02
}

func_dict[statment]()




