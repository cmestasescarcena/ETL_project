import pandas as pd
from pandasql import sqldf
#import numpy as np

pysqldf = lambda q: sqldf(q, locals())

class Transform():

    def __init__(self) -> None:
        self.process = 'Transform Process'

    def enunciado1(self, _orders_df, _order_items_df):
        # Modificar la fecha para guardar solamente año  
     
        orders_df_per_year_month = _orders_df
        orders_df_per_year_month['year']  = pd.DatetimeIndex(orders_df_per_year_month['order_date']).year
        orders_df_per_year_month['month'] = pd.DatetimeIndex(orders_df_per_year_month['order_date']).month 
        # Join con la tabla de detalle
        df_join = _order_items_df.merge(orders_df_per_year_month, left_on = 'order_item_order_id', right_on = 'order_id')
        # Agrupar por fecha de orden y sumar los montos de venta
        df = df_join.groupby(['year','month'])['order_item_subtotal'].sum().reset_index()
        df['date_time'] = pd.Timestamp("now")
        return df
        
    def enunciado2(self, customer, orders, items):
        q = """
                SELECT
                    customer_id, customer_fname, customer_lname, customer_email, sum(order_item_quantity) as quantity_item_total, sum(order_item_subtotal)as total
                FROM
                    customer as c
                INNER JOIN
                    orders as o
                    ON c.customer_id = o.order_customer_id
                INNER JOIN
                    items as oi
                    ON o.order_id = oi.order_item_order_id
                WHERE order_status <> 'CANCELED'
                GROUP BY customer_id, customer_fname, customer_lname, customer_email
                ORDER BY  total DESC
                LIMIT 20
            """
        result = sqldf(q)

        return result