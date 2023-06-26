from process.Extract import Extract
from process.Load import Load
#from process.Transform import Trans
#form from process.Load import Load
import pandas as pd

extract = Extract()
load    = Load()

order_items_df = extract.read_cloud_storage('data-engineering-python','retail/order_items')

load.load_to_cloud_storage('data-engineering-python','landing/order_items',order_items_df)

order_items_df = extract.read_cloud_storage('data-engineering-python','landing/order_items')

