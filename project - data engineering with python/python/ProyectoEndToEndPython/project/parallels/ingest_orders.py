from process.Extract import Extract
from process.Load import Load
#from process.Transform import Trans
#form from process.Load import Load
import pandas as pd

extract = Extract()
load    = Load()

orders_df      = extract.read_cloud_storage('data-engineering-python','retail/orders')

load.load_to_cloud_storage('data-engineering-python','landing/orders',orders_df)

orders_df      = extract.read_cloud_storage('data-engineering-python','landing/orders')