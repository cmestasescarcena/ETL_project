from process.Extract import Extract
from process.Load import Load
#from process.Transform import Trans
#form from process.Load import Load
import pandas as pd

extract = Extract()
load    = Load()

customers_df   = extract.read_cloud_storage('data-engineering-python','retail/customers')

load.load_to_cloud_storage('data-engineering-python','landing/customers',customers_df)

customers_df   = extract.read_cloud_storage('data-engineering-python','landing/customers')

