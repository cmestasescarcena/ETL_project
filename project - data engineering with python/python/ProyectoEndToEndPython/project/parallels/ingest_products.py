from process.Extract import Extract
from process.Load import Load
#from process.Transform import Trans
#form from process.Load import Load
import pandas as pd

extract = Extract()
load    = Load()

products_df    = extract.read_mongo_db('retail_db','products')

load.load_to_cloud_storage('data-engineering-python','landing/products',products_df)

products_df    = extract.read_cloud_storage('data-engineering-python','landing/products')

