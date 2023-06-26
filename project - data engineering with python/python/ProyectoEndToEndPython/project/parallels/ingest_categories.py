from process.Extract import Extract
from process.Load import Load
#from process.Transform import Trans
#form from process.Load import Load
import pandas as pd

extract = Extract()
load    = Load()

categories_df = extract.read_mysql('retail_db', 'categories')

load.load_to_cloud_storage('data-engineering-python','landing/categories',categories_df)

categories_df  = extract.read_cloud_storage('data-engineering-python','landing/categories')



