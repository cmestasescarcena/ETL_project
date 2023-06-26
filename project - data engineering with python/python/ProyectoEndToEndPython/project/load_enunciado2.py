from process.Extract import Extract
from process.Load import Load
from process.Transform import Transform
import pandas as pd



extract = Extract()
load    = Load()
transform = Transform()


df_enunciado2 = extract.read_cloud_storage('data-engineering-python','gold/df_enunciado2')

load.load_to_mysql('gold', 'enunciado2', df_enunciado2)