from process.Extract import Extract
from process.Load import Load
import pandas as pd
import sys

extract = Extract()
load    = Load()

statment     = sys.argv[1]

def statment01():
    df_enunciado1 = extract.read_cloud_storage('data-engineering-python','gold/df_enunciado1')
    load.load_to_mysql('gold', 'enunciado1', df_enunciado1)  
    pass
    
def statment02():
    df_enunciado2 = extract.read_cloud_storage('data-engineering-python','gold/df_enunciado2')
    load.load_to_mysql('gold', 'enunciado2', df_enunciado2)
    pass

func_dict = {
    "statment01": statment01,
    "statment02": statment02
}

func_dict[statment]()

