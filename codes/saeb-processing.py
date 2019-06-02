'''
Big Data Studies
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.4.3
      /_/


@claudio alves monteiro
maio/2019
'''

#==========================================#
# IMPORT MODULES AND CONFIG ENVIRONMENT
#========================================#

# import modules
import os
from time import time

# paths to spark and python3
os.environ['PYSPARK_SUBMIT_ARGS'] = '--executor-memory 1G pyspark-shell'
os.environ["SPARK_HOME"] = "/home/pacha/spark"
os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"

# execute PYSPARK
exec(open('/home/pacha/spark/python/pyspark/shell.py').read())
# UI: 127.0.0.1:4040

#=========================#
# IMPORT DATA
#========================#

# return list of files in directory
fl = os.listdir()
fl

# list of columns to be selected
cols = ['NU_DURACAO_TURMA', 'TP_SEXO', 'TP_COR_RACA', 'CO_MUNICIPIO_END', 'TP_DEPENDENCIA']

t0 = time() # time of import

# import files, select and combine data
key = True
for file in fl:
    if file.endswith(".CSV"):
        if key == True:
            df = spark.read.csv(file, header=True, inferSchema=True, sep='|')
            df = df.select(cols)
            key = False
        else:
            x = spark.read.csv(file, header=True, inferSchema=True, sep='|')
            x = x.select(cols)
            df = df.union(x)

tt = time() - t0

print('Task 1 performed in {} minutes'.format(tt/60))

df.printSchema()