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
next(os.walk('.'))[1]

fl = os.listdir('data/saeb/2017')
fl

# list of columns to be selected
cols2 = ['ID_ALUNO','IN_SITUACAO_CENSO', 'ID_ESCOLA', 'ID_TURMA', 'ID_LOCALIZACAO', 'ID_SERIE']

# import data
t0 = time() 
df2 = spark.read.csv('data/saeb/2017/TS_ALUNO_5EF.csv', header=True, inferSchema=True, sep=',')
tt = time() - t0

print('Task 1 performed in {} minutes'.format(tt/60))

# print schema
df.printSchema()

# codes identifiers
saeb_ident = df2.select(cols2)
saeb_ident.select('ID_ALUNO').show(300000)
saeb_ident.printSchema()

city = cit.join(brdf, ['code_muni2'])