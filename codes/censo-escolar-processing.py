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
# port: 127.0.0.1:4040

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

#===============================#
# DATA STRUCTURE OPERATIONS
#==============================#


#--------- SAMPLE
t0 = time() 
sdf = spark.createDataFrame(df.rdd.takeSample(False, 500000, seed=0))
tt = time() - t0

print('Task 2 performed in {} minutes'.format(tt/60))

#--------- SELECT
sdf.select('TP_COR_RACA', 'TP_SEXO').show(10)

#--------- FILTER
sRec = sdf.filter(sdf.CO_MUNICIPIO_END == 2611606)
sRec.show(10)

#--------- MUTATE [create column]
sdf = sdf.withColumn('DURACAO_TURMA_H', sdf.NU_DURACAO_TURMA/60)
sdf.printSchema()

#-------- GROUP BY

# call group 
gsdf = sdf.groupby('TP_COR_RACA')

# call average function
gsdf.avg('NU_DURACAO_TURMA').show() 

# call average function
gsdf.avg('NU_DURACAO_TURMA','TP_SEXO').show() 

#------- MULTIPLE GROUP BY

# to call multiple agreg functions at once, pass a dictionary
gsdf.agg({'*': 'count', 'NU_DURACAO_TURMA': 'avg', 'TP_SEXO': 'sum'}).show()

# renaming agreg columns 
(
gsdf.agg({'*': 'count', 'NU_DURACAO_TURMA': 'avg', 'TP_SEXO': 'sum'})
    .toDF('Raca', 'count', 'average_duracao_turma', 'total_sex')
    .show()
)

# save renamed agreg
cit = (
    sdf.groupby('CO_MUNICIPIO_END').agg({'*': 'count', 'NU_DURACAO_TURMA': 'avg'})
        .toDF('code_muni2', 'count_alunos', 'avg_duracao_turma')
)

cit.show(20)

#------- SORT ROWS
cit.sort('NU_DURACAO_TURMA', ascending = False).show(10)

#------ JOIN [merge]

# read file for merge
brdf = spark.read.csv('dados/BR_muni_code.csv', header=True, inferSchema=True)
brdf.printSchema()

# join
city = cit.join(brdf, ['code_muni2'])