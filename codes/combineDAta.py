'''
Big Data Studies
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.4.3
      /_/

@claudio alves monteiro
maio/2019


IMPORT TURMA AND ESCOLA DATA 
TO MERGE WITH SAEB

ESCOLA_CENSO, TURMA_CENSO[*se tem todas as series], ALUNO_SAEB -> NOTA ALUNO

Linear
Decision Tree
Random Forest
Hierarquical Modelling
'''

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

'''
# import and select data
df = spark.read.csv('data/censo-escolar/2017/MATRICULA_SUL.CSV', header=True, inferSchema=True, sep='|')
df = df.select('NU_DURACAO_TURMA','NU_ANO_CENSO','ID_MATRICULA','CO_PESSOA_FISICA','CO_MUNICIPIO','ID_TURMA','TP_LOCALIZACAO', 'TP_SEXO', 'TP_COR_RACA', 'CO_MUNICIPIO_END', 'TP_DEPENDENCIA')

df2 = spark.read.csv('data/saeb/2017/TS_ALUNO_5EF.csv', header=True, inferSchema=True, sep=',')
df2.printSchema()
df2 = df2.select('ID_ALUNO','IN_SITUACAO_CENSO', 'ID_ESCOLA', 'ID_TURMA', 'ID_LOCALIZACAO', 'ID_SERIE')
'''

#==========================
# IMPORT AND CONCAT DATA
#========================

def csvCombiner(strdata, filename, sep):
    ''' Concatenate each dataset  with the same name
        located in folders and subfolders
    '''
    camino = 'data/' + strdata  + '/'
    paths = next(os.walk(camino))[1]
    key = True
    for i in paths[:-1]:
        listin = os.listdir((camino + i))
        for file in listin:
            if filename in file:
                imp = camino + i + '/' + file
                if key == True:
                    df = spark.read.csv(imp, header=True, inferSchema=True, sep=sep)
                    # df = df.select(cols)
                    key = False
                else:
                    x = spark.read.csv(imp, header=True, inferSchema=True, sep=sep)
                    #x = x.select(cols)
                    df = df.union(x)
    return df

turmaDATA = csvCombiner('censo-escolar', 'TURMA', '|')
alunoSAEB = csvCombiner('censo-escolar', 'TURMA', '|')
