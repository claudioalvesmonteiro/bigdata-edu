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
import pandas as pd
import numpy as np
import plotly.offline as py
import cufflinks as cf

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

# define columns for selections
col_turma = ['ID_TURMA','NO_TURMA','NU_ANO_CENSO','CO_MUNICIPIO','CO_UF','TP_DEPENDENCIA','NU_DURACAO_TURMA']

col_escola = ['CO_ENTIDADE','NO_ENTIDADE','DT_ANO_LETIVO_INICIO','CO_MUNICIPIO','TP_LOCALIZACAO',
              'TP_DEPENDENCIA','IN_LOCAL_FUNC_UNID_PRISIONAL','IN_LOCAL_FUNC_PRISIONAL_SOCIO','IN_AGUA_FILTRADA',
              'IN_AGUA_INEXISTENTE','IN_ENERGIA_INEXISTENTE','IN_ESGOTO_INEXISTENTE','IN_LIXO_RECICLA',
              'IN_LABORATORIO_CIENCIAS','IN_LABORATORIO_INFORMATICA','IN_QUADRA_ESPORTES','IN_BIBLIOTECA_SALA_LEITURA',
              'IN_BANHEIRO_DENTRO_PREDIO','IN_BANHEIRO_CHUVEIRO','IN_AREA_VERDE','IN_COMPUTADOR','IN_INTERNET','IN_ALIMENTACAO'
              #'QT_COMP_ALUNO','QT_FUNCIONARIOS','QT_SALAS_EXISTENTES', 'QT_SALAS_UTILIZADAS'
              # 
            ]

## function
def csvCombiner(strdata, filename, sep, cols):
    ''' Concatenate each dataset  with the same name
        located in folders and subfolders
    '''
    camino = 'data/' + strdata  + '/'
    paths = next(os.walk(camino))[1]
    key = True
    for i in paths:
        if i == '2018' or i == '2017' or i == '2015': # AJEITAR MATCH DE COLUNAS
            listin = os.listdir((camino + i))
            for file in listin:
                if filename in file:
                    imp = camino + i + '/' + file
                    if key == True:
                        df = spark.read.csv(imp, header=True, inferSchema=True, sep=sep)
                        df = df.select(cols)
                        key = False
                    else:
                        x = spark.read.csv(imp, header=True, inferSchema=True, sep=sep)
                        x = x.select(cols)
                        df = df.union(x)
    return df

#---- combinar dados ESCOLA
censo_esc = csvCombiner('censo-escolar', 'ESCOLA', '|', col_escola)

#---- combinar dados TURMA
#alunoSAEB = csvCombiner('censo-escolar', 'TURMA', '|', col_turma)

#---- combinar dados SAEB

#===========================#
# Exploratory Analysis
#==========================#

# Group by city
colsel = ['IN_AGUA_FILTRADA','IN_AGUA_INEXISTENTE',
        #'IN_ENERGIA_INEXISTENTE','IN_ESGOTO_INEXISTENTE',
       # 'IN_LABORATORIO_CIENCIAS','IN_LABORATORIO_INFORMATICA',
        'IN_QUADRA_ESPORTES','IN_BIBLIOTECA_SALA_LEITURA',
       # 'IN_BANHEIRO_DENTRO_PREDIO','IN_BANHEIRO_CHUVEIRO',
        'IN_INTERNET', 'CO_MUNICIPIO']


munidata = censo_esc.select(colsel).groupby('CO_MUNICIPIO').sum()
munidata.show(10)

# renomear colunas
newColumns = ['code_muni'] + [x.lower() for x in colsel]

muni = munidata.toDF(*newColumns)
muni.show(10)

# select data from city
#city = censo_esc.filter(censo_esc.CO_MUNICIPIO == 3158953)


'''
x = spark.read.csv('data/censo-escolar/2018/ESCOLAS.CSV', header=True, inferSchema=True, sep='|')
x = x.select(col_escola)

y = spark.read.csv('data/censo-escolar/2017/ESCOLAS.CSV', header=True, inferSchema=True, sep='|')
y = y.select(col_escola)

w = spark.read.csv('data/censo-escolar/2015/ESCOLAS.CSV', header=True, inferSchema=True, sep='|')
w = w.select(col_escola)

a = x.union(y)
a = a.union(w)

2014 PARA BAIXO: VERIFICAR MATCH DE COLUNAS
h = spark.read.csv('data/censo-escolar/2014/ESCOLAS.CSV', header=True, inferSchema=True, sep='|')
h = h.select(col_escola)

j = spark.read.csv('data/censo-escolar/2013/ESCOLAS.CSV', header=True, inferSchema=True, sep='|')
j = j.select(col_escola)
'''
