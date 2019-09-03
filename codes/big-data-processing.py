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
import time 
import pandas as pd

# execute PYSPARK
exec(open('/home/pacha/spark/python/pyspark/shell.py').read())
# UI: 127.0.0.1:4040

#===========================#
# IMPORT AND CONCAT DATA
#==========================#

# import functions
import pyspark.sql.functions as SF

#---------- define columns for selections
col_turma = ['ID_TURMA','NO_TURMA','NU_ANO_CENSO','CO_MUNICIPIO','CO_UF','TP_DEPENDENCIA','NU_DURACAO_TURMA']

col_escola = ['CO_ENTIDADE','NO_ENTIDADE','DT_ANO_LETIVO_INICIO','CO_MUNICIPIO','TP_LOCALIZACAO',
              'TP_DEPENDENCIA','IN_LOCAL_FUNC_UNID_PRISIONAL','IN_LOCAL_FUNC_PRISIONAL_SOCIO','IN_AGUA_FILTRADA',
              'IN_AGUA_INEXISTENTE','IN_ENERGIA_INEXISTENTE','IN_ESGOTO_INEXISTENTE','IN_LIXO_RECICLA',
              'IN_LABORATORIO_CIENCIAS','IN_LABORATORIO_INFORMATICA','IN_QUADRA_ESPORTES','IN_BIBLIOTECA_SALA_LEITURA',
              'IN_BANHEIRO_DENTRO_PREDIO','IN_BANHEIRO_CHUVEIRO','IN_AREA_VERDE','IN_COMPUTADOR','IN_INTERNET','IN_ALIMENTACAO'
              #'QT_COMP_ALUNO','QT_FUNCIONARIOS','QT_SALAS_EXISTENTES', 'QT_SALAS_UTILIZADAS' 
            ]

col_nota = 'ID_MUNICIPIO ID_AREA ID_ESCOLA ID_DEPENDENCIA_ADM ID_LOCALIZACAO ID_TURMA ID_TURNO ID_SERIE ID_ALUNO PROFICIENCIA_MT'.split(' ')


##------------------ function for loading
def csvCombiner(strdata, filename, sep, cols, anoinit, anoend):
    ''' Concatenate each dataset  with the same name
        located in folders and subfolders
    '''
    # load SF
    import pyspark.sql.functions as SF
    # define variables
    camino = 'data/' + strdata  + '/'
    paths = next(os.walk(camino))[1]
    key = True
    anos = [str(x) for x in range(anoinit, anoend+1)]
    # verify data
    if strdata == 'censo-escolar':
        column = 'CO_MUNICIPIO'
    elif strdata == 'saeb':
        column = 'ID_MUNICIPIO'
    # loop import
    for i in anos:
        listin = os.listdir((camino + i))
        for file in listin:
            if filename in file:
                print(file)
                imp = camino + i + '/' + file
                print(imp)
                if key == True:
                    df = spark.read.csv(imp, header=True, inferSchema=True, sep=sep)
                    df = df.filter(df[column] == 2611606) ### SELECT RECIFE
                    df = df.select(cols)
                    df = df.withColumn('ano', SF.lit(i))
                    key = False
                else:
                    x = spark.read.csv(imp, header=True, inferSchema=True, sep=sep)
                    x = x.filter(x[column] == 2611606) ### SELECT RECIFE
                    x = x.select(cols)
                    x = x.withColumn('ano', SF.lit(i))
                    df = df.union(x)
    return df

#----------- CENSO ESCOLAR [ESCOLAR] -------------#

# generate data
start_time = time.time()
censo_esc_rec = csvCombiner('censo-escolar', 'ESCOLA', '|', col_escola, 2015, 2018)
print("--- %s seconds ---" % (time.time() - start_time))

# save data
pd1 = censo_esc_rec.toPandas()
pd1.to_csv('results/data/censo_esc_rec.csv')

#--------- CENSO ESCOLAR [TURMA] ----------#

# generate data
censo_turma = csvCombiner('censo-escolar', 'TURMA', '|', col_turma, 2015, 2018)

#------------- ANEB ---------------#

# generate data and verify time of execution
start_time = time.time()
aneb_rec = csvCombiner('saeb', 'TS_ALUNO', ',', col_nota, 2013, 2017)
print("--- %s seconds ---" % (time.time() - start_time))

# save data
pd2 = aneb_rec.toPandas()
pd2.to_csv('results/data/aneb_rec.csv')


#============== COMBINE DATA ===============#

# contar numero de casos por base
print((censo_esc.count(), len(censo_esc.columns)))
print((notas.count(), len(notas.columns)))

# visualizar code infos
notas.select('ID_ESCOLA').show(100)
censo_esc.select('CO_ENTIDADE').show(100)

# renomear coluna
censo_esc = censo_esc.withColumnRenamed('CO_ENTIDADE', 'ID_ESCOLA')

# join data
mfDF = censo_esc.join(notas, ['ID_ESCOLA'])

# selec recife and save
recmf = mfDF.filter(mfDF.CO_MUNICIPIO == 2611606)

recdf = recmf.toPandas()
recdf.to_csv('rec_big.csv', index = False)










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
