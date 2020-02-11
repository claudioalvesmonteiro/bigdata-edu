'''
@claudio alves monteiro
2020
'''

# import modules
import pandas as pd

#===========================#
# IMPORT AND CONCAT DATA
#==========================#

# import functions
import pyspark.sql.functions as SF

bases = ['2015', '2014']

#def fileGenerator():
for base in bases:
    print('Inicializando processamento da base MICRODADOS_ENEM_'+base)
    if base == '2015' or base == '2014':
        df = spark.read.csv('C:/Users/DELL/Documents/Bases de Dados/enem'+base+'/MICRODADOS_ENEM_'+base+'.CSV', header=True, sep=',')
    else:
        df = spark.read.csv('C:/Users/DELL/Documents/Bases de Dados/enem'+base+'/MICRODADOS_ENEM_'+base+'.CSV', header=True, sep=';')
    # selecionar recife
    df = df.filter(df['NO_MUNICIPIO_RESIDENCIA'] == 'Recife')
    # selecionar variaveis de interesse
    df = df.select('NU_ANO',
                'NU_IDADE', 
                'TP_SEXO', 
                'TP_COR_RACA', 
                'CO_ESCOLA',
                'NU_NOTA_CN',
                'NU_NOTA_CH' ,
                'NU_NOTA_LC', 
                'NU_NOTA_MT',
                'TX_RESPOSTAS_CN',  
                'TX_RESPOSTAS_CH', 
                'TX_RESPOSTAS_LC', 
                'TX_RESPOSTAS_MT')
    # salvar base tratada
    data = df.toPandas()
    data.to_csv('C:/Users/DELL/Documents/Consultorias/DIAGJUV/bigdata-edu/data/enem/ENEM_'+base+'_recife.csv')
    print('base ENEM_'+base+' salva')
