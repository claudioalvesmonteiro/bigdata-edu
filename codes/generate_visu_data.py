'''
DIAGJUV

@ claudioalvesmonteiro 2019
'''

# importar pacotes
import pandas as pd

#************** loop para cada coluna

# import data
df = pd.read_csv('results/data/censo_esc_rec.csv')

# selecionar coluna
coldt = df[['IN_INTERNET', 'ano']]

# agrupamento
x = pd.DataFrame(coldt[['IN_INTERNET', 'ano']].groupby('ano').sum()).reset_index()
y = pd.DataFrame(coldt[['IN_INTERNET', 'ano']].groupby('ano').size()).reset_index()

# combinar dados e renomear colunas
grupodt = x.merge(y)
grupodt.columns = ['ano', 'IN_INTERNET', 'valor'] 

# calcular proporcao
grupodt['proporcao'] = round((grupodt['IN_INTERNET']/grupodt['valor'])*100, 2)

print(grupodt)
