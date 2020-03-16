'''
DIAGJUV

@ claudioalvesmonteiro 2019
'''

# importar pacotes
import pandas as pd

# importar dados
df = pd.read_csv('results/data/censo_esc_rec.csv')

# contagem do total [minimo1 X todos os equipamentos]
coluna = [0]*len(df)
sumcols =  ['IN_ALIMENTACAO','IN_AREA_VERDE','IN_BIBLIOTECA_SALA_LEITURA',
            'IN_COMPUTADOR','IN_INTERNET', 'IN_LABORATORIO_CIENCIAS',
            'IN_LABORATORIO_INFORMATICA', 'IN_QUADRA_ESPORTES']

for i in sumcols:
    colsum = df[i].fillna(0)
    coluna = coluna + colsum


df['cont_infra'] = coluna
df['MINIMO1_INFRA'] = [1 if x != 0 else 0 for x in df['cont_infra']]
df['TODOS_INFRA'] = [1 if x == 5 else 0 for x in df['cont_infra']]


# definir colunas a serem calculadas
colunas = ['IN_AGUA_FILTRADA', 'IN_AGUA_INEXISTENTE', 'IN_ENERGIA_INEXISTENTE',
            'IN_ESGOTO_INEXISTENTE', 'IN_LIXO_RECICLA', 'IN_LABORATORIO_CIENCIAS',
            'IN_LABORATORIO_INFORMATICA', 'IN_QUADRA_ESPORTES',
            'IN_BIBLIOTECA_SALA_LEITURA', 'IN_BANHEIRO_DENTRO_PREDIO',
            'IN_BANHEIRO_CHUVEIRO', 'IN_AREA_VERDE', 'IN_COMPUTADOR', 'IN_INTERNET',
            'IN_ALIMENTACAO', 'MINIMO1_INFRA', 'TODOS_INFRA'
            ]

# GERAR PROP ESPECIFICAS
for col in colunas:
    # selecionar coluna
    coldt = df[[col, 'ano']]
    # agrupamento
    x = pd.DataFrame(coldt[[col, 'ano']].groupby('ano').sum()).reset_index()
    y = pd.DataFrame(coldt[[col, 'ano']].groupby('ano').size()).reset_index()
    # combinar dados e renomear colunas
    grupodt = x.merge(y)
    grupodt.columns = ['ano', 'contagem', 'total'] 
    # calcular proporcao
    grupodt['proporcao'] = round((grupodt['contagem']/grupodt['total'])*100, 2)
    # salvar base
    grupodt.to_csv('results/tables/data_'+col+'.csv', index=False)


# contagem e proporcao por ano