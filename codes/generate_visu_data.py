'''
DIAGJUV

@ claudioalvesmonteiro 2019
'''

# importar pacotes
import pandas as pd

# importar dados
df = pd.read_csv('results/data/censo_esc_rec.csv')

# definir colunas a serem calculadas
colunas = ['IN_AGUA_FILTRADA', 'IN_AGUA_INEXISTENTE', 'IN_ENERGIA_INEXISTENTE',
            'IN_ESGOTO_INEXISTENTE', 'IN_LIXO_RECICLA', 'IN_LABORATORIO_CIENCIAS',
            'IN_LABORATORIO_INFORMATICA', 'IN_QUADRA_ESPORTES',
            'IN_BIBLIOTECA_SALA_LEITURA', 'IN_BANHEIRO_DENTRO_PREDIO',
            'IN_BANHEIRO_CHUVEIRO', 'IN_AREA_VERDE', 'IN_COMPUTADOR', 'IN_INTERNET',
            'IN_ALIMENTACAO'
            ]


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