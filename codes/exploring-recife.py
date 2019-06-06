'''
Big Data Studies
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.4.3
      /_/

@claudio alves monteiro
junho/2019
'''

# import modules
import pandas as pd 
import seaborn as sns 
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split

sns.set_style('whitegrid')

# import data
df = pd.read_csv('rec_data.csv')

#==================================
# Pre-processing
#==================================

# visualizae missing data
sns.heatmap(df.isnull(), yticklabels=False, cbar=False, cmap='viridis')
plt.tight_layout()
plt.gcf().subplots_adjust(bottom=0.15)
plt.show()

# drop na values
df[['IN_AGUA_FILTRADA', 'IN_AGUA_INEXISTENTE',
    'IN_LABORATORIO_INFORMATICA', 'IN_QUADRA_ESPORTES',
    'IN_BIBLIOTECA_SALA_LEITURA' ,  'IN_INTERNET',
    'PROFICIENCIA_MT']].dropna(inplace=True)


#==================================
# MACHINE LEARNING
#=================================

# select TARGET [variavel dependente]
features = ['IN_AGUA_FILTRADA', 'IN_AGUA_INEXISTENTE', 
           # 'IN_ENERGIA_INEXISTENTE','IN_ESGOTO_INEXISTENTE', 
           # 'IN_LIXO_RECICLA', 'IN_LABORATORIO_CIENCIAS',
             'IN_LABORATORIO_INFORMATICA', 'IN_QUADRA_ESPORTES',
             'IN_BIBLIOTECA_SALA_LEITURA', #'IN_BANHEIRO_DENTRO_PREDIO',
           # 'IN_BANHEIRO_CHUVEIRO', 'IN_AREA_VERDE', 
             'IN_INTERNET' # 'IN_COMPUTADOR'
          ]

# select FEATURES [variaveis independentes]
target = ['PROFICIENCIA_MT']

# SPLIT in trainers and testers
f_train, f_test, t_train, t_test = train_test_split(features, target, test_size=0.1, random_state=101)