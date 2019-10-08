'''
Big Data Studies

@claudio alves monteiro
junho/2019
'''

# import modules
import numpy as np
import pandas as pd 
import seaborn as sns 
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split

sns.set_style('whitegrid')

# import data
df = pd.read_csv('results/data/censo_esc_rec.csv')

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
    'PROFICIENCIA_MT_SAEB']].dropna(inplace=True)

# drop NaN values
df = df[np.isfinite(df['PROFICIENCIA_MT_SAEB'])]

#==================================
# MACHINE LEARNING
#=================================
# https://twiecki.io/blog/2017/02/08/bayesian-hierchical-non-centered/

# select TARGET [variavel dependente]
features = df[['IN_AGUA_FILTRADA', 'IN_AGUA_INEXISTENTE', 
           # 'IN_ENERGIA_INEXISTENTE','IN_ESGOTO_INEXISTENTE', 
           # 'IN_LIXO_RECICLA', 'IN_LABORATORIO_CIENCIAS',
             'IN_LABORATORIO_INFORMATICA', 'IN_QUADRA_ESPORTES',
             'IN_BIBLIOTECA_SALA_LEITURA', #'IN_BANHEIRO_DENTRO_PREDIO',
           # 'IN_BANHEIRO_CHUVEIRO', 'IN_AREA_VERDE', 
             'IN_INTERNET' # 'IN_COMPUTADOR'
          ]]


features = features.astype(float)
#features = features.astype(int64)
#pd.get_dummies(target)

# select FEATURES [variaveis independentes]
target = df['PROFICIENCIA_MT_SAEB']
#target = target.astype(np.int64)

# SPLIT in trainers and testers
f_train, f_test, t_train, t_test = train_test_split(features, target, test_size=0.1, random_state=101)

# import algoritm 
from sklearn.linear_model import LinearRegression

# create a linear model object
lm = LinearRegression()

# fit model to training data
lm.fit(f_train, t_train)

# visualization on coefficients
pd.DataFrame(lm.coef_, features.columns, columns=['Coeff'])

# predict values of target [house value] based on features
predictions= lm.predict(f_test)
predictions

# test how far are predictions from actual observations
from sklearn.metrics import r2_score
r2_score(t_test, predictions)

#===================================
# build statistical visualization
#=================================

# importar pacote stats
from scipy import stats

# parameters and predicitons of model
params = np.append(lm.intercept_,lm.coef_)
predictions = lm.predict(f_train)

# make model data
newX = pd.DataFrame({"Constant":np.ones(len(f_train))})

f_train['index'] = np.arange(0, len(f_train))
f_train.set_index('index', inplace=True)

newX = pd.concat([newX, f_train], axis=1)

MSE = (sum((t_train-predictions)**2))/(len(newX)-len(newX.columns))

# Note if you don't want to use a DataFrame replace the two lines above with
# newX = np.append(np.ones((len(X),1)), X, axis=1)
# MSE = (sum((y-predictions)**2))/(len(newX)-len(newX[0]))

var_b = MSE*(np.linalg.inv(np.dot(newX.T,newX)).diagonal())
sd_b = np.sqrt(var_b)
ts_b = params/ sd_b

p_values =[2*(1-stats.t.cdf(np.abs(i),(len(newX)-1))) for i in ts_b]

sd_b = np.round(sd_b,3)
ts_b = np.round(ts_b,3)
p_values = np.round(p_values,3)
params = np.round(params,4)

myDF3 = pd.DataFrame()
myDF3["Coefficients"],myDF3["Standard Errors"],myDF3["t values"],myDF3["Probabilites"] = [params,sd_b,ts_b,p_values]
print(myDF3)