'''
VM

'''

#================#
# Funcoes
#================#

# funcao para selecionar dados a partir dos nomes das colunas
def searchAndString(dt, colunas):
    ''' procura valor das colunas em cada linha a partir
        do id da respectiva coluna. Armazena infos em string CSV.
    '''
    strData = ''
    for linha in dt:
        row = linha.split('|')
        for col in colunas[0:(len(colunas)-1)]:
            id = head.index(col)
            strData += row[id] + ','
        id = head.index(colunas[len(colunas)-1])
        strData += row[id]
        strData += '\n'
    return(strData)

# funcao para salvar
def writeArq(string):
    file = open('matriculas_co.csv', 'w')
    file.write(string)
    file.close()

#===================#
# dados e colunas
#===================#

# chave de execucao
key = True

if key == True:
    dt = open('ESCOLAS.CSV', 'r', encoding = "ISO-8859-1")
    colunas = ['TP_LOCALIZACAO','NU_ANO_CENSO','CO_REGIAO','CO_UF', 'CO_MUNICIPIO','CO_ENTIDADE', 'NO_ENTIDADE',
                'IN_CONVENIADA_PP','IN_BANHEIRO_PNE','IN_DEPENDENCIAS_PNE','IN_SALA_ATENDIMENTO_ESPECIAL',
                'IN_AGUA_FILTRADA', 'IN_AGUA_INEXISTENTE','IN_ENERGIA_INEXISTENTE','IN_ESGOTO_INEXISTENTE']
else:
    dt = open('MATRICULA_CO.CSV', 'r')
    colunas = ['TP_ETAPA_AGREGADA']


dt = dt.readlines()
head = dt[0]
head = head.split('|')

#=========================#
# executar processamento
#========================#

# capturar dados
strData = searchAndString(dt, colunas)

# visualizar 1000 primeiros carcteres
print(strData[1:1000])

# escrever no arquivo
writeArq(strData)
