#================================================
# DIAGJUV
# Secretaria Executiva de Juventude do Recife
#
# @claudioalvesmonteiro
# clam@ufpe.cin.br
#================================================


def annotaText(x, y, text, mini, maxi):
    ''' adiciona marcadores dos valores em cada
        ponto do grafico
    '''
    import plotly.graph_objs as go
    listona = []
    cont = 0

    if not isinstance(text[0], int):
        xt = [str(i) for i in text]

    while cont < len(x):
        dic = {'x': x[cont], 
               'y': y[cont], 
               'xref': 'x', 
               'yref':'y', 
                'text': xt[cont],
                'showarrow':True,
                'arrowhead' : 7,
                'ax' : 0,
                'ay': -40}
        listona.append(dic)
        cont = cont + 1

    layout = go.Layout(
        showlegend=False,
        annotations= listona,
        yaxis = dict( range=[mini, maxi] )
    )
    
    return layout