from bs4 import BeautifulSoup
import pandas as pd
from urllib.request import urlopen
import pendulum
from sqlalchemy import create_engine

now = pendulum.now('America/Sao_Paulo')
year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")
time = now.strftime("%H:%M")
today = now.strftime('%Y-%m-%d')


RangeDatas = pd.date_range(start="2022-01-01",end="2022-03-31").set_names('dataDia')
## Transformando a variável em um dataframe Pandas e adicioando colunas ao dataframe
dataFrameDatas = pd.DataFrame(RangeDatas)
dataFrameDatas['year'] = RangeDatas.strftime('%Y')
dataFrameDatas['month'] = RangeDatas.strftime('%m') 
dataFrameDatas['day'] = RangeDatas.strftime('%d') 


dffinal = pd.DataFrame()

for row in dataFrameDatas.index:

    datadia = dataFrameDatas['dataDia'][row]
    ano  = dataFrameDatas['year'][row]
    mes = dataFrameDatas['month'][row]
    dia   = dataFrameDatas['day'][row]

    
    html = urlopen(f'https://www2.bmf.com.br/pages/portal/bmfbovespa/lumis/lum-taxas-referenciais-bmf-ptBR.asp?Data={dia}/{mes}/{ano}&Data1={ano}{mes}{dia}&slcTaxa=PRE')
    bs = BeautifulSoup(html, 'html.parser')
    linhas = bs.find_all('td')

    dia, idx_252, idx_360 = [],[],[]

    ## busca e coleta os textos das tags filhas da tabela 
    # Como todos os registros estão em um único array, tive que fazer um salto
    # de 3 em 3 registros para gravar nos arrays criados
    # ##
    children = linhas

    row = 0
    while row < len(children):
        dia.append(children[row].text)
        row = row + 3

    row = 1
    while row < len(children):
        idx_252.append(children[row].text)
        row = row + 3

    row = 2
    while row < len(children):
        idx_360.append(children[row].text)
        row = row + 3  

    df = pd.DataFrame({'Data':datadia, 'Dia': dia, 'ind252':idx_252, 'ind360':idx_360})

    df['ind252'] = df['ind252'].replace(to_replace=',', value='.', regex=True)
    df['ind360'] = df['ind360'].replace(to_replace=',', value='.', regex=True)

    df['Data'] = df['Data'].astype('datetime64[ns]')
    df['Dia'] = df['Dia'].astype(int)
    df['ind252'] = df['ind252'].astype(float)
    df['ind360'] = df['ind360'].astype(float)

    
    dffinal = pd.concat([df, dffinal])

    #### NO FINAL VOCÊ TERÁ UM DATAFRAME COM TODOS OS DADOS DO PERÍODO PODENDO GRAVAR EM EXCEL, BANCO, OU CLOUD