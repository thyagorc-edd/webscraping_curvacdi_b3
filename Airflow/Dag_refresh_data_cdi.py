
from datetime import datetime, timedelta, timezone
from operator import index
from os import sep
from bs4 import BeautifulSoup
import pandas as pd
from urllib.request import urlopen
import pendulum


from airflow import DAG
from airflow.utils import timezone
from airflow.models import Variable
from airflow.operators.python import PythonOperator


nowtz = pendulum.now('America/Sao_Paulo')

now = nowtz.subtract(days=1)
year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")
time = now.strftime("%H:%M")
today = now.strftime('%Y-%m-%d')


from sqlalchemy import create_engine
engine = create_engine(f'mssql+pyodbc://{user}:{password}@{hostName}:1433/{db}?driver=ODBC+Driver+17+for+SQL+Server')
conn = engine.connect()


with DAG(
    dag_id='web.001_cdiB3_diario',
    schedule_interval='00 11 * * *', # executar 22h e 3 minutos horário UTC
    start_date=datetime(2022, 4, 10),
    catchup=False,
    tags=['WEB','DIARIO','thyago.carvalho'],
) as dag:

    def def_acessar_taxas_b3(**kwargs):
        
        html = urlopen(f'https://www2.bmf.com.br/pages/portal/bmfbovespa/lumis/lum-taxas-referenciais-bmf-ptBR.asp?Data={day}/{month}/{year}&Data1={year}{month}{day}&slcTaxa=PRE')
        bs = BeautifulSoup(html, 'html.parser')
        linhas = bs.find_all('td')

        print('PASSO 01 - Criando array de dados')
        
        ### Criando arrays para guardar os registros da coleta
        dia, idx_252, idx_360 = [],[],[]
        
        row = 0
        while row < len(linhas):
            dia.append(linhas[row].text)
            row = row + 3

        row = 1
        while row < len(linhas):
            idx_252.append(linhas[row].text)
            row = row + 3

        row = 2
        while row < len(linhas):
            idx_360.append(linhas[row].text)
            row = row + 3   
        
        print('PASSO 02 - Criando Dataframe com os arrays')

        df = pd.DataFrame({'Data':today, 'Dia': dia, 'ind252':idx_252, 'ind360':idx_360})
        print(df)
        df.to_csv('dadosB3.csv', index=False)

    def def_transformar_colunas_em_valores(**kwargs):
        df = pd.read_csv('dadosB3.csv')

        df['ind252'] = df['ind252'].replace(to_replace=',', value='.', regex=True)
        df['ind360'] = df['ind360'].replace(to_replace=',', value='.', regex=True)

        df['Dia'] = df['Dia'].astype(int)
        df['ind252'] = df['ind252'].astype(float)
        df['ind360'] = df['ind360'].astype(float)

        print(df)
        print(df.dtypes)
        df.to_csv('dadosB3.csv', index=False)
    
    def def_lercsv_gravar_sqlserver():
        df = pd.read_csv('dadosB3.csv')
        df['Data'] = df['Data'].astype('datetime64[ns]')
        print(df)
        print(df.dtypes)
        df.to_sql('web_curva_cdi_b3', engine, index=False, if_exists='append')
        
        
    ## criação das tasks de serviço
    task_acessar_taxas_b3 = PythonOperator(
        task_id='id_acessar_taxas_b3',
        provide_context=True,
        python_callable=def_acessar_taxas_b3
    )
    
    task_mudar_tipo_dado = PythonOperator(
        task_id='id_mudar_tipo_dado',
        provide_context=True,
        python_callable=def_transformar_colunas_em_valores
    )
    
    task_lercsv_gravar_sqlserver = PythonOperator(
        task_id='id_lercsv_gravar_sqlserver',
        provide_context=True,
        python_callable=def_lercsv_gravar_sqlserver
    )


    task_acessar_taxas_b3 >> task_mudar_tipo_dado >> task_lercsv_gravar_sqlserver