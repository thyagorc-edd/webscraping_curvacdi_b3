# Objetivo do repositório

Coleta de dados da Fonte da B3 que contém curva de CDI futuro
Fonte: https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/mercado-de-derivativos/precos-referenciais/taxas-referenciais-bm-fbovespa/

Atualização da fonte: Diária 

Webscraping com urllib.request e BeautifulSoup para *"parsear"* o html.

# Códigos 
### TESTED PYTHON 3.8.12
* 01 - carga_inicial.py - Realiza carga inicial dos dados a partir de um período
* 02 - jupyter_cdi.ipynb - Jupyter Notebook para testes de código e visualização de resultados
* Dag_refresh_data_cdi.py - DAG para APACHE AIRFLOW (v2.2.2) para refresh de dados conforme período desejado
##### PARA A DAG, IMPLEMENTAR A CARGA DE DADOS A PARTIR DA LINHA 86

# Pacotes necessários
Todos as dependencias estão em */requirements.txt*
para instalar, "*pip3 install -r requirements.txt*"