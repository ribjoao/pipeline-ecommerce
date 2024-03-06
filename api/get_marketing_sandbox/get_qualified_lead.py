import requests
import pandas as pd
from io import StringIO

# API URL
url = 'http://localhost:8000/api/qualified_leads'

# optional params
params = {'page': 1,
          'items_per_page': 10,
          'mql_id':'b4bc852d233dfefc5131f593b538befa'}

# Request get
response = requests.get(url, params=params)

if response.status_code == 200:

    dados_da_api = response.json()
    
    df = pd.read_json(StringIO(dados_da_api))
    dados = dados_da_api
    
    print(dados)
    print(df)
else:
    print(f"Erro na requisição. Código de status: {response.status_code}")