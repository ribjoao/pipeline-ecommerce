import requests
import pandas as pd
from io import StringIO

# API url
url = 'http://localhost:8000/api/closed_deals'

# optional parameters
params = {'page': 1,
          'items_per_page': 10,
          'mql_id':'327174d3648a2d047e8940d7d15204ca'}

# Get request
response = requests.get(url, params=params)

if response.status_code == 200:

    dados_da_api = response.json()

    df = pd.read_json(StringIO(dados_da_api))
    dados = dados_da_api
    
    print(dados)
    print(df)
else:
    print(f"Erro na requisição. Código de status: {response.status_code}")