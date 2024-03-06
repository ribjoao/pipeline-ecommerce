import requests
import pandas as pd
from io import StringIO

#Requests to call API
mql = requests.get('http://localhost:8000/qualified_leads')

#requests object to JSON
mql = mql.json()
print(type(mql))

# Read pandas
df = pd.read_json(StringIO(mql))

print(df['mql_id'].info())
print(df.head())

# print(mql)