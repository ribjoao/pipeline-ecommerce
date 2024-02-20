import pandas as pd
from sqlalchemy import create_engine
from time import time
import os
import re


def load_database(user, password, host, port, db, schema_db, dir_name, csv_file):
    print("diretorio=", dir_name)
    print("nome do arquivo=", csv_file)


    # 1. Engine to connect Postgres
    print("Criando a engine ...")
    
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        print(engine.connect())
        print("Conexão realizada!")  
    except Exception as e:
        print("Erro:",e)
        print("Falha ao conectar. Conexão inexistente!")
        
    # 2. Inserindo arquivo:
    
    t_inicio = time()
    
    file = csv_file

    print("\n Leitura de dados ...")

    file_path = dir_name + '/' + file
    print(f'Lendo o arquivo : {file}, na pasta {file_path}')
    
    df_olist = pd.read_csv(file_path)

    #print(df_olist.info())
    #print(df_olist.head())


    # 5. Create table name

    table_name = re.findall('(.*).csv', file)  
    
    print('Nome da tabela:\n',table_name[0])


    # Inserir tabela e tipo dos dados em db Postgres
    try:
        print(f"\n Criando o schema: {schema_db}")
        schema_query = f'CREATE SCHEMA IF NOT EXISTS {schema_db}'
        engine.execute(schema_query)
    except Exception as e:
        print("Erro ao criar o schema!")
        
    
    print(f"\n Inserindo estrutura da tabela {table_name} no schema:{schema_db}...")
    df_olist.head(n=0).to_sql(name=table_name[0],schema = 'ecommerce', con=engine, if_exists='replace')

    t_start = time()
    print("\n Populando a tabela ...")
    df_olist.to_sql(name=table_name[0], schema ='ecommerce', con=engine, if_exists='replace',index=False)
            
    t_end = time()

    print('Inserindo dados ..., aprox. %.3f segundos' % (t_end - t_start))
    print('_________________________________________________________')

    t_fim = time()

    print('Tempo total ..., aprox. %.3f segundos' % (t_fim - t_inicio))
    print('Fim =)')           




# # 2. Read Files from path
# t_inicio = time()

# url_dir = '/home/ribjoao/desktop/projetos_portfolio/ecommerce-pipeline/ingest_postgres/olist_csvs'

# def read_files(path):
#     files = [file for file in os.listdir(path)]
#     return files

# print("\n Buscando Arquivos csv no diretório ...")

# olist_files = read_files(url_dir)

# print("lista de arquivos:\n",olist_files)









## 5. Create table names:
#def create_table_name(filename):
    #removes = ['olist_','_dataset.csv','.csv']
    #aux= [map(name.strip, removes) for name in filenames]
    #table_names = [name.strip('_dataset.csv') for name in aux]
    #names = [name.strip('_dataset.csv') for name in filenames]
    #return names
    


# print("Criando nomes das tabelas")

# table_names = create_table_names(olist_files)

# print(*table_names, sep= "\n")

#url = '/home/ribjoao/desktop/projetos_portfolio/ecommerce-pipeline/ingest_postgres/olist_csvs/olist_order_items_dataset.csv'


# # 4. Read CSV pandas
# for file in olist_files:
    
#     url = file

#     print("\n Leitura de dados ...")
#     print(f'Lendo o arquivo : {url}')

#     url_file = url_dir + '/' + url
#     df_olist = pd.read_csv(url_file)

#     #print(df_olist.info())
#     #print(df_olist.head())


#     # 5. Create table name

#     table_name = re.findall('(.*).csv', url)
        
#     print('Nome da tabela:\n',table_name[0])


#     # Inserir tabela e tipo dos dados em db Postgres
#     print("\n Inserindo formato tabela ...")
#     df_olist.head(n=0).to_sql(name=table_name[0],schema = 'ecommerce', con=engine, if_exists='replace')

#     t_start = time()

#     df_olist.to_sql(name=table_name[0], schema ='ecommerce', con=engine, if_exists='replace',index=False)
            
#     t_end = time()

#     print('Inserindo dados ..., aprox. %.3f segundos' % (t_end - t_start))
#     print('_________________________________________________________')

# t_fim = time()

# print('Tempo total ..., aprox. %.3f segundos' % (t_inicio - t_fim))
# print('Fim =)')



##def main():

    # 1. Download arquivos (not)
        ## other script #######
    
    # 2. Read files from path

    # 3. Engine to connect Postgres
    
    # 4. Read csv pandas (1 iteration file)
    
    # 5. Create table names?
    
    # 6. Insert empty table
    
    # 7. Insert rows from table to Postgres
             # return to step 3.