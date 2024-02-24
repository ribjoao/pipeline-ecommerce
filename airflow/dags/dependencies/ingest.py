import logging
import pandas as pd
from sqlalchemy import create_engine
from time import time
import os
import re

def extract_from_database(**kwargs):
    
    logging.info("\n Init Extract from Database!")
    
    # 1. Engine to connect database
    user = kwargs.get('user')
    password = kwargs.get('password')
    host = kwargs.get('host')
    port = kwargs.get('port')
    db = kwargs.get('db')

    logging.info("\n Create engine ...")
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        conn = engine.connect()
        logging.info("Connection OK!")  
    except Exception as e:
        logging.info("Error:",e)
        logging.info("Connection Fail!")
        
    # 2. Query from database
    schema = kwargs.get('schema')
    table = kwargs.get('table')
    
    # Time queries
    t_init = time()
    logging.info("\n Read data from database...")
    query = kwargs.get('query')
    
    # Read first 10 lines
    logging.info('\n Table name from source: ', table)
    
    df = pd.read_sql_query(sql = query, con = conn)

    print(df.info())
    print(df.head())
    
    t_finish = time()
    logging.info("Query time =",t_finish - t_init)
    
    # Close connection
    conn.close()
    
    return df.head(5)


    # t_start = time()
    # print("\n Populando a tabela ...")
    # df_olist.to_sql(name=table_name[0], schema ='ecommerce', con=engine, if_exists='replace',index=False)
            
    # t_end = time()

    # print('Inserindo dados ..., aprox. %.3f segundos' % (t_end - t_start))
    # print('_________________________________________________________')

    # t_fim = time()

    # print('Tempo total ..., aprox. %.3f segundos' % (t_fim - t_inicio))
    # print('Fim =)')
    
    # # Close connection
    # conn.close()      

def create_table_to_database(ti,**kwargs):
    
    logging.info("\n Init Create table to Landing Database!")
    
    # 1. Engine to connect database
    user = kwargs.get('user')
    password = kwargs.get('password')
    host = kwargs.get('host')
    port = kwargs.get('port')
    db = kwargs.get('db')

    logging.info("\n Create engine ...")
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        conn = engine.connect()
        logging.info("Connection OK!")  
    except Exception as e:
        logging.info("Error:",e)
        logging.info("Connection Fail!")
        
    # 2. Query from database
    schema = kwargs.get('schema')
    table = kwargs.get('table')
    
    
    # Create schema to Storage Database
    logging.info(f"\n Create schema: {schema}")
    try:
        schema_query = f'CREATE SCHEMA IF NOT EXISTS {schema}'
        conn.execute(schema_query)
        logging.info(f"Schema {schema} Created!")
    except Exception as e:
        logging.info("Created schema ERROR!")
    
    #create table structure into schema
    logging.info(f"\n Insert table structure '{table}' in schema:'{schema}'...")
    df = ti.xcom_pull(task_ids="extract_from_database")
    df.head(n=0).to_sql(name=table,schema = schema, con=engine, if_exists='replace')
    
    # Close connection
    conn.close()
    
def load_to_database(**kwargs):
    pass