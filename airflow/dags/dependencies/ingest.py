import logging
import pandas as pd
from sqlalchemy import create_engine
from time import time
import os
import re

def extract_from_database_incremental(ti,**kwargs):
    
    logging.info("Init Extract Incremental from Database!")
    
    # 1. Engine to connect database
    user = kwargs.get('source_user')
    password = kwargs.get('source_password')
    host = kwargs.get('source_host')
    port = kwargs.get('source_port')
    db = kwargs.get('source_db')

    # Create Engine function ...
    logging.info("Create engine ...")
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        conn = engine.connect()
        logging.info("Connection OK!")  
    except Exception as e:
        logging.info("Error:",e)
        logging.info("Connection Fail!")
        

    # schema = kwargs.get('schema')
    # table = kwargs.get('table')
    
    # 2. Read sql query from file
    
    query_file = read_sql_file(
        path= kwargs.get('query_path')).replace(
            '{{ date_previous }}',
            kwargs.get('ds')
        )
    logging.info("\n Query file:",query_file)
    query = query_file
   
   # 3. Read data from database 
    t_init = time()
    logging.info("Read data from database...")

    df = pd.read_sql_query(sql = query, con = conn)
    
    t_finish = time()
    logging.info("Query time =",t_finish - t_init)
    
    # Close connection
    conn.close()
    
    logging.info("dataframe info:",df.info)
    
    ti.xcom_push(key='df_extract', value=df)

def load_to_database_incremental(ti,**kwargs):
    
    logging.info("Init Load Incremental to Landing Database!")
    t_init = time()
    

    user = kwargs.get('target_user')
    password = kwargs.get('target_password')
    host = kwargs.get('target_host')
    port = kwargs.get('target_port')
    db = kwargs.get('target_db')

    # 1. Engine to connect database
    logging.info("Create engine ...")
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        conn = engine.connect()
        logging.info("Connection OK!")  
    except Exception as except_engine:
        logging.info("Exception:",except_engine)
        logging.info("Connection Fail!")
        
    schema = kwargs.get('target_schema')
    table = kwargs.get('target_table')
    
    # 2. Check if schema and table exists..
    logging.info(f"Check if table '{table}' already exist:")
       
    try:
        query_exists = f"SELECT EXISTS (SELECT FROM pg_tables \
            WHERE schemaname = '{schema}' AND tablename  = '{table}')"
        result = conn.execute(query_exists)
        r = result.fetchone()[0]
        
        if r is True:
            logging.info(f"Schema and Table {schema} already exist!")
        else:
            logging.info(f"Schema and Table {schema} NOT exist!")
            # Create schema to Storage Database
            logging.info(f"Create schema: {schema}")
            
            try:
                schema_query = f'CREATE SCHEMA IF NOT EXISTS {schema}'
                conn.execute(schema_query)
                logging.info(f"Schema {schema} Created!")
            except Exception as except_schemadb:
                logging.info("Schemadb Exception =",except_schemadb)
                logging.info("Created schema ERROR!")
            
            # Insert structure table if not exists
            logging.info(f"Insert table structure '{table}' in schema:'{schema}'...")
            
            df = ti.xcom_pull(key='df_incremental')
            df.head(n=0).to_sql(name=table,schema = schema, con=engine, if_exists='replace')
            
            logging.info(f"Table '{table}' created!")
            
    except Exception as except_checktable:
        logging.info("Check table exception =",except_checktable)
        logging.info("Created schema and table Exception!")
    
    # 3. Populate Table
    try:
        t_start = time()
        logging.info("Populate table ...")
        
        df = ti.xcom_pull(key='df_extract')
        df.to_sql(name=table, schema =schema, con=engine, if_exists='append',index=False)
            
        t_end = time()

        logging.info('Inserted into table ..., aprox. %.3f seconds' % (t_end - t_start))
        logging.info(f'{df.shape} table')
        logging.info('_________________________________________________________')
        t_final = time()

        logging.info('Total time Load Database ..., aprox. %.3f seconds' % (t_final - t_init))
        logging.info('End =)')
    except Exception as except_insert:
        logging.info("Insert Exception:",except_insert)
        
    # Close connection
    conn.close()

def read_sql_file(path):
    
    with open(path) as f:
        content = f.read()
        logging.info("Reading SQL file ...")
        return content