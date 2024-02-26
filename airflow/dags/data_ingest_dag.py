from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import os
from datetime import datetime
from dotenv import load_dotenv

from dependencies.ingest import extract_from_database
from dependencies.ingest import create_table_to_database
from dependencies.ingest import load_to_database

# Environment variables
load_dotenv()

# Source
SOURCE_HOST = os.environ.get('SOURCE_HOST')
SOURCE_USER = os.environ.get('SOURCE_USER')
SOURCE_PASSWORD = os.environ.get('SOURCE_PASSWORD')
SOURCE_PORT = os.environ.get('SOURCE_PORT')
SOURCE_DATABASE = os.environ.get('SOURCE_DATABASE')

# set Schema and table (hardcoded)
SOURCE_SCHEMA = os.environ.get('SCHEMA')
SOURCE_TABLE = os.environ.get('TABLE')

# Source database
query_source = f'SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} LIMIT 10'       

# Ingest Layer database
TARGET_HOST = os.environ.get('TARGET_HOST')
TARGET_USER = os.environ.get('TARGET_USER')
TARGET_PASSWORD = os.environ.get('TARGET_PASSWORD')
TARGET_PORT = os.environ.get('TARGET_PORT')
TARGET_DATABASE = os.environ.get('TARGET_DATABASE')


# Landing database
#query_source = f'INSERT INTO {schema_table}.{table_name} SELECT * FROM {{ ti.xcom_pull(task_ids="select_data_origem" }}'

# DAG ------

local_workflow = DAG(
    "Ingest_local",
    schedule_interval="0 0 * * *",
    start_date= datetime(2024,2,24)
)

with local_workflow:
    
    extract_database_task = PythonOperator(
        task_id='extract_from_database',
        python_callable=extract_from_database,
        op_kwargs=dict(
            user=SOURCE_USER,
            password=SOURCE_PASSWORD,
            host=SOURCE_HOST,
            port=SOURCE_PORT,
            db=SOURCE_DATABASE,
            schema=SOURCE_SCHEMA,
            table=SOURCE_TABLE,
            query=query_source
            )
    )
    
    create_table_landing_task = PythonOperator(
        task_id='create_table_to_landing',
        python_callable=create_table_to_database,
        op_kwargs=dict(
            user=TARGET_USER,
            password=TARGET_PASSWORD,
            host=TARGET_HOST,
            port=TARGET_PORT,
            db=TARGET_DATABASE,
            schema=SOURCE_SCHEMA,
            table=SOURCE_TABLE
            )
        
        
    )
    # load_database_task = PythonOperator(
    #     task_id ='load_to_database',
    #     python_callable= load_database,
    #     op_kwargs=dict(
    #         user=PG_USER,
    #         password=PG_PASSWORD,
    #         host=PG_HOST,
    #         port=PG_PORT,
    #         db=PG_DATABASE,
    #         schema_db = PG_SCHEMA,
    #         dir_name=DATA_HOME,
    #         csv_file=OUTPUT_FILE,
    #     )
    # )
    
    extract_database_task >> create_table_landing_task ## >> load_database_task