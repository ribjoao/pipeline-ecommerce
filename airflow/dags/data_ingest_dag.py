from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
import os

from ingest_script import load_database

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "opt/airflow/")

DATA_HOME = AIRFLOW_HOME + '/data'
OUTPUT_FILE = 'olist_customers_dataset.csv'
SCHEMA_DB = 'ecommerce'

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

local_workflow = DAG(
    "Ingest_local",
    schedule_interval="0 0 * * *",
    start_date= datetime(2024,2,19)
)

with local_workflow:
    
    extract_task = BashOperator(
        task_id='extract_datasets',
        bash_command='pwd'
    )
    
    load_database_task = PythonOperator(
        task_id ='load_to_database',
        python_callable= load_database,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            schema_db = SCHEMA_DB,
            dir_name=DATA_HOME,
            csv_file=OUTPUT_FILE
        )
    )
    
    extract_task >> load_database_task