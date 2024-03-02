from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from docker.types import Mount

from dependencies.ingest import extract_from_database_incremental
from dependencies.ingest import load_to_database_incremental

## Environment variables ---------------
load_dotenv()

# Source database
source_kwargs= dict(source_host = os.environ.get('SOURCE_HOST'),
                    source_user = os.environ.get('SOURCE_USER'),
                    source_password = os.environ.get('SOURCE_PASSWORD'),
                    source_port = os.environ.get('SOURCE_PORT'),
                    source_db = os.environ.get('SOURCE_DATABASE')
)

# Target database
target_kwargs= dict(target_host = os.environ.get('TARGET_HOST'),
                    target_user = os.environ.get('TARGET_USER'),
                    target_password = os.environ.get('TARGET_PASSWORD'),
                    target_port = os.environ.get('TARGET_PORT'),
                    target_db = os.environ.get('TARGET_DATABASE')
)

# table - INSERT HERE!!---------------------------
table = 'sellers'
table_kwargs= dict(query_path = os.environ.get('PATH_SELLERS'),
                    target_schema = os.environ.get('SELLERS_SCHEMA'),
                    target_table = os.environ.get('SELLERS_TABLE')
)
table_kwargs.update(source_kwargs)
table_kwargs.update(target_kwargs)


###  DAG ----------------------------
default_args = {
    'owner': 'pipeline-ecommerce',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

local_workflow = DAG(
    "ingest_table_incremental",
    schedule_interval="0 0 * * *",
    start_date= datetime(2024,3,2),
    catchup=False,
    max_active_runs=1,
    default_args=default_args

)

with local_workflow:
    
    init_ingestion = BashOperator(
        task_id="start_data_ingestion",
        bash_command='echo "start_ingestion_date={{ ds }}"'
    )
    
    extract_table = PythonOperator(
        task_id=f'extract_from_db_{table}',
        python_callable=extract_from_database_incremental,
        op_kwargs=table_kwargs
    )

    load_stg_table = PythonOperator(
        task_id =f'load_to_staging_{table}',
        python_callable= load_to_database_incremental,
        op_kwargs=table_kwargs
    )   
    
    finish_ingestion = BashOperator(
        task_id="finish_data_ingestion",
        bash_command='echo "finish_ingestion_date={{ ds }}"'
    )
    
    init_ingestion >> extract_table >> load_stg_table >> finish_ingestion 
    