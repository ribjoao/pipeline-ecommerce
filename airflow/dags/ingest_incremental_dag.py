from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

from dependencies.ingest import extract_from_database_incremental
from dependencies.ingest import load_to_database_incremental

# Environment variables
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

# Schema and tables ----------

# Orders table args
orders_kwargs= dict(query_path = os.environ.get('PATH_ORDERS'),
                    target_schema = os.environ.get('ORDERS_SCHEMA'),
                    target_table = os.environ.get('ORDERS_TABLE')
)
orders_kwargs.update(source_kwargs)
orders_kwargs.update(target_kwargs)

# # #  Order items args
# order_items_kwargs= dict(query_path = os.environ.get('PATH_ORDER_ITEMS'),
#                     target_schema = os.environ.get('ORDER_ITEMS_SCHEMA'),
#                     target_table = os.environ.get('ORDER_ITEMS_TABLE')
# )
# order_items_kwargs.update(source_kwargs)
# order_items_kwargs.update(target_kwargs)

# DAG ----------
default_args = {
    'owner': 'pipeline-ecommerce',
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

local_workflow = DAG(
    "ingest_incremental_query",
    schedule_interval="0 0 * * *",
    start_date= datetime(2018,8,1),
    end_date = datetime(2018,8,3),
    catchup=True,
    max_active_runs=1,
    default_args=default_args

)

with local_workflow:
    
    init = DummyOperator(
        task_id='init_ingestion'
    )
    
    extract_database_task = PythonOperator(
        task_id='extract_from_database_orders',
        python_callable=extract_from_database_incremental,
        provide_context=True,
        op_kwargs=orders_kwargs
    )
    
    load_database_task = PythonOperator(
        task_id ='load_to_staging_orders',
        python_callable= load_to_database_incremental,
        op_kwargs=orders_kwargs
    )
    
    final = DummyOperator(
        task_id='final_ingestion'
    )
    
    init >> extract_database_task >> load_database_task >> final