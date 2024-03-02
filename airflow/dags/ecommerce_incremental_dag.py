from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from docker.types import Mount

from dependencies.ingest import extract_from_database_incremental
from dependencies.ingest import load_to_database_incremental

## Environment variables ---------------
load_dotenv()
DBT_PROJECT = os.environ.get('DBT_PROJECT')
DBT_PROFILES = os.environ.get('DBT_PROFILES')

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

## Schema and tables 

# Orders table args
orders_kwargs= dict(query_path = os.environ.get('PATH_ORDERS'),
                    target_schema = os.environ.get('ORDERS_SCHEMA'),
                    target_table = os.environ.get('ORDERS_TABLE')
)
orders_kwargs.update(source_kwargs)
orders_kwargs.update(target_kwargs)

# Order items args 
order_items_kwargs= dict(query_path = os.environ.get('PATH_ORDER_ITEMS'),
                    target_schema = os.environ.get('ORDER_ITEMS_SCHEMA'),
                    target_table = os.environ.get('ORDER_ITEMS_TABLE')
)
order_items_kwargs.update(source_kwargs)
order_items_kwargs.update(target_kwargs)

# Order payments args 
order_payments_kwargs= dict(query_path = os.environ.get('PATH_ORDER_PAYMENTS'),
                    target_schema = os.environ.get('ORDER_PAYMENTS_SCHEMA'),
                    target_table = os.environ.get('ORDER_PAYMENTS_TABLE')
)
order_payments_kwargs.update(source_kwargs)
order_payments_kwargs.update(target_kwargs)

# Customers args
customers_kwargs= dict(query_path = os.environ.get('PATH_CUSTOMERS'),
                    target_schema = os.environ.get('CUSTOMERS_SCHEMA'),
                    target_table = os.environ.get('CUSTOMERS_TABLE')
)
customers_kwargs.update(source_kwargs)
customers_kwargs.update(target_kwargs)

# Order Reviews args
order_reviews_kwargs= dict(query_path = os.environ.get('PATH_REVIEWS'),
                    target_schema = os.environ.get('REVIEWS_SCHEMA'),
                    target_table = os.environ.get('REVIEWS_TABLE')
)
order_reviews_kwargs.update(source_kwargs)
order_reviews_kwargs.update(target_kwargs)

# DBT docker
DBT_PROJECT = os.environ.get('DBT_PROJECT')
DBT_PROFILES = os.environ.get('DBT_PROFILES')



###  DAG ----------------------------
default_args = {
    'owner': 'pipeline-ecommerce',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

local_workflow = DAG(
    "ecommerce_incremental",
    schedule_interval="0 0 * * *",
    start_date= datetime(2018,8,20),
    end_date= datetime(2018,8,31),
    catchup=True,
    max_active_runs=1,
    default_args=default_args

)

with local_workflow:
    
    init_ingestion = BashOperator(
        task_id="start_data_ingestion",
        bash_command='echo "start_ingestion_date={{ ds }}"'
    )
    
    extract_orders = PythonOperator(
        task_id='extract_from_db_orders',
        python_callable=extract_from_database_incremental,
        op_kwargs=orders_kwargs
    )
    
    extract_order_items = PythonOperator(
        task_id='extract_from_db_order_items',
        python_callable=extract_from_database_incremental,
        op_kwargs=order_items_kwargs
    )
    
    extract_order_payments = PythonOperator(
        task_id='extract_from_db_order_payments',
        python_callable=extract_from_database_incremental,
        op_kwargs=order_payments_kwargs
    )
    
    extract_customers = PythonOperator(
        task_id='extract_from_db_customers',
        python_callable=extract_from_database_incremental,
        op_kwargs=customers_kwargs
    )
    
    extract_order_reviews = PythonOperator(
        task_id='extract_from_db_order_reviews',
        python_callable=extract_from_database_incremental,
        op_kwargs=order_reviews_kwargs
    )
    load_stg_orders = PythonOperator(
        task_id ='load_to_staging_orders',
        python_callable= load_to_database_incremental,
        op_kwargs=orders_kwargs
    )
    
    load_stg_order_items = PythonOperator(
        task_id ='load_to_staging_order_items',
        python_callable= load_to_database_incremental,
        op_kwargs=order_items_kwargs
    )
    
    load_stg_order_payments = PythonOperator(
        task_id ='load_to_staging_order_payments',
        python_callable= load_to_database_incremental,
        op_kwargs=order_payments_kwargs
    )
    
    load_stg_customers = PythonOperator(
        task_id ='load_to_staging_customers',
        python_callable= load_to_database_incremental,
        op_kwargs=customers_kwargs
    )
    
    load_stg_order_reviews = PythonOperator(
        task_id ='load_to_staging_order_reviews',
        python_callable= load_to_database_incremental,
        op_kwargs=order_reviews_kwargs
    )   
    
    finish_ingestion = BashOperator(
        task_id="finish_data_ingestion",
        bash_command='echo "finish_ingestion_date={{ ds }}"'
    )
    
    dbt_task = DockerOperator(
        task_id='dbt_run',
        image='dbt/postgres',
        container_name='dbt_docker',
        command="bash -c 'dbt run'",
        docker_url='tcp://docker-proxy:2375',
        network_mode='airflow_default',
        auto_remove=True,
        mounts = [Mount(
            source=DBT_PROJECT, target="//usr/app/dbt", type="bind"),
            Mount(
            source=DBT_PROFILES,target="/root/.dbt/",type="bind")],
        mount_tmp_dir = False
    )
    
    init_ingestion >> extract_orders >> load_stg_orders
    extract_order_items >> load_stg_order_items
    extract_order_payments >> load_stg_order_payments
    extract_order_reviews >> load_stg_order_reviews
    extract_customers >> load_stg_customers
    [load_stg_orders, load_stg_order_items,
     load_stg_order_payments, load_stg_order_reviews,
     load_stg_customers] >> finish_ingestion
    finish_ingestion >> dbt_task
    