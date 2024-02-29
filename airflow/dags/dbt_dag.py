from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from docker.types import Mount

# Environment variables ----
load_dotenv()

DBT_PROJECT = os.environ.get('DBT_PROJECT')
DBT_PROFILES = os.environ.get('DBT_PROFILES')

# DAG ----------
default_args = {
    'owner': 'dbt-ecommerce',
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

dbt_dag = DAG(
    "dbt_e-commerce",
    schedule_interval="0 0 * * *",
    start_date= datetime(2024,2,26),
    catchup=False,
    max_active_runs=1,
    default_args=default_args
)

with dbt_dag:
    
    hello_task = DockerOperator(
        task_id='hello_docker_python_task',
        image='python:3.8-slim-buster',
        command='echo "Hello python inside Docker!"',
        docker_url='tcp://docker-proxy:2375',
        auto_remove=True,
        network_mode='airflow_default',
    ) 
    
    dbt_task = DockerOperator(
        task_id='dbt_task',
        image='dbt/postgres',
        container_name='dbt_debug_docker',
        command="bash -c 'dbt build'",
        docker_url='tcp://docker-proxy:2375',
        network_mode='airflow_default',
        auto_remove=True,
        mounts = [Mount(
            source=DBT_PROJECT, target="//usr/app/dbt", type="bind"),
            Mount(
            source=DBT_PROFILES,target="/root/.dbt/",type="bind")],
        mount_tmp_dir = False
        )
    
    hello_task >> dbt_task