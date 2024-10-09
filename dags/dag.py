# File: dags/dag.py

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
from docker.types import Mount  # Import Mount

# Default arguments for the DAG
default_args = {
    'start_date': datetime(2023, 1, 1),
}

# Define the DAG
with DAG(
    dag_id='data_pipeline_dag',
    default_args=default_args,
    description='A DAG that runs ingestion, preprocessing, and analysis tasks wrapped with runtime.py',
    schedule=None,  # Changed from schedule_interval=None
    catchup=False,
) as dag:

    # Task 1: Ingestion using DockerOperator
    # ingest_task = DockerOperator(
    #     task_id='ingest_data',
    #     image='ingest:latest',
    #     api_version='auto',
    #     auto_remove='success',
    #     docker_url='unix://var/run/docker.sock',  # Correct URL
    #     network_mode='bridge',
    #     mounts=[Mount(source=f'{os.getcwd()}/data', target='/app/data', type='bind')],
    #     working_dir='/app/scripts',
    # )
    
    ingest_task = BashOperator(
        task_id='ingest_data',
        bash_command='python /opt/airflow/scripts/runtime.py /opt/airflow/scripts/ingest.py',
        cwd=os.getcwd(),
    )

    preprocess_task = BashOperator(
        task_id='preprocess_data',
        bash_command='python /opt/airflow/scripts/runtime.py /opt/airflow/scripts/preprocess.py',
        cwd=os.getcwd(),
    )

    analyze_task = BashOperator(
        task_id='analyze_data',
        bash_command='python /opt/airflow/scripts/runtime.py /opt/airflow/scripts/analyze.py',
        cwd=os.getcwd(),
    )


    # Set task dependencies
    ingest_task >> preprocess_task >> analyze_task
