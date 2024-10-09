from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from docker.types import Mount
import os

# Default arguments for the DAG
default_args = {
    'start_date': datetime(2023, 1, 1),
}

# Define the DAG
with DAG(
    dag_id='data_pipeline_dag',
    default_args=default_args,
    description='A DAG that runs ingestion, preprocessing, and analysis tasks wrapped with runtime.py',
    schedule=None,  # No schedule
    catchup=False,
) as dag:

    # Ingest task with DockerOperator
    ingest_task = DockerOperator(
        task_id='ingest_data',
        image='ingest:latest',  # Ensure this image is correctly built
        container_name='ingest_container',
        auto_remove=True,
        mounts=[
            # Bind the local directory to the container's data directory
            Mount(source='/Users/saipaladugu/Desktop/Misc/projects/airflow/data', target='/opt/airflow/data', type='bind'),
        ],  
        command="python /opt/ingest/ingest.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
    )

    # Preprocess task (unchanged)
    preprocess_task = BashOperator(
        task_id='preprocess_data',
        bash_command='python /opt/airflow/scripts/runtime.py /opt/airflow/scripts/preprocess.py',
        cwd=os.getcwd(),
    )

    # Analyze task (unchanged)
    analyze_task = BashOperator(
        task_id='analyze_data',
        bash_command='python /opt/airflow/scripts/runtime.py /opt/airflow/scripts/analyze.py',
        cwd=os.getcwd(),
    )

    # Set task dependencies
    ingest_task >> preprocess_task >> analyze_task
