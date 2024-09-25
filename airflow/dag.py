from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 1),
    'retries': 1
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='A simple data pipeline',
    schedule_interval=None
)

def ingest_data(**kwargs):
    # Code to run your ingest_data script here
    os.system('python3 /scripts/ingest_data.py')

def preprocess_data(**kwargs):
    # Code to run your preprocess_data script here
    os.system('python3 /scripts/preprocess_data.py')

def transform_data(**kwargs):
    # Code to run your transform_data script here
    os.system('python3 /scripts/transform_data.py')

# Define Airflow tasks
ingestion_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag
)

preprocessing_task = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data,
    dag=dag
)

transformation_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

# Define task dependencies
ingestion_task >> preprocessing_task >> transformation_task
