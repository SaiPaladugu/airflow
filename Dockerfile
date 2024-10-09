FROM apache/airflow:2.10.2

# Switch to root to perform administrative tasks
USER root

# Ensure the data directory exists and set permissions (only user airflow)
RUN mkdir -p /opt/airflow/data && chown airflow /opt/airflow/data

# Switch to the airflow user before running pip install
USER airflow

# Install Python packages using pip
RUN pip install --no-cache-dir huggingface_hub pandas 'dask[dataframe]'
