FROM apache/airflow:2.10.2

# Switch to root to install the required packages
USER root

# Install Docker SDK for Python
RUN pip install docker

# Ensure the data directory exists and set permissions (only user airflow)
RUN mkdir -p /opt/airflow/data && chown airflow /opt/airflow/data

# Switch back to airflow user
USER airflow
