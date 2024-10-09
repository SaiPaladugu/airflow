# Use a lightweight Python base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app/scripts

# Copy the ingestion.py script into the container
COPY scripts/runtime.py .
COPY scripts/ingest.py .

# Install the required Python packages
RUN pip install pandas huggingface_hub psutil

# Ensure the data directory exists (will be mounted to host)
RUN mkdir -p /app/data

# Set the command to execute the ingestion script
CMD ["python", "runtime.py", "ingest.py"]
