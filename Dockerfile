# Use a base image compatible with the Airflow infrastructure
FROM python:3.9-slim

# Set environment variables to avoid interactive installs
ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_HOME=/opt/airflow

# Create the necessary directories
RUN mkdir -p $AIRFLOW_HOME/dags

# Copy DAG files into the DAGs directory
COPY dags/ $AIRFLOW_HOME/dags/

# Set the working directory for the DAGs
WORKDIR $AIRFLOW_HOME

# Set default command
CMD ["sleep", "infinity"]