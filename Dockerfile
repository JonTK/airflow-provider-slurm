# Dockerfile for airflow-provider-slurm development
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /workspace

# Set environment variables
ENV AIRFLOW_HOME=/airflow \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////airflow/airflow.db \
    PYTHONUNBUFFERED=1

# Create airflow directory
RUN mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins

# Install Apache Airflow with PostgreSQL support
ARG AIRFLOW_VERSION=2.8.1
RUN pip install --no-cache-dir \
    "apache-airflow[postgres]==${AIRFLOW_VERSION}" \
    "psycopg2-binary>=2.9.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

# Copy provider source code
COPY . /workspace/

# Install provider in development mode with dev dependencies
RUN pip install --no-cache-dir -e ".[dev]"

# Initialize Airflow database
RUN airflow db init && \
    airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Expose Airflow webserver port
EXPOSE 8080

# Default command
CMD ["airflow", "webserver"]
