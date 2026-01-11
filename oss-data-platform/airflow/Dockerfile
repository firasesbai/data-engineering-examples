FROM apache/airflow:2.9.1

USER root
# Install git for dbt and postgresql-client for verification
RUN apt-get update && \
    apt-get install -y git postgresql-client && \
    apt-get clean

USER airflow

# Install dependencies
RUN pip install --no-cache-dir \
    duckdb==0.10.2 \
    dbt-core==1.8.0 \
    dbt-duckdb==1.8.0 \
    pandas \
    pydantic \
    apache-airflow-providers-amazon \
    apache-airflow-providers-postgres
