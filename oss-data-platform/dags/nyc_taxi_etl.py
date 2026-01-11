
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import boto3
import logging
from pathlib import Path
import os

# S3 Configuration from environment or defaults
S3_ENDPOINT = os.getenv('AIRFLOW_CONN_AWS_DEFAULT_ENDPOINT', 'http://minio:9000')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')
BUCKET_NAME = 'data-lake'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='nyc_taxi_etl',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['example', 'duckdb', 'minio']
) as dag:

    @task
    def create_bucket():
        """Creates the S3 bucket if it doesn't exist."""
        s3 = boto3.client(
            's3',
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        try:
            s3.create_bucket(Bucket=BUCKET_NAME)
            logging.info(f"Created bucket {BUCKET_NAME}")
        except s3.exceptions.BucketAlreadyOwnedByYou:
            logging.info(f"Bucket {BUCKET_NAME} already exists")

    @task
    def upload_sample_data():
        """Uploads the local sample data to MinIO."""
        s3 = boto3.client(
            's3',
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        # Path mapped in docker-compose
        local_path = Path('/opt/airflow/data/nyc_taxi_sample.csv')
        s3_key = 'landing/nyc_taxi_sample.csv'
        
        if not local_path.exists():
            raise FileNotFoundError(f"Sample data not found at {local_path}")
            
        s3.upload_file(str(local_path), BUCKET_NAME, s3_key)
        logging.info(f"Uploaded {local_path} to s3://{BUCKET_NAME}/{s3_key}")

    # Run dbt to transform data (Raw -> Staging -> Mart)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_project && dbt build --profiles-dir .',
    )

    create_bucket() >> upload_sample_data() >> dbt_run
