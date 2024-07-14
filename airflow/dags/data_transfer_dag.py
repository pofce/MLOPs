import json
import boto3
from pathlib import Path
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

config_path = '/opt/airflow/config/secrets.json'

# Load the JSON file
with open(config_path, 'r') as file:
    config = json.load(file)

AWS_ACCESS_KEY_ID = config['aws_access_key_id']
AWS_SECRET_ACCESS_KEY = config['aws_secret_access_key']
BUCKET_NAME = config['bucket_name']
DATA_DIRECTORY = config['data_directory']

# Create an S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)


def file_exists_in_bucket(s3_client, bucket_name, key):
    """
    Function to check if a file exists in the S3 bucket.
    """
    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except:
        return False


def upload_file_to_s3(file_path, bucket_name, base_directory):
    """
    Function to upload a file to an S3 bucket, preserving the directory structure.
    """
    relative_path = file_path.relative_to(base_directory)
    key = str(relative_path)

    # Open the file and upload it
    with file_path.open("rb") as data:
        s3_client.upload_fileobj(data, Bucket=bucket_name, Key=key)
        logging.info(f'Uploaded {file_path.name} to {bucket_name}/{key}')


def transfer_to_s3():
    """
    Function that transfers data to the S3 bucket, preserving directory structure.
    """
    base_directory = Path(DATA_DIRECTORY)
    # Loop through each file in the directory
    for file_path in base_directory.rglob('*'):
        if file_path.is_file():
            key = str(file_path.relative_to(base_directory))
            # Check if the file already exists in the bucket
            if not file_exists_in_bucket(s3_client, BUCKET_NAME, key):
                # Upload the file if it does not exist
                upload_file_to_s3(file_path, BUCKET_NAME, base_directory)
            else:
                print(f'{key} already exists in the bucket {BUCKET_NAME}.')


default_args = {
    'owner': 'Vladyslav Radchenko',
    'start_date': datetime.now(),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
}

with DAG('Data_Transfer_Dag', default_args=default_args, schedule_interval="@daily", tags=["MLOPs"]):
    PythonOperator(task_id='Data_Transfer_From_Local_To_S3', python_callable=transfer_to_s3)
