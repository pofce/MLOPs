from minio import Minio, S3Error
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


BUCKET_NAME = 'train-data'

# Initialize MinIO client
client = Minio('minio:9000',
               access_key='minio',
               secret_key='miniosecret',
               secure=False)


def create_bucket_if_not_exists(minio_client, bucket_name):
    """
    Ensure the bucket exists in MinIO.
    """
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f'Bucket {bucket_name} created.')
        else:
            print(f'Bucket {bucket_name} already exists.')
    except S3Error as e:
        print(f"Failed to create or check bucket {bucket_name}: {e}")


def file_exists_in_bucket(minio_client, object_name):
    """
    Function to check if a file exists in the bucket
    """
    objects = minio_client.list_objects(BUCKET_NAME)
    for obj in objects:
        if obj.object_name == object_name:
            return True
    return False


def transfer_to_minio():
    """
    Function that transfers data to the MinIO bucket.
    """
    create_bucket_if_not_exists(client, BUCKET_NAME)

    # Loop through each file in the directory
    for file_path in Path('data/').iterdir():
        if file_path.is_file():
            # Check if the file already exists in the bucket
            if not file_exists_in_bucket(client, file_path.name):
                # Upload the file if it does not exist
                client.fput_object(BUCKET_NAME, object_name=file_path.name, file_path=str(file_path))
                print(f'Uploaded {file_path.name} to {BUCKET_NAME}')
            else:
                print(f'{file_path.name} already exists in the bucket {BUCKET_NAME}.')


default_args = {
    'owner': 'Vladyslav Radchenko',
    'start_date': datetime.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
}


with DAG('Data_Transfer_Dag', default_args=default_args, schedule_interval="@daily", tags=["MLOPs", "Hw2"]):

    PythonOperator(task_id='Data_Transfer_From_Local_To_MinIO', python_callable=transfer_to_minio)
