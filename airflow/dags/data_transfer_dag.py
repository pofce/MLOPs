import json

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path

config_path = '/opt/airflow/config/secrets.json'

# Load the JSON file
with open(config_path, 'r') as file:
    config = json.load(file)

ACCOUNT_URL = config["account_url"]
SAS_TOKEN = config["sas_token"]
CONTAINER_NAME = config["container_name"]
DATA_DIRECTORY = config["data_directory"]

# Create a BlobServiceClient
blob_service_client = BlobServiceClient(account_url=ACCOUNT_URL, credential=SAS_TOKEN)


def create_container_if_not_exists(blob_service_client, CONTAINER_NAME):
    """
    Ensure the container exists in Azure Blob Storage.
    """
    try:
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        container_client.create_container()
        print(f'Container {CONTAINER_NAME} created.')
    except ResourceExistsError:
        print(f'Container {CONTAINER_NAME} already exists.')


def file_exists_in_container(container_client, blob_name):
    """
    Function to check if a file exists in the container.
    """
    blob_client = container_client.get_blob_client(blob_name)
    return blob_client.exists()


def transfer_to_azure_blob():
    """
    Function that transfers data to the Azure Blob Storage container.
    """
    create_container_if_not_exists(blob_service_client, CONTAINER_NAME)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    # Loop through each file in the directory
    for file_path in Path(DATA_DIRECTORY).iterdir():
        if file_path.is_file():
            # Check if the file already exists in the container
            if not file_exists_in_container(container_client, file_path.name):
                # Upload the file if it does not exist
                with open(file_path, "rb") as data:
                    container_client.upload_blob(name=file_path.name, data=data, overwrite=True)
                print(f'Uploaded {file_path.name} to {CONTAINER_NAME}')
            else:
                print(f'{file_path.name} already exists in the container {CONTAINER_NAME}.')


default_args = {
    'owner': 'Vladyslav Radchenko',
    'start_date': datetime.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
}


with DAG('Data_Transfer_Dag', default_args=default_args, schedule_interval="@daily", tags=["MLOPs", "Hw2"]):

    PythonOperator(task_id='Data_Transfer_From_Local_To_Azure', python_callable=transfer_to_azure_blob)
