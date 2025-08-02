from airflow import DAG
from airflow.operators.python import PythonOperator
from config import Config
from datetime import datetime
import boto3
import requests
import zipfile
import os
import tempfile

# CONFIGURATION
BUCKET_NAME = Config.bucket_name
FOLDERS = Config.folder_created_in_bucket
GITHUB_REPO = Config.git_project_download
FILES_TO_FETCH = Config.required_file_list
BACKUP_ZIP_URL = Config.api_for_main_data_download_in_zip  # ZIP generator

# Task 1: Create S3 bucket and folders
def create_s3_structure():
    s3 = boto3.client('s3')
    existing_buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    if BUCKET_NAME not in existing_buckets:
        s3.create_bucket(
            Bucket=BUCKET_NAME
        )
    for folder in FOLDERS:
        s3.put_object(Bucket=BUCKET_NAME, Key=folder)

# Task 2: Download files from GitHub and upload to S3
def fetch_scripts_from_github():
    s3 = boto3.client('s3')
    for filename in FILES_TO_FETCH:
        url = f"{GITHUB_REPO}/{filename}"
        response = requests.get(url)
        if response.status_code == 200:
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=f"scripts/{filename}",
                Body=response.content
            )
            print(f"✅ Uploaded {filename} to scripts/")
        else:
            print(f"❌ Failed to download {filename}")

# Task 3: Download ZIP from server, extract, and upload to S3 under bronze/
def download_backup_zip_and_upload():
    s3 = boto3.client('s3')

    # Create temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, 'backup.zip')

        # Download ZIP file
        response = requests.get(BACKUP_ZIP_URL)
        if response.status_code != 200:
            print(f"❌ Failed to download ZIP: {BACKUP_ZIP_URL}")
            return

        with open(zip_path, 'wb') as f:
            f.write(response.content)
        print("✅ ZIP downloaded.")

        # Extract ZIP
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(tmpdir)
        print("✅ ZIP extracted.")

        # Walk through extracted files and upload to S3
        for root, dirs, files in os.walk(tmpdir):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, tmpdir)
                s3_key = f"bronze/{relative_path}"
                with open(file_path, 'rb') as data:
                    s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=data)
                    print(f"⬆️ Uploaded to S3: {s3_key}")

# DAG Setup
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 24),
}

with DAG(
    dag_id='setup_data_lake_and_scripts',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['s3', 'github', 'backup'],
) as dag:

    task_create_s3 = PythonOperator(
        task_id='create_s3_bucket_and_folders',
        python_callable=create_s3_structure
    )

    task_fetch_scripts = PythonOperator(
        task_id='download_scripts_from_github',
        python_callable=fetch_scripts_from_github
    )

    task_download_backup = PythonOperator(
        task_id='download_backup_zip_and_upload',
        python_callable=download_backup_zip_and_upload
    )

    task_create_s3 >> [task_fetch_scripts, task_download_backup]