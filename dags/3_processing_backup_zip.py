from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from config import Config
import os
import tempfile
import zipfile
import boto3
import logging

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Config
BUCKET = Config.bucket_name
ZIP_KEY = Config.bronz_s3

# Function to extract and upload CSVs
def extract_and_upload_to_bronze():
    s3 = boto3.client('s3')
    temp_dir = tempfile.mkdtemp()
    local_zip = os.path.join(temp_dir, "backup.zip")

    logger.info("📥 Downloading ZIP from S3")
    s3.download_file(BUCKET, ZIP_KEY, local_zip)

    logger.info("📂 Extracting ZIP")
    with zipfile.ZipFile(local_zip, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)

    for mobile_number in os.listdir(temp_dir):
        mobile_path = os.path.join(temp_dir, mobile_number)
        if not os.path.isdir(mobile_path) or not mobile_number.isdigit():
            continue

        for date_folder in os.listdir(mobile_path):
            date_path = os.path.join(mobile_path, date_folder)
            if not os.path.isdir(date_path):
                continue

            try:
                entry_date = datetime.strptime(date_folder, "%d%m%Y").date()
            except ValueError:
                logger.warning(f"❌ Skipping unknown date format: {date_folder}")
                continue

            for file in os.listdir(date_path):
                if not file.endswith(".csv"):
                    continue

                full_path = os.path.join(date_path, file)
                key = f"bronze/raw/mobile_number={mobile_number}/entry_date={entry_date}/{file}"

                with open(full_path, "rb") as f:
                    s3.upload_fileobj(f, BUCKET, key)
                    logger.info(f"✅ Uploaded: {key}")

# DAG setup
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 24),
}

with DAG(
    dag_id='extract_zip_to_bronze',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['bronze', 'extract', 'zip'],
) as dag:

    task_extract_upload = PythonOperator(
        task_id='extract_and_upload_to_bronze',
        python_callable=extract_and_upload_to_bronze
    )
