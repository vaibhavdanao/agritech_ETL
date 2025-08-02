from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from config import Config
import requests
import boto3
import os
import logging

# === CONFIGURATION ===
BUCKET_NAME = Config.bucket_name
ZIP_URL = Config.api_for_main_data_download_in_zip
LOCAL_ZIP_PATH = Config.zip_path
S3_KEY = Config.bronz_s3

def download_and_upload_zip():
    logging.info("🔄 Starting test: download ZIP and upload to S3")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115 Safari/537.36"
    }

    logging.info(f"📥 Downloading ZIP from {ZIP_URL}")
    response = requests.get(ZIP_URL, headers=headers)
    logging.info(f"🔁 HTTP Status: {response.status_code}")

    if response.status_code != 200:
        logging.error(f"❌ Failed to download ZIP: {response.status_code}")
        return

    with open(LOCAL_ZIP_PATH, "wb") as f:
        f.write(response.content)
    logging.info(f"✅ ZIP saved to {LOCAL_ZIP_PATH}")

    s3 = boto3.client("s3")
    with open(LOCAL_ZIP_PATH, "rb") as f:
        s3.upload_fileobj(f, BUCKET_NAME, S3_KEY)
    logging.info(f"☁️ Uploaded to S3: {BUCKET_NAME}/{S3_KEY}")

    os.remove(LOCAL_ZIP_PATH)
    logging.info(f"🧹 Removed local file: {LOCAL_ZIP_PATH}")

# === DAG DEFINITION ===
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 25),
}

with DAG(
    dag_id="test_backup_zip_download",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["test", "zip", "s3"],
) as dag:

    task_download_backup_zip_and_upload = PythonOperator(
        task_id="download_backup_zip_and_upload",
        python_callable=download_and_upload_zip,
    )
