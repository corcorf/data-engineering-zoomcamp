"""DAG for importing 2021 NY Yellow Taxi Data to Google Cloud Storage."""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from ingestion_functions import (
    convert_source_to_parquet,
    delete_from_gcs,
    download_source_file_to_gcs,
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

SOURCE_FILENAME = "taxi+_zone_lookup.csv"
SOURCE_URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/misc"
SOURCE_URL = os.path.join(SOURCE_URL_PREFIX, SOURCE_FILENAME)
LOCAL_PATH = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
GCS_PATH = "raw/"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_zones",
    schedule_interval="@once",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=["dtc-de"],
) as dag:

    download_dataset_task = PythonOperator(
        task_id="download_dataset_task",
        python_callable=download_source_file_to_gcs,
        op_kwargs={
            "src_url": SOURCE_URL,
            "bucket": BUCKET,
            "gcs_path": GCS_PATH,
        },
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=convert_source_to_parquet,
        op_kwargs={
            "src_filename": SOURCE_FILENAME,
            "bucket": BUCKET,
            "gcs_path": GCS_PATH,
        },
    )

    delete_source_task = PythonOperator(
        task_id="delete_source_task",
        python_callable=delete_from_gcs,
        op_kwargs={
            "src_filename": SOURCE_FILENAME,
            "bucket": BUCKET,
            "gcs_path": GCS_PATH,
        },
    )

    (download_dataset_task >> format_to_parquet_task >> [delete_source_task])
