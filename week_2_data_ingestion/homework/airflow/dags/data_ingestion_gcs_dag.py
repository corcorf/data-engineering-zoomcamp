"""DAG for importing 2021 NY Yellow Taxi Data to Google Cloud Storage."""
import logging
import os
from datetime import date

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryCreateExternalTableOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

SOURCE_FILENAME = "yellow_tripdata_{{ data_interval_start }}.csv"
SOURCE_URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data"
SOURCE_URL = os.path.join(SOURCE_URL_PREFIX, SOURCE_FILENAME)
LOCAL_PATH = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")


def download_source_file(src_url, local_path):
    """Download a csv file and save locally.

    :param src_url: url of source csv (string)
    :param src_filename: filename for csv source (string)
    :param local_path: local path at which to save csv (string)
    """
    import shutil
    import requests
    fn = os.path.split(src_url)[-1]
    local_fn = os.path.join(local_path, fn)
    with requests.get(src_url, stream=True) as file_data:
        with open(local_fn, "wb") as f:
            shutil.copyfileobj(file_data.raw, f)


def format_to_parquet(local_path, src_filename):
    """Load a csv file, convert and save as parquet.

    :param src_filename: filename for csv source (string)
    """
    import pyarrow.csv as pv
    import pyarrow.parquet as pq
    src_file = os.path.join(local_path, src_filename)
    if not src_file.endswith(".csv"):
        logging.error("Can only accept source files in CSV format")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace(".csv", ".parquet"))


def upload_to_gcs(bucket, gcs_path, local_path, filename):
    """Upload file to Google Cloud Storage.

    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    from google.cloud import storage
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    object_name = os.path.join(gcs_path, filename)
    blob = bucket.blob(object_name)
    local_file = os.path.join(local_path, filename)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": date(2021, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=["dtc-de"],
) as dag:

    download_dataset_task = PythonOperator(
        task_id="download_dataset_task",
        python_callable=download_source_file,
        op_kwargs={
            "src_url": SOURCE_URL,
            "local_path": LOCAL_PATH,
        },
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "local_path": LOCAL_PATH,
            "src_template": SOURCE_FILENAME,
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "gcs_path": "raw/",
            "local_path": LOCAL_PATH,
            "fn_template": SOURCE_FILENAME.replace(".csv", ".parquet"),
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{SOURCE_FILENAME}"],
            },
        },
    )

    (
        download_dataset_task
        >> format_to_parquet_task
        >> local_to_gcs_task
        >> bigquery_external_table_task
    )
