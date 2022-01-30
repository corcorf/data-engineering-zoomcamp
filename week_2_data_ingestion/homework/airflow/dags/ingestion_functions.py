"""Functions common to data ingestion DAGs."""
import os


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
    import logging

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


def download_source_file_to_gcs(src_url, bucket, gcs_path):
    """Download a csv file from a url and save to google cloud storage.

    :param src_url: url of source csv (string)
    :param bucket: Google Cloud Storage Bucket to save csv (string)
    :param gcs_path: path in Google Cloud Storage Bucket to save csv (string)
    """
    import shutil
    from tempfile import TemporaryDirectory

    import requests
    from google.cloud import storage

    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    client = storage.Client()
    bucket = client.bucket(bucket)
    src_filename = os.path.split(src_url)[-1]
    object_name = os.path.join(gcs_path, src_filename)
    blob = bucket.blob(object_name)
    with TemporaryDirectory() as tempDir:
        tmpPth = os.path.join(tempDir, src_filename)
        with requests.get(src_url, stream=True) as file_data:
            with open(tmpPth, "wb") as filehandler:
                shutil.copyfileobj(file_data.raw, filehandler)
        blob.upload_from_filename(tmpPth)


def convert_source_to_parquet(src_filename, bucket, gcs_path):
    """Convert source file (on GSC) to parquet and save back to GCS.

    :param src_filename: source file name
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :return:
    """
    import logging

    if not src_filename.endswith(".csv"):
        logging.error("Can only accept source files in CSV format")
        return
    from tempfile import TemporaryDirectory

    import pyarrow.csv as pv
    import pyarrow.parquet as pq
    from google.cloud import storage

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    client = storage.Client()
    bucket = client.bucket(bucket)
    dst_filename = src_filename.replace(".csv", ".parquet")
    with TemporaryDirectory() as tempDir:
        tmp_src = os.path.join(tempDir, src_filename)
        tmp_dst = os.path.join(tempDir, dst_filename)
        src_object_name = os.path.join(gcs_path, src_filename)
        src_blob = bucket.blob(src_object_name)
        src_blob.download_to_filename(tmp_src)

        table = pv.read_csv(tmp_src)
        pq.write_table(table, tmp_dst)

        dst_object_name = os.path.join(gcs_path, dst_filename)
        dst_blob = bucket.blob(dst_object_name)
        dst_blob.upload_from_filename(tmp_dst)


def delete_from_gcs(src_filename, bucket, gcs_path):
    """Delete a file from google cloud storage.

    :param src_filename: name of file for deletion (string)
    :param bucket: Google Cloud Storage Bucket to save csv (string)
    :param gcs_path: path in Google Cloud Storage Bucket to save csv (string)
    """
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket)
    object_name = os.path.join(gcs_path, src_filename)
    blob = bucket.blob(object_name)
    blob.delete()
