from dotenv import load_dotenv
from google.cloud import storage
from os import environ
import tempfile

load_dotenv()

class GcsTools(object):
    """
    Singleton object for working with GCS objects.
    """

    _client = None
    _instance = None

    def __new__(cls, *args, **kwargs):
        """
        Singleton catcher. Can optionally pass a kwarg or "use_service_account" with a value of
        {'keyfile'=path_to_json_keyfile} to authenticate as a service account
        :param args:
        :param kwargs:
        """
        if cls._instance is None:
            cls._instance = super(GcsTools, cls).__new__(cls)

            if 'use_service_account' in kwargs:
                gcs_account = kwargs['use_service_account']
                cls._client = storage.Client.from_service_account_json(gcs_account['keyfile'])
            else:
                cls._client = storage.Client()
        return cls._instance


    @staticmethod
    def check_file_exists_on_cloud(bucket, filename):
        """
        Pass a bucket and a filename (including any prefix path
        and return whether it exists on cloud storage
        :param bucket: Bucket name
        :param filename: A path and filename to the file in the bucket
        :return: Boolean, exists or not
        """
        bucket = GcsTools._client.bucket(bucket)
        stats = storage.Blob(bucket=bucket, name=filename).exists(GcsTools._client)
        return stats

    @staticmethod
    def list_blobs(bucket_name, p=''):
        """
        Return a list of all object blobs in the given bucket matching the optional prefix p
        :param bucket_name:
        :param p:
        :return:
        """
        blobs = GcsTools._client.list_blobs(bucket_name, prefix=p)
        return blobs

    @staticmethod
    def list_blobs_names(bucket_name, p=''):
        """
        Return a list of all blob names in the given bucket matching the optional prefix p
        :param bucket_name:
        :param p:
        :return:
        """
        blobs = GcsTools._client.list_blobs(bucket_name, prefix=p)
        blobnames = [x.name for x in blobs]
        return blobnames

    @staticmethod
    def download_temp(bucket, remote_path):
        """
        Download a file from GCS and write it to a temporary file on disk. Return the named
        temporary file.
        :param bucket:
        :param remote_path:
        :return:
        """
        bucket = GcsTools._client.bucket(bucket)
        blob = bucket.blob(remote_path)

        fp = tempfile.NamedTemporaryFile()

        GcsTools._client.download_blob_to_file(blob, fp)
        fp.seek(0)
        return fp

    @staticmethod
    def download_blob(bucket, remote_path, local_path):
        """
        Download a file from GCS and write it to disk.
        :param bucket:
        :param remote_path:
        :return:
        """
        bucket = GcsTools._client.bucket(bucket)
        blob = bucket.blob(remote_path)

        blob.download_to_filename(local_path)
        return

    @staticmethod
    def upload_temp(bucket_name, source_file_obj, destination_blob_name):
        """
        Upload a file object from disk. Works with temp files, should also work with "real" files as long
        as they're open as a file object. TODO test real files
        :param bucket_name: Bucket to upload file to
        :param source_file_obj: An active file object to upload
        :param destination_blob_name: A name, including any "subdirectories", to upload the blob to (but not bucket)
        :return:
        """
        bucket = GcsTools._client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_file(source_file_obj)
        #We return the blob object in order to make the temporary file public for download in main.py
        return blob

    @staticmethod
    def delete_blob(bucket_name, blob_name):
        """Deletes a blob from the bucket."""
        # bucket_name = "your-bucket-name"
        # blob_name = "your-object-name"

        bucket = GcsTools._client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()

        print("Blob {} deleted.".format(blob_name))
        return

    @staticmethod
    def move_blob(source_bucket, source_blob_name, dest_bucket, dest_blob_name):
        """

        :param source_bucket:
        :param source_blob_name:
        :param dest_bucket:
        :param dest_blob_name:
        :return:
        """
        source_bucket = GcsTools._client.bucket(source_bucket)
        source_blob = source_bucket.blob(source_blob_name)
        dest_bucket = GcsTools._client.bucket(dest_bucket)

        new_blob = source_bucket.copy_blob(source_blob, dest_bucket, dest_blob_name)
        source_blob.delete()
        print('File moved from {} to {}'.format(source_blob_name, dest_blob_name))
