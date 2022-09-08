import binascii
import collections
import datetime
import hashlib
import json
import re
import tempfile
import six
import sys

from dotenv import load_dotenv
from io import FileIO
from google.cloud import storage
from google.oauth2 import service_account
from six.moves.urllib.parse import quote

from os import environ


load_dotenv()


class GcsTools(object):
    """
    Singleton object for working with GCS objects.
    """

    # _client = storage.Client()
    _instance = None
    _creds = None

    def __new__(cls, *args, **kwargs):
        """
        Singleton catcher. Can optionally pass a kwarg or "use_service_account" with a value of
        {'keyfile'=path_to_json_keyfile} to authenticate as a service account
        :param args:
        :param kwargs:
        """
        if cls._instance is None:
            cls._instance = super(GcsTools, cls).__new__(cls)

            if "use_service_account" in kwargs:
                gcs_account = kwargs["use_service_account"]
                cls._client = storage.Client.from_service_account_json(
                    gcs_account["keyfile"]
                )
                cls._creds = service_account.Credentials.from_service_account_file(
                    gcs_account["keyfile"]
                )
            else:
                cls._client = storage.Client()
        return cls._instance

    @staticmethod
    def check_file_exists_on_cloud(bucket: str, filename: str) -> storage.Blob:
        """[summary]
        Pass a bucket and a filename (including any prefix path
        and return whether it exists on cloud storage

        Args:
        :param bucket: Bucket name
        :param filename: A path and filename to the file in the bucket
        :return: Boolean, exists or not
        """
        bucket = GcsTools._client.bucket(bucket)
        stats = storage.Blob(bucket=bucket, name=filename).exists(GcsTools._client)
        return stats

    @staticmethod
    def list_blobs(bucket_name: str, p: str = "") -> list:
        """[summary]
        Return a list of all object blobs in the given bucket matching the optional prefix p

        Args:
        :param bucket_name: The name of the source bucket
        :param p: An optional destination path if the sourced files are in a specific folder.
        :return:
        """
        blobs = GcsTools._client.list_blobs(bucket_name, prefix=p)
        return blobs

    @staticmethod
    def list_blobs_names(bucket_name: str, p: str = "") -> list:
        """[summary]
        Return a list of all blob names in the given bucket matching the optional prefix p

        Args:
        :param bucket_name: The name of the source bucket
        :param p: An optional destination folder path if the sourced files are in a specific folder.
        :return:
        """
        blobs = GcsTools._client.list_blobs(bucket_name, prefix=p)
        blobnames = [x.name for x in blobs]
        return blobnames

    @staticmethod
    def get_list_blobs_uris(bucket_name: str, p: str = "") -> dict:
        """[summary]
        Return a list of all blob uris within a gcp bucket. These objects are specifically .tif files to prevent bad data from being fed in.

        This function is specially modified and used in rpms-merge

        Args:
        :param bucket_name: The name of the source bucket
        :param p: An optional destination path if the sourced .tif files are in a specific folder.
        :return:
        """
        # bucket = GcsTools._client.bucket(bucket_name)
        blobs = GcsTools._client.list_blobs(bucket_name, prefix=p)
        year_dict = {}
        blobnames = [
            "gs://" + bucket_name + "/" + x.name
            for x in blobs
            if x.name.endswith(".tif")
        ]

        for blob in blobnames:
            match = re.search("\d{4}", blob)
            if match.group() not in year_dict:
                temp_list = [blob]
                year_dict[match.group()] = temp_list
            else:
                year_dict[match.group()].append(blob)

        return year_dict

    @staticmethod
    def download_temp(bucket: str, remote_path: str) -> tempfile.NamedTemporaryFile:
        """[summary]
        Download a file from GCS and write it to a temporary file on disk. Return the named
        temporary file.

        Args:
        :param bucket_name: The name of the source bucket
        :param remote_path: The source path of the source blob.
        :return:
        """
        bucket = GcsTools._client.bucket(bucket)
        blob = bucket.blob(remote_path)

        fp = tempfile.NamedTemporaryFile()

        GcsTools._client.download_blob_to_file(blob, fp)
        fp.seek(0)
        return fp

    @staticmethod
    def download_blob(bucket: str, remote_path: str, local_path: str) -> None:
        """[summary]
        Download a file from GCS and write it to disk.

        Args:
        :param bucket_name: The name of the source bucket
        :param remote_path: The destination folder path of the sourced blob.
        :return:
        """
        bucket = GcsTools._client.bucket(bucket)
        blob = bucket.blob(remote_path)

        blob.download_to_filename(local_path)
        return

    @staticmethod
    def upload_temp(
        bucket_name: str, source_file_obj: FileIO, destination_blob_name: str
    ) -> None:
        """[summary]
        Upload a file object from disk. Works with temp files, should also work with "real" files as long
        as they're open as a file object. TODO test real files

        Args:
        :param bucket_name: Bucket to upload file to
        :param source_file_obj: An active file object to upload
        :param destination_blob_name: A name, including any "subdirectories", to upload the blob to (but not bucket)
        :return:
        """
        bucket = GcsTools._client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_file(source_file_obj, rewind=True)
        # We return the blob object in order to make the temporary file public for download in main.py
        return blob

    @staticmethod
    def upload_from_memory(
        bucket_name: str, contents: str, destination_blob_name: str
    ) -> None:
        """ "[summary]
        Upload a string from memory.

        Args:
        :param bucket_name: Bucket to upload file to
        :param contents: A string to write to file
        :param destination_blob_name: A name, including any "subdirectories", to upload the blob to (but not bucket)
        :return:
        """
        bucket = GcsTools._client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(contents)

        # print(
        #     f"{destination_blob_name} with contents {contents} uploaded to {bucket_name}."
        # )
        return

    @staticmethod
    def delete_blob(bucket_name: str, blob_name: str) -> None:
        """[summary]
        Deletes a blob from the bucket.

        Args:
        :param bucket_name: The name of the source bucket
        :param blob_name: The source path of the source blob.
        :return:
        """

        # bucket_name = "your-bucket-name"
        # blob_name = "your-object-name"

        bucket = GcsTools._client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()

        print("Blob {} deleted.".format(blob_name))
        return

    @staticmethod
    def move_blob(
        source_bucket: str, source_blob_name: str, dest_bucket: str, dest_blob_name: str
    ) -> None:
        """[summary]
        A function to move a blob from one bucket to the other. WARNING: This function deletes the source blob from its source folder after moving.

        Args:
        :param source_bucket: The name of the source bucket
        :param source_blob_name: The source path of the source blob.
        :param dest_bucket: The name of the destination bucket
        :param dest_blob_name: The destination path of the sourced blob.
        :return:
        """
        source_bucket = GcsTools._client.bucket(source_bucket)
        source_blob = source_bucket.blob(source_blob_name)
        dest_bucket = GcsTools._client.bucket(dest_bucket)

        new_blob = source_bucket.copy_blob(source_blob, dest_bucket, dest_blob_name)
        source_blob.delete()
        print("File moved from {} to {}".format(source_blob_name, dest_blob_name))

    @staticmethod
    def copy_blob(
        bucket_name: str,
        blob_name: str,
        destination_bucket_name: str,
        destination_blob_name: str,
        preserve_acl: bool = False
    ) -> None:
        """[summary]
        Copies a blob from one bucket to another with a new name.

        Args:
        :param bucket_name: The name of the source bucket
        :param blob_name: The source path of the source blob.
        :param destination_bucket_name: The name of the destination bucket
        :param destination_blob_name: The destination path of the sourced blob.
        :param preserve_acl: Whether or not to preserve the ACL of the source blob.

        :return:

        """
        # bucket_name = "your-bucket-name"
        # blob_name = "your-object-name"
        # destination_bucket_name = "destination-bucket-name"
        # destination_blob_name = "destination-object-name"

        source_bucket = GcsTools._client.bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        destination_bucket = GcsTools._client.bucket(destination_bucket_name)

        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_blob_name, preserve_acl=preserve_acl
        )

    @staticmethod
    def upload_input_group(
        bucket_name: str, source_file_name: str, data: dict, data_type
    ) -> None:
        """[summary]
        This helper function that uploads a data cluster of user inputs to a unique folder, likely named with a user generated hash id.

        This function is specially modified and used in hwpc-web

        Args:
            bucket_name ([type]): The name of the destination bucket you are uploading to.
            source_file_name ([type]): The name of the destination folder for the user date, this consists of "hwpc-user-inputs" and the user's generated id.
            data ([type]): The data to be delivered to the destination folder.
            data_type ([type]): A logic check for harvested_wood_products.
        """

        data_json = {}
        # Code parses through data pulled from web
        for key,value in data.items():
            # If the code was potentially converted from a pandas dataframe for wide-to-long formatting or is just a string type it pases through here
            if str(type(value)) == "<class 'str'>":
                path = source_file_name+key
                temp_file = tempfile.TemporaryFile()
                temp_file.write(value.encode())
                temp_file.seek(0)
                data_json[key] = path
                GcsTools.upload_temp(bucket_name, temp_file, path)
                temp_file.close()
            # If the input is not empty, it will make the file and upload. If it is empty, it will be skipped and save memory.
            if str(type(value)) == "<class 'werkzeug.datastructures.FileStorage'>":
                if (value.content_length == "text/csv"):
                    path = source_file_name+key
                    temp_file = tempfile.TemporaryFile()
                    temp_file.write(value.read())
                    temp_file.seek(0)
                    data_json[key]=path
                    GcsTools.upload_temp(bucket_name, temp_file, path)
                    temp_file.close()
                path = source_file_name+key
                data_json[key]=path
            
        # The json of all the file paths is converted into a string then to bytes to be uploaded as a temp file for use in the worker.
        data_json = json.dumps(data_json)
        data_json = data_json.encode()
        user_file = tempfile.TemporaryFile()
        user_file.write(data_json)
        user_file.seek(0)
        GcsTools.upload_temp(bucket_name, user_file, source_file_name + "user_input.json")
        user_file.close()
        return

    @staticmethod
    def make_blob_public(bucket_name: str, blob_name: str) -> None:
        """Makes a blob publicly accessible."""
        # bucket_name = "your-bucket-name"
        # blob_name = "your-object-name"

        bucket = GcsTools._client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        blob.make_public()

    @staticmethod
    def generate_signed_url(
        bucket_name,
        object_name,
        subresource=None,
        expiration=1000,
        http_method="GET",
        query_parameters=None,
        headers=None,
    ):
        """
        TODO add docs
        """
        if expiration > 604800:
            print("Expiration Time can't be longer than 604800 seconds (7 days).")
            return None

        escaped_object_name = quote(six.ensure_binary(object_name), safe=b"/~")
        canonical_uri = "/{}".format(escaped_object_name)

        datetime_now = datetime.datetime.now(tz=datetime.timezone.utc)
        request_timestamp = datetime_now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = datetime_now.strftime("%Y%m%d")

        google_credentials = GcsTools._creds
        if google_credentials is None:
            print("No service account available for signing.")
            return None

        client_email = google_credentials.service_account_email
        credential_scope = "{}/auto/storage/goog4_request".format(datestamp)
        credential = "{}/{}".format(client_email, credential_scope)

        if headers is None:
            headers = dict()
        host = "{}.storage.googleapis.com".format(bucket_name)
        headers["host"] = host

        canonical_headers = ""
        ordered_headers = collections.OrderedDict(sorted(headers.items()))
        for k, v in ordered_headers.items():
            lower_k = str(k).lower()
            strip_v = str(v).lower()
            canonical_headers += "{}:{}\n".format(lower_k, strip_v)

        signed_headers = ""
        for k, _ in ordered_headers.items():
            lower_k = str(k).lower()
            signed_headers += "{};".format(lower_k)
        signed_headers = signed_headers[:-1]  # remove trailing ';'

        if query_parameters is None:
            query_parameters = dict()
        query_parameters["X-Goog-Algorithm"] = "GOOG4-RSA-SHA256"
        query_parameters["X-Goog-Credential"] = credential
        query_parameters["X-Goog-Date"] = request_timestamp
        query_parameters["X-Goog-Expires"] = expiration
        query_parameters["X-Goog-SignedHeaders"] = signed_headers
        if subresource:
            query_parameters[subresource] = ""

        canonical_query_string = ""
        ordered_query_parameters = collections.OrderedDict(
            sorted(query_parameters.items())
        )
        for k, v in ordered_query_parameters.items():
            encoded_k = quote(str(k), safe="")
            encoded_v = quote(str(v), safe="")
            canonical_query_string += "{}={}&".format(encoded_k, encoded_v)
        canonical_query_string = canonical_query_string[:-1]  # remove trailing '&'

        canonical_request = "\n".join(
            [
                http_method,
                canonical_uri,
                canonical_query_string,
                canonical_headers,
                signed_headers,
                "UNSIGNED-PAYLOAD",
            ]
        )

        canonical_request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()

        string_to_sign = "\n".join(
            [
                "GOOG4-RSA-SHA256",
                request_timestamp,
                credential_scope,
                canonical_request_hash,
            ]
        )

        # signer.sign() signs using RSA-SHA256 with PKCS1v15 padding
        signature = binascii.hexlify(
            google_credentials.signer.sign(string_to_sign)
        ).decode()

        scheme_and_host = "{}://{}".format("https", host)
        signed_url = "{}{}?{}&x-goog-signature={}".format(
            scheme_and_host, canonical_uri, canonical_query_string, signature
        )

        return signed_url
