import logging
from pathlib import Path
from typing import List

from google.oauth2.credentials import Credentials
from google.cloud import storage
from google.cloud import exceptions
from google.cloud.storage import Client, Bucket, Blob

from config import CONFIGURATION
from Authentication.Authentication import Authentication
from GoogleCloudStorage.Exceptions import *


class GoogleCloudStorage():
    ''' Google Cloud Storage Wrapper Class '''

    scopes: List[str]    
    credentials: Credentials
    project_id: str
    client: Client

    def __init__(self) -> None:

        auth_handler = Authentication()
        self.scopes = CONFIGURATION.get("SCOPES").get("GOOGLE_CLOUD_STORAGE")
        self.credentials = auth_handler.getServiceAccountCredentials(scopes=self.scopes)
        self.project_id = CONFIGURATION.get("PROJECT_ID")
        self.client = storage.Client(credentials=self.credentials, project=self.project_id)

    def downloadFile(self, bucket_name: str, blob_name: str, download_path: Path) -> Path:
        ''' Download File From GoogleCloudStorage 
            Arguments:
                - bucket_name (str) : Name of the bucket where data has to be downloaded from
                - blob_name (str) : Blob Name for the file to be downloaded
                - download_path (pathlib.path) : Path where data has to be downloaded
            Returns:
                - file_path (pathlib.Path) : Path to file where data has been downloaded
        '''

        bucket: Bucket = self.client.bucket(bucket_name=bucket_name)

        try:
            logging.debug(f"[+] Downloading File: {bucket_name}/{blob_name} ...")
            blob: Blob = bucket.get_blob(blob_name=blob_name)
            blob.download_to_filename(str(download_path))
            logging.debug(f"[+] File Downloaded: {bucket_name}/{blob_name}")
            return download_path
        except exceptions.NotFound as error:
            logging.error(f"[-] Unable to Find File: {bucket_name}/{blob_name}. Encountered Error: {error}")
            raise DownloadFileError(error)
        except exceptions.Forbidden as error:
            logging.error(f"[-] File {bucket_name}/{blob_name} is forbidden or invalid. Encounterd Error: {error}")
            raise DownloadFileError(error)
        except Exception as error:
            logging.error(f"[-] Unable to Download File: {bucket_name}/{blob_name}. Encountered Error: {error}")
            raise DownloadFileError(error)
            

    def getMetadata(self, bucket_name: str, blob_name: str) -> dict:
        ''' Get Metadata of a file in GoogleCloudStorage
            Arguments:
                - bucket_name (str) : Name of the bucket 
                - blob_name (str) : Name of the file whose metadata is required
            Returns:
                Metadata (dict) : Metadata object 
        '''

        bucket: Bucket = self.client.bucket(bucket_name=bucket_name)

        try:
            logging.debug(f"[+] Fetching Metadata for File: {bucket_name}/{blob_name} ...")
            blob: Blob = bucket.get_blob(blob_name=blob_name)
            metadata: dict =  blob.metadata
            metadata["size"] = blob.size
            metadata["created_ts"] = blob.time_created.timestamp
            metadata["filename"] = blob_name.split("|")[-1]
            logging.debug(f"[+] Fetched Metadata: {bucket_name}/{blob_name}")
            return metadata
        except exceptions.NotFound as error:
            logging.error(f"[-] Unable to Find File: {bucket_name}/{blob_name}. Encountered Error: {error}")
            raise MetadataError(error)
        except exceptions.Forbidden as error:
            logging.error(f"[-] File {bucket_name}/{blob_name} is forbidden or invalid. Encounterd Error: {error}")
            raise MetadataError(error)
        except Exception as error:
            logging.error(f"[-] Unable to Fetch Metdata: {bucket_name}/{blob_name}. Encountered Error: {error}")
            raise MetadataError(error)

    def uploadFile(self, bucket_name: str, blob_name: str, filepath: Path, metadata: dict = {}) -> None:
        ''' Download File From GoogleCloudStorage 
            Arguments:
                - bucket_name (str) : Name of the bucket where data has to be uploaded
                - blob_name (str) : Blob Name for the file to be uploaded
                - filepath (pathlib.Path) : Path of the file whose data has to be uploaded
        '''

        logging.debug(f"[+] Uploading File : {filepath}...")
        bucket: Bucket = self.client.bucket(bucket_name=bucket_name)

        try:
            blob: Blob = bucket.blob(blob_name=blob_name)
            blob.metadata = metadata
            blob.upload_from_filename(str(filepath))
        except Exception as error:
            logging.error(f"[-] Unable to Upload File: {filepath}")
            UploadFileError(error)
        
        logging.debug(f"[+] File Uploaded : {blob_name} !!!")