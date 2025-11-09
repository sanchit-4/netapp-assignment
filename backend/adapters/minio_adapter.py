from typing import IO, Tuple
import boto3
from botocore.client import Config
from config import settings
from adapters.base import StorageAdapter

class MinioAdapter(StorageAdapter):
    """Adapter for interacting with a MinIO storage bucket."""

    def __init__(self):
        self.bucket_name = "private-cloud" # As per hackathon plan
        self.client = boto3.client(
            "s3",
            endpoint_url=f"http://{settings.minio_endpoint}",
            aws_access_key_id=settings.minio_access_key,
            aws_secret_access_key=settings.minio_root_password,
            config=Config(signature_version="s3v4"),
        )
        self._create_bucket_if_not_exists()

    def _create_bucket_if_not_exists(self):
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
        except self.client.exceptions.ClientError as e:
            # If a 404 error is raised, the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                self.client.create_bucket(Bucket=self.bucket_name)

    def put(self, key: str, data: IO[bytes]) -> Tuple[str, str]:
        self.client.upload_fileobj(data, self.bucket_name, key)
        path = f"s3://{self.bucket_name}/{key}"
        checksum = self.get_checksum(key)
        return path, checksum

    def get(self, key: str) -> IO[bytes]:
        response = self.client.get_object(Bucket=self.bucket_name, Key=key)
        return response['Body']

    def delete(self, key: str):
        self.client.delete_object(Bucket=self.bucket_name, Key=key)

    def metadata(self, key: str) -> dict:
        response = self.client.head_object(Bucket=self.bucket_name, Key=key)
        return {
            "size_bytes": response['ContentLength'],
            "last_accessed_at": response['LastModified'],
            "checksum": response['ETag'].strip('"'),
        }

    def get_checksum(self, key: str) -> str:
        response = self.client.head_object(Bucket=self.bucket_name, Key=key)
        return response['ETag'].strip('"')
