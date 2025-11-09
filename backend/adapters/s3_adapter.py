from typing import IO, Tuple
import boto3
from config import settings
from adapters.base import StorageAdapter

class S3Adapter(StorageAdapter):
    """Adapter for interacting with an AWS S3 bucket."""

    def __init__(self):
        if not settings.aws_region or not settings.aws_s3_bucket_name or not settings.aws_access_key_id or not settings.aws_secret_access_key:
            raise ValueError(
                "AWS_REGION, AWS_S3_BUCKET_NAME, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY must be set in the .env file"
            )
        
        self.bucket_name = settings.aws_s3_bucket_name
        self.client = boto3.client(
            "s3",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )

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
