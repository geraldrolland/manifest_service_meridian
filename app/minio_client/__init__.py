"""MinIO client for the manifest service.

Provides a singleton MinIO client and an upload helper that sends
local files to the configured bucket.
"""

import logging

from minio import Minio

from app.config import settings

logger = logging.getLogger(__name__)

client = Minio(
    endpoint=settings.minio_endpoint,
    access_key=settings.minio_access_key,
    secret_key=settings.minio_secret_key,
    secure=settings.minio_secure,
)


def upload_object(file_path: str, object_key: str, bucket_name: str) -> str:
    """Upload a local file to the specified bucket.

    Args:
        file_path: Local path of the file to upload.
        object_key: The destination object key in the bucket.
        bucket_name: The name of the bucket to upload to.

    Returns:
        The object key.
    """
    client.fput_object(
        bucket_name=bucket_name,
        object_name=object_key,
        file_path=file_path,
    )
    logger.info(
        "Uploaded %s → %s/%s",
        file_path,
        bucket_name,
        object_key,
    )
    return object_key
