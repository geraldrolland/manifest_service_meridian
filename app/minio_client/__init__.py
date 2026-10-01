"""MinIO client for the manifest service.

Provides a singleton MinIO client, an upload helper that sends
local files to the configured bucket, and a delete helper that
removes objects from a bucket.
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


def get_object_bytes(bucket_name: str, object_key: str) -> bytes:
    """Read an object from the bucket fully into memory.

    Args:
        bucket_name: The bucket containing the object.
        object_key: The key of the object to read.

    Returns:
        The raw object bytes.

    Raises:
        S3Error: If the object does not exist or cannot be read.
    """
    response = client.get_object(bucket_name, object_key)
    try:
        data = response.read()
    finally:
        response.close()
        response.release_conn()
    logger.debug("Read %d bytes from %s/%s", len(data), bucket_name, object_key)
    return data


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


def delete_object(object_key: str, bucket_name: str) -> None:
    """Delete an object from the specified bucket if it exists.

    Args:
        object_key: The object key to delete.
        bucket_name: The name of the bucket to delete from.

    Raises:
        Exception: Any error other than the object not existing.
    """
    try:
        client.remove_object(
            bucket_name=bucket_name,
            object_name=object_key,
        )
    except Exception as exc:
        # S3Error carries .code; tolerate only "object absent" (checked by
        # attribute, not type, so tests that mock the minio package still work).
        if getattr(exc, "code", None) in ("NoSuchKey", "NoSuchObject"):
            logger.debug("Object %s/%s already absent", bucket_name, object_key)
            return
        raise
    logger.info("Deleted %s/%s", bucket_name, object_key)
