"""Utility functions for the manifest service."""

import os
import re
import shutil

from app.config import settings


def build_object_url(object_key: str, bucket_name: str) -> str:
    """Build a full MinIO object URL from an object key and bucket name.

    Args:
        object_key: The object key within the bucket.
        bucket_name: The name of the bucket.

    Returns:
        Full URL, e.g. "http://minio:9000/bucket/object_key"
    """
    return f"http://{settings.minio_endpoint}/{bucket_name}/{object_key}"


def resolve_object_key(file_path: str, prefix: str) -> str:
    """Compute the MinIO object key by stripping the first directory prefix.

    File paths look like: /tmp/manifest/{video_id}/manifest_{uuid8}.mpd
    Object key: {video_id}/manifest_{uuid8}.mpd

    For .m4s files, trailing _<8-hex-char uuid> is also stripped.

    Args:
        file_path: The local file path to resolve.
        prefix: The directory prefix to strip (e.g. MANIFEST_DIR).

    Returns:
        The computed object key.
    """
    parts = file_path.replace("\\", "/").split("/")
    prefix = prefix.strip("/").split("/")[-1]
    try:
        idx = parts.index(prefix)
        key = "/".join(parts[idx + 1:])
    except ValueError:
        key = "/".join(parts[-3:]) if len(parts) >= 3 else os.path.basename(file_path)

    key = re.sub(r"_[0-9a-f]{8}(?=\.m4s$)", "", key)
    return key


def cleanup_manifest(video_id: str) -> None:
    """Delete the local manifest output directory for a video if it exists.

    Args:
        video_id: The video whose manifest directory should be removed
            (e.g. "/tmp/manifest/{video_id}").
    """
    path = os.path.join("/tmp/manifest", video_id)
    if os.path.isdir(path):
        shutil.rmtree(path)
