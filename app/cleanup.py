"""Video-deletion cleanup: remove manifest objects from MinIO and their rows.

Consumed by the Kafka `video.deleted` handler in `app.consumer`.
"""

import logging
from urllib.parse import urlparse

from app.minio_client import delete_object

logger = logging.getLogger(__name__)


def _object_key_from_url(url: str) -> tuple[str, str] | None:
    """Split ``http://minio:9000/{bucket}/{key}`` into ``(bucket, key)``."""
    parsed = urlparse(url)
    path_parts = parsed.path.lstrip("/").split("/", 1)
    if len(path_parts) != 2 or not path_parts[0] or not path_parts[1]:
        return None
    return path_parts[0], path_parts[1]


def cleanup_manifest(db, manifests) -> None:
    """Delete manifest objects (if any) and their DB rows, then commit.

    Args:
        db: Open database session with the manifests loaded.
        manifests: ManifestTask-like rows to remove. Rows without a
            ``manifest_url`` have no object to delete.
    """
    for manifest in manifests:
        if manifest.manifest_url:
            target = _object_key_from_url(manifest.manifest_url)
            if target is None:
                logger.warning(
                    "Skipping object delete for %s — unparseable url %r",
                    manifest.id,
                    manifest.manifest_url,
                )
            else:
                bucket, key = target
                delete_object(key, bucket)
        db.delete(manifest)
    db.commit()
    logger.info("Cleaned up %d manifest(s)", len(manifests))
