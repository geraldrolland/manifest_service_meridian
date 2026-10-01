"""Celery task for generating and uploading DASH manifests for pending tasks."""

import logging
from datetime import datetime, timedelta, timezone

from app.celery_app import celery_app
from app.codecs import codec_from_init
from app.db_config import get_sync_session
from app.generating_manifest import GenerateManifest
from app.lock import acquire_lock, release_lock, LockState
from app.minio_client import get_object_bytes, upload_object
from app.models.manifest_task import ManifestTask, ManifestStatus
from app.utils import build_object_url, resolve_object_key

logger = logging.getLogger(__name__)

MANIFEST_DIR = "/tmp/manifest"
MANIFEST_BUCKET = "manifest"
GEN_MANIFEST_MAX_RETRIES = 5


def _split_media_prefix(media_prefix: str) -> tuple[str, str]:
    """Split "bucket/key/prefix/" into ("bucket", "key/prefix/")."""
    parts = media_prefix.strip("/").split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"invalid media_prefix: {media_prefix!r}")
    return parts[0], f"{parts[1].rstrip('/')}/"


def _resolve_codecs(md: dict) -> tuple[dict, str | None]:
    """Resolve @codecs values for every rendition (and audio).

    Probes each rendition's init.mp4 in MinIO and parses the codec string
    from its avcC/esds box. Skips probing when manifest_metadata already
    carries codec strings for every rendition.

    Args:
        md: manifest_metadata for the task.

    Returns:
        Tuple of (renditions with "codecs" filled in, audio codec string).

    Raises:
        CodecError / S3Error: propagated so the task retries, then fails
            rather than publishing a manifest players cannot decode.
    """
    renditions = {
        name: dict(props) for name, props in (md.get("renditions") or {}).items()
    }
    if not renditions:
        raise ValueError("manifest_metadata has no renditions")

    if not all(props.get("codecs") for props in renditions.values()):
        bucket, key_prefix = _split_media_prefix(md["media_prefix"])
        for name in renditions:
            init_bytes = get_object_bytes(bucket, f"{key_prefix}{name}/init.mp4")
            renditions[name]["codecs"] = codec_from_init(init_bytes, kind="video")
            logger.debug("Probed codec for %s: %s", name, renditions[name]["codecs"])

    audio_codecs = md.get("audio_codecs")
    if md.get("has_audio", True) and not audio_codecs:
        bucket, key_prefix = _split_media_prefix(md["media_prefix"])
        audio_bytes = get_object_bytes(bucket, f"{key_prefix}audio/init.mp4")
        audio_codecs = codec_from_init(audio_bytes, kind="audio")
        logger.debug("Probed audio codec: %s", audio_codecs)

    return renditions, audio_codecs


@celery_app.task(
    name="app.tasks.process_manifest_task",
    acks_late=True,
    reject_on_worker_lost=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=5,
    default_retry_delay=60,
)
def process_manifest_task():
    """Process PENDING manifest tasks in batches of 50.

    For each task:
    1. Acquire PROCESSING lock
    2. Resolve @codecs by probing each rendition's init.mp4 in MinIO
    3. Generate DASH manifest via GenerateManifest (output to MANIFEST_DIR)
    4. Resolve object key and upload to MANIFEST_BUCKET
    5. Acquire COMMITTING lock
    6. Set status=COMPLETED and manifest_url (published stays False)
    7. Commit atomically
    8. On exception: rollback, increment num_of_retries, set retry_after
       or FAILED when max retries reached
    9. Release locks
    """
    session = get_sync_session()
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        pending = (
            session.query(ManifestTask)
            .filter(
                ManifestTask.status == ManifestStatus.PENDING.value,
                (ManifestTask.retry_after.is_(None))
                | (ManifestTask.retry_after < now),
            )
            .limit(50)
            .all()
        )

        if not pending:
            return {"processed": 0}

        processed = 0

        for task in pending:
            processing_lock = None
            committing_lock = None
            try:
                processing_lock = acquire_lock(LockState.PROCESSING, task.id)
                if processing_lock is None:
                    logger.debug("PROCESSING lock held for %s, skipping", task.id)
                    continue

                md = task.task_metadata or {}
                renditions, audio_codecs = _resolve_codecs(md)
                generator = GenerateManifest(
                    video_id=task.video_id,
                    media_prefix=md["media_prefix"],
                    output_dir=MANIFEST_DIR,
                    segment_duration=md["segment_duration"],
                    renditions=renditions,
                    segment_prefix=md["segment_filename_prefix"],
                    video_duration=md["video_duration"],
                    framerate=md["framerate"],
                    has_audio=md.get("has_audio", True),
                    audio_codecs=audio_codecs,
                )
                mpd_path = generator.generate_manifest()

                object_key = resolve_object_key(mpd_path, MANIFEST_DIR)
                upload_object(mpd_path, object_key, MANIFEST_BUCKET)

                committing_lock = acquire_lock(LockState.COMMITTING, task.id)
                if committing_lock is None:
                    logger.warning("COMMITTING lock held for %s, skipping", task.id)
                    continue

                task = session.get(ManifestTask, task.id)
                if task is None or task.status != ManifestStatus.PENDING.value:
                    continue

                task.status = ManifestStatus.COMPLETED.value
                task.manifest_url = build_object_url(object_key, MANIFEST_BUCKET)
                session.commit()
                processed += 1
                logger.info(
                    "Generated manifest for task %s url=%s",
                    task.id,
                    task.manifest_url,
                )

            except Exception:
                session.rollback()
                logger.exception(
                    "Error processing manifest task %s", task.id
                )
                try:
                    failed_task = session.get(ManifestTask, task.id)
                    if failed_task:
                        new_count = (failed_task.num_of_retries or 0) + 1
                        if new_count >= GEN_MANIFEST_MAX_RETRIES:
                            failed_task.num_of_retries = GEN_MANIFEST_MAX_RETRIES
                            failed_task.retry_after = None
                            failed_task.status = ManifestStatus.FAILED.value
                        else:
                            failed_task.num_of_retries = new_count
                            failed_task.retry_after = (
                                datetime.now(timezone.utc).replace(tzinfo=None)
                                + timedelta(minutes=2)
                            )
                        session.commit()
                except Exception:
                    session.rollback()
                    logger.exception(
                        "Error processing manifest task %s", task.id
                    )
                    raise
            finally:
                if committing_lock is not None:
                    release_lock(committing_lock)
                if processing_lock is not None:
                    release_lock(processing_lock)

        return {"processed": processed}

    finally:
        session.close()
