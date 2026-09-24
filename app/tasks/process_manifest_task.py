"""Celery task for generating and uploading DASH manifests for pending tasks."""

import logging
from datetime import datetime, timedelta, timezone

from app.celery_app import celery_app
from app.db_config import get_sync_session
from app.generating_manifest import GenerateManifest
from app.lock import acquire_lock, release_lock, LockState
from app.minio_client import upload_object
from app.models.manifest_task import ManifestTask, ManifestStatus
from app.utils import build_object_url, resolve_object_key

logger = logging.getLogger(__name__)

MANIFEST_DIR = "/tmp/manifest"
MANIFEST_BUCKET = "manifest"
GEN_MANIFEST_MAX_RETRIES = 5


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
    2. Generate DASH manifest via GenerateManifest (output to MANIFEST_DIR)
    3. Resolve object key and upload to MANIFEST_BUCKET
    4. Acquire COMMITTING lock
    5. Set status=COMPLETED and manifest_url (published stays False)
    6. Commit atomically
    7. On exception: rollback, increment num_of_retries, set retry_after
       or FAILED when max retries reached
    8. Release locks
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
                generator = GenerateManifest(
                    video_id=task.video_id,
                    media_prefix=md["media_prefix"],
                    output_dir=MANIFEST_DIR,
                    segment_duration=md["segment_duration"],
                    renditions=md["renditions"],
                    segment_prefix=md["segment_filename_prefix"],
                    video_duration=md["video_duration"],
                    framerate=md["framerate"],
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
