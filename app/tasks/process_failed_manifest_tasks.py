"""Celery task for cleaning up and publishing failed manifest tasks."""

import logging

from app.celery_app import celery_app
from app.db_config import get_sync_session
from app.lock import acquire_lock, release_lock, LockState
from app.models.manifest_task import ManifestTask, ManifestStatus
from app.models.outbox import Outbox
from app.utils import cleanup_manifest

logger = logging.getLogger(__name__)

BATCH_SIZE = 50


@celery_app.task(
    name="app.tasks.process_failed_manifest_tasks",
    acks_late=True,
    reject_on_worker_lost=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=5,
    default_retry_delay=60,
)
def process_failed_manifest_tasks():
    """Process FAILED manifest tasks that haven't been published.

    Flow per task:
        1. Acquire PROCESSING lock
        2. cleanup_manifest(video_id): delete /tmp/manifest/{video_id} if present
        3. Acquire COMMITTING lock
        4. Create Outbox event (topic=manifest.failed, payload={origin_service, manifest_id, video_id})
        5. Set task.published = True
        6. Commit atomically
        7. Release locks
    """
    session = get_sync_session()
    try:
        failed_tasks = (
            session.query(ManifestTask)
            .filter(
                ManifestTask.status == ManifestStatus.FAILED.value,
                ManifestTask.published == False,  # noqa: E712
            )
            .limit(BATCH_SIZE)
            .all()
        )

        if not failed_tasks:
            return {"processed": 0}

        processed = 0

        for task in failed_tasks:
            processing_lock = None
            committing_lock = None
            try:
                processing_lock = acquire_lock(LockState.PROCESSING, task.id)
                if processing_lock is None:
                    logger.debug("PROCESSING lock held for %s, skipping", task.id)
                    continue

                cleanup_manifest(task.video_id)

                committing_lock = acquire_lock(LockState.COMMITTING, task.id)
                if committing_lock is None:
                    logger.warning("COMMITTING lock held for %s, skipping", task.id)
                    continue

                task = session.get(ManifestTask, task.id)
                if (
                    task is None
                    or task.status != ManifestStatus.FAILED.value
                    or task.published
                ):
                    continue

                outbox = Outbox(
                    topic="manifest.failed",
                    payload={
                        "origin_service": "manifest_service",
                        "manifest_id": task.id,
                        "video_id": task.video_id,
                    },
                )
                session.add(outbox)

                task.published = True
                session.commit()
                processed += 1

                logger.info(
                    "Manifest task %s cleanup completed, published=True",
                    task.id,
                )

            except Exception:
                session.rollback()
                logger.exception(
                    "Error processing failed manifest task %s", task.id
                )
            finally:
                if committing_lock is not None:
                    release_lock(committing_lock)
                if processing_lock is not None:
                    release_lock(processing_lock)

        return {"processed": processed}

    finally:
        session.close()
