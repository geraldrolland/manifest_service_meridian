"""Celery task for publishing outbox events to Kafka."""

import logging
from datetime import datetime, timedelta, timezone

from kafka.errors import NoBrokersAvailable

from app.celery_app import celery_app
from app.db_config import get_sync_session
from app.lock import acquire_lock, release_lock, LockState
from app.models.manifest_task import ManifestTask, ManifestStatus
from app.models.outbox import Outbox, OutboxStatus
from app.producer import kafka_producer

logger = logging.getLogger(__name__)

BATCH_SIZE = 100


@celery_app.task(
    name="app.tasks.process_outbox_task",
    acks_late=True,
    reject_on_worker_lost=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=5,
    default_retry_delay=60,
)
def process_outbox_task():
    """Process PENDING outbox events in batches of 100.

    For each event:
    1. Acquire Redis lock (2 min TTL)
    2. Publish to Kafka
    3. If success -> set status=PROCESSED
    4. If NoBrokersAvailable -> raise, let Celery retry the task later
    5. If other error -> increment num_of_retry
       - If num_of_retry >= 5 -> status=FAILED; if payload has manifest_id,
         set that ManifestTask status=FAILED and published=False
       - Else -> set retry_after=now+2min
    6. Commit atomically
    7. Release lock
    """
    session = get_sync_session()
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        pending = (
            session.query(Outbox)
            .filter(
                Outbox.status == OutboxStatus.PENDING.value,
                (Outbox.retry_after.is_(None)) | (Outbox.retry_after < now),
            )
            .limit(BATCH_SIZE)
            .all()
        )

        if not pending:
            return {"processed": 0}

        processed = 0

        for event in pending:
            lock = acquire_lock(LockState.COMMITTING, event.id)
            if lock is None:
                continue

            try:
                outbox = session.get(Outbox, event.id)
                if outbox is None or outbox.status != OutboxStatus.PENDING.value:
                    continue

                # Publish to Kafka synchronously
                kafka_producer.publish(outbox.topic, outbox.payload)

                outbox.status = OutboxStatus.PROCESSED.value
                session.commit()
                processed += 1
                logger.info("Processed outbox event %s", outbox.id)

            except NoBrokersAvailable:
                logger.warning(
                    "Kafka broker unavailable, skipping outbox event %s", event.id
                )
                raise  # Let Celery retry the task later

            except Exception:
                session.rollback()
                try:
                    outbox = session.get(Outbox, event.id)
                    if outbox:
                        new_count = (outbox.num_of_retry or 0) + 1
                        if new_count >= 5:
                            outbox.num_of_retry = 5
                            outbox.retry_after = None
                            outbox.status = OutboxStatus.FAILED.value

                            manifest_id = (outbox.payload or {}).get("manifest_id")
                            if manifest_id:
                                manifest = session.get(ManifestTask, manifest_id)
                                if manifest is not None:
                                    manifest.status = ManifestStatus.FAILED.value
                                    manifest.published = False
                        else:
                            outbox.num_of_retry = new_count
                            outbox.retry_after = (
                                datetime.now(timezone.utc).replace(tzinfo=None)
                                + timedelta(minutes=2)
                            )
                        session.commit()
                except Exception:
                    session.rollback()
                    logger.exception("Error processing outbox event %s", event.id)
                    raise
            finally:
                release_lock(lock)

        return {"processed": processed}

    finally:
        session.close()