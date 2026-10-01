"""Kafka consumer for the manifest service.

Subscribes to the job.completed topic and persists ManifestTask records
along with a manifest.generating outbox event.

Also subscribes to video.deleted: fetches every manifest task for the
video and runs `cleanup_manifest` (delete MPD objects + rows).
"""

import asyncio
import json
import logging
from typing import Any

from aiokafka import AIOKafkaConsumer, TopicPartition, ConsumerRebalanceListener
from sqlalchemy.exc import IntegrityError

from app.cleanup import cleanup_manifest
from app.config import settings
from app.db_config import async_session_factory, get_sync_session
from app.models.manifest_task import ManifestTask
from app.models.outbox import Outbox
from app.utils import cleanup_local_manifest_dir

logger = logging.getLogger(__name__)


class AppRebalanceListener(ConsumerRebalanceListener):
    """Logs Kafka partition rebalance events for the manifest consumer group."""

    async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        logger.info("Partitions revoked: %s", revoked)

    async def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        logger.info("Partitions assigned: %s", assigned)


def _handle_video_deleted(video_id: str) -> None:
    """Remove every manifest (objects + rows) for a deleted video.

    Runs in a worker thread — MinIO deletes and the sync session block.
    `cleanup_manifest` commits once after all rows are removed; the
    outbox.manifest_id FK cascades any unpublished events with them.
    """
    session = get_sync_session()
    try:
        manifests = (
            session.query(ManifestTask)
            .filter(ManifestTask.video_id == video_id)
            .all()
        )
        cleanup_manifest(session, manifests)
        cleanup_local_manifest_dir(video_id)
    finally:
        session.close()


async def consume_messages(consumer: AIOKafkaConsumer) -> None:
    """Main consumer loop. Runs until cancelled."""
    try:
        async for msg in consumer:
            session = None
            try:
                raw_value: bytes = msg.value
                if raw_value is None:
                    logger.warning("Received empty message at offset %d", msg.offset)
                    await consumer.commit(
                        {TopicPartition(msg.topic, msg.partition): msg.offset + 1}
                    )
                    continue

                event_dict: dict[str, Any] = json.loads(raw_value.decode("utf-8"))

                if msg.topic == settings.kafka_video_deleted_topic:
                    deleted_video_id = event_dict.get("video_id")
                    if not deleted_video_id:
                        logger.warning(
                            "video.deleted message without video_id, skipping"
                        )
                        await consumer.commit(
                            {TopicPartition(msg.topic, msg.partition): msg.offset + 1}
                        )
                        continue
                    await asyncio.to_thread(_handle_video_deleted, deleted_video_id)
                    logger.info(
                        "Cleaned up manifests for deleted video %s", deleted_video_id
                    )
                    await consumer.commit(
                        {TopicPartition(msg.topic, msg.partition): msg.offset + 1}
                    )
                    continue

                event_id = event_dict.get("event_id")
                video_id = event_dict.get("video_id")
                job_id = event_dict.get("job_id")
                thumbnail_url = event_dict.get("thumbnail_url")
                manifest_metadata = event_dict.get("manifest_metadata")
                origin_service = event_dict.get("origin_service")

                if not event_id or not video_id or not job_id:
                    logger.warning(
                        "Missing required fields (event_id, video_id, job_id), skipping"
                    )
                    await consumer.commit(
                        {TopicPartition(msg.topic, msg.partition): msg.offset + 1}
                    )
                    continue

                manifest_task = ManifestTask(
                    id=f"manifest:{event_id}",
                    video_id=video_id,
                    job_id=job_id,
                    task_metadata=manifest_metadata or {},
                )

                outbox = Outbox(
                    topic="manifest.generating",
                    payload={
                        "origin_service": origin_service,
                        "video_id": video_id,
                        "thumbnail_url": thumbnail_url,
                    },
                    manifest_id=manifest_task.id,
                )

                session = async_session_factory()
                session.add(manifest_task)
                session.add(outbox)
                await session.commit()
                await session.close()

                logger.info(
                    "ManifestTask committed id=manifest:%s video_id=%s job_id=%s",
                    event_id,
                    video_id,
                    job_id,
                )

                await consumer.commit(
                    {TopicPartition(msg.topic, msg.partition): msg.offset + 1}
                )

            except IntegrityError:
                logger.warning(
                    "IntegrityError for offset %d, rolling back and committing offset",
                    msg.offset,
                )
                if session is not None:
                    await session.rollback()
                    await session.close()
                await consumer.commit(
                    {TopicPartition(msg.topic, msg.partition): msg.offset + 1}
                )

            except json.JSONDecodeError as e:
                logger.error("Failed to decode message JSON: %s", e)
                await consumer.commit(
                    {TopicPartition(msg.topic, msg.partition): msg.offset + 1}
                )

            except Exception as e:
                logger.error("Error processing message: %s", e, exc_info=True)
                if session is not None:
                    await session.rollback()
                    await session.close()
                await asyncio.sleep(1)

    except asyncio.CancelledError:
        logger.info("Consumer task cancelled")
    finally:
        await consumer.stop()
        logger.info("Consumer stopped")


async def start_consumer() -> None:
    """Create and start the Kafka consumer."""
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group_id,
        auto_offset_reset=settings.kafka_auto_offset_reset,
        enable_auto_commit=False,
        session_timeout_ms=30000,
        max_poll_interval_ms=300000,
        rebalance_timeout_ms=60000,
    )

    await consumer.start()
    consumer.subscribe(
        [settings.kafka_topic, settings.kafka_video_deleted_topic],
        listener=AppRebalanceListener(),
    )
    logger.info(
        "Kafka consumer started — topics=%s group=%s",
        [settings.kafka_topic, settings.kafka_video_deleted_topic],
        settings.kafka_consumer_group_id,
    )

    await consume_messages(consumer)