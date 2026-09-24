"""Tests for app.consumer -- Kafka consume_messages loop."""

import asyncio
import json
import sys
from unittest.mock import patch, MagicMock, AsyncMock

from sqlalchemy.exc import IntegrityError

sys.modules.setdefault("asyncpg", MagicMock())
sys.modules.setdefault("minio", MagicMock())
sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("aiokafka", MagicMock())


class _FakeMsg:
    def __init__(self, value, topic="job.completed", partition=0, offset=0):
        self.value = value
        self.topic = topic
        self.partition = partition
        self.offset = offset


class _FakeConsumer:
    def __init__(self, messages):
        self._messages = list(messages)
        self.commit = AsyncMock()
        self.stop = AsyncMock()

    def __aiter__(self):
        self._iter = iter(self._messages)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise asyncio.CancelledError


def _run(consumer):
    asyncio.run(_consume(consumer))


async def _consume(consumer):
    from app.consumer import consume_messages
    await consume_messages(consumer)


def _payload(**overrides):
    base = {
        "event_id": "evt1",
        "video_id": "vid1",
        "job_id": "job:1",
        "thumbnail_url": "http://minio:9000/vidthumbnails/vid1/thumb.jpg",
        "manifest_metadata": {"framerate": 30},
        "origin_service": "media_processing_service",
    }
    base.update(overrides)
    return base


def _make_session():
    session = MagicMock()
    session.add = MagicMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    return session


class TestConsumeMessages:

    def test_null_value_commits_and_skips_db(self):
        consumer = _FakeConsumer([_FakeMsg(None)])
        with patch("app.consumer.async_session_factory") as mock_factory:
            _run(consumer)
        mock_factory.assert_not_called()
        consumer.commit.assert_awaited()
        consumer.stop.assert_awaited()

    def test_missing_required_fields_commits_and_skips_db(self):
        msg = _FakeMsg(json.dumps({"event_id": "evt1", "video_id": None, "job_id": "job:1"}).encode())
        consumer = _FakeConsumer([msg])
        with patch("app.consumer.async_session_factory") as mock_factory:
            _run(consumer)
        mock_factory.assert_not_called()
        consumer.commit.assert_awaited()
        consumer.stop.assert_awaited()

    def test_happy_path_persists_task_and_outbox(self):
        msg = _FakeMsg(json.dumps(_payload()).encode())
        consumer = _FakeConsumer([msg])
        session = _make_session()
        with patch("app.consumer.async_session_factory", return_value=session):
            _run(consumer)

        assert session.add.call_count == 2
        added_task = session.add.call_args_list[0][0][0]
        added_outbox = session.add.call_args_list[1][0][0]
        assert added_task.id == "manifest:evt1"
        assert added_task.video_id == "vid1"
        assert added_task.job_id == "job:1"
        assert added_task.task_metadata == {"framerate": 30}
        assert added_outbox.topic == "manifest.generating"
        assert added_outbox.payload["origin_service"] == "media_processing_service"
        assert added_outbox.payload["video_id"] == "vid1"
        assert added_outbox.payload["thumbnail_url"] == (
            "http://minio:9000/vidthumbnails/vid1/thumb.jpg"
        )
        session.commit.assert_awaited_once()
        session.close.assert_awaited_once()
        consumer.commit.assert_awaited()
        consumer.stop.assert_awaited()

    def test_null_manifest_metadata_becomes_empty_dict(self):
        payload = _payload()
        payload["manifest_metadata"] = None
        msg = _FakeMsg(json.dumps(payload).encode())
        consumer = _FakeConsumer([msg])
        session = _make_session()
        with patch("app.consumer.async_session_factory", return_value=session):
            _run(consumer)

        added_task = session.add.call_args_list[0][0][0]
        assert added_task.task_metadata == {}

    def test_json_decode_error_commits_offset(self):
        msg = _FakeMsg(b"not-json{")
        consumer = _FakeConsumer([msg])
        with patch("app.consumer.async_session_factory") as mock_factory:
            _run(consumer)
        mock_factory.assert_not_called()
        consumer.commit.assert_awaited()
        consumer.stop.assert_awaited()

    def test_integrity_error_rolls_back_and_commits_offset(self):
        msg = _FakeMsg(json.dumps(_payload()).encode())
        consumer = _FakeConsumer([msg])
        session = _make_session()
        session.commit = AsyncMock(
            side_effect=IntegrityError("INSERT", {}, Exception("duplicate"))
        )
        with patch("app.consumer.async_session_factory", return_value=session):
            _run(consumer)

        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()
        consumer.commit.assert_awaited()
        consumer.stop.assert_awaited()

    def test_generic_error_sleeps_without_kafka_commit(self):
        msg = _FakeMsg(json.dumps(_payload()).encode())
        consumer = _FakeConsumer([msg])
        session = _make_session()
        session.commit = AsyncMock(side_effect=Exception("db down"))
        with patch("app.consumer.async_session_factory", return_value=session), \
             patch("asyncio.sleep", new=AsyncMock()) as mock_sleep:
            _run(consumer)

        mock_sleep.assert_awaited_with(1)
        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()
        consumer.commit.assert_not_awaited()
        consumer.stop.assert_awaited()

    def test_cancelled_stops_consumer(self):
        consumer = _FakeConsumer([])
        _run(consumer)
        consumer.stop.assert_awaited_once()
        consumer.commit.assert_not_awaited()
