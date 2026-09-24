"""Tests for app.tasks module -- all 4 Celery tasks."""

import sys
from unittest.mock import patch, MagicMock

import pytest

sys.modules["asyncpg"] = MagicMock()
sys.modules["minio"] = MagicMock()
sys.modules["redis"] = MagicMock()
sys.modules["aiokafka"] = MagicMock()


class _FakeNoBrokersAvailable(Exception):
    pass


_kafka_mock = MagicMock()
_kafka_errors_mock = MagicMock()
_kafka_errors_mock.NoBrokersAvailable = _FakeNoBrokersAvailable
sys.modules["kafka"] = _kafka_mock
sys.modules["kafka.errors"] = _kafka_errors_mock

from app.models.manifest_task import ManifestTask, ManifestStatus
from app.models.outbox import Outbox, OutboxStatus


def _make_manifest_task(
    task_id="manifest:1",
    video_id="vid1",
    job_id="job:1",
    status=ManifestStatus.PENDING,
    published=False,
    retries=0,
    manifest_url=None,
    metadata=None,
):
    t = MagicMock(spec=ManifestTask)
    t.id = task_id
    t.video_id = video_id
    t.job_id = job_id
    t.status = status.value
    t.published = published
    t.num_of_retries = retries
    t.retry_after = None
    t.manifest_url = manifest_url
    t.task_metadata = metadata if metadata is not None else {
        "media_prefix": "vidsegments/vid1",
        "segment_duration": 4,
        "renditions": {"720p": {"width": 1280, "height": 720, "bitrate": "800k"}},
        "segment_filename_prefix": "seg_",
        "video_duration": 60.0,
        "framerate": 30,
    }
    return t


def _make_outbox(event_id="ob1", topic="manifest.completed", payload=None, num_of_retry=0):
    o = MagicMock(spec=Outbox)
    o.id = event_id
    o.topic = topic
    o.payload = payload if payload is not None else {}
    o.status = OutboxStatus.PENDING.value
    o.num_of_retry = num_of_retry
    o.retry_after = None
    return o


# ---------------------------------------------------------------------------
# TestProcessManifestTask
# ---------------------------------------------------------------------------

class TestProcessManifestTask:

    @patch("app.tasks.process_manifest_task.get_sync_session")
    def test_no_pending_tasks(self, mock_get_session):
        session = mock_get_session.return_value
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = []

        from app.tasks.process_manifest_task import process_manifest_task
        result = process_manifest_task()

        assert result == {"processed": 0}
        session.close.assert_called_once()

    @patch("app.tasks.process_manifest_task.build_object_url", return_value="http://minio:9000/manifest/vid1/manifest_abc.mpd")
    @patch("app.tasks.process_manifest_task.upload_object")
    @patch("app.tasks.process_manifest_task.resolve_object_key", return_value="vid1/manifest_abc.mpd")
    @patch("app.tasks.process_manifest_task.GenerateManifest")
    @patch("app.tasks.process_manifest_task.release_lock")
    @patch("app.tasks.process_manifest_task.acquire_lock")
    @patch("app.tasks.process_manifest_task.get_sync_session")
    def test_successful_generation(
        self, mock_get_session, mock_acquire, mock_release,
        mock_gen_cls, mock_resolve, mock_upload, mock_build_url,
    ):
        session = mock_get_session.return_value
        task = _make_manifest_task()
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [task]
        session.get.return_value = task
        mock_acquire.return_value = MagicMock()
        mock_gen = mock_gen_cls.return_value
        mock_gen.generate_manifest.return_value = "/tmp/manifest/vid1/manifest_abc.mpd"

        from app.tasks.process_manifest_task import process_manifest_task
        result = process_manifest_task()

        assert result["processed"] == 1
        mock_gen_cls.assert_called_once()
        call_kwargs = mock_gen_cls.call_args.kwargs
        assert call_kwargs["video_id"] == "vid1"
        assert call_kwargs["framerate"] == 30
        mock_gen.generate_manifest.assert_called_once()
        mock_upload.assert_called_once()
        assert task.status == ManifestStatus.COMPLETED.value
        assert task.manifest_url == "http://minio:9000/manifest/vid1/manifest_abc.mpd"
        assert task.published is False
        session.commit.assert_called()
        session.close.assert_called_once()
        assert mock_acquire.call_count == 2
        assert mock_release.call_count == 2

    @patch("app.tasks.process_manifest_task.get_sync_session")
    def test_processing_lock_failed(self, mock_get_session):
        session = mock_get_session.return_value
        task = _make_manifest_task()
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [task]

        with patch("app.tasks.process_manifest_task.acquire_lock", return_value=None) as mock_acquire, \
             patch("app.tasks.process_manifest_task.release_lock") as mock_release:
            from app.tasks.process_manifest_task import process_manifest_task
            result = process_manifest_task()

        assert result == {"processed": 0}
        assert mock_acquire.call_count == 1
        mock_release.assert_not_called()
        session.close.assert_called_once()

    @patch("app.tasks.process_manifest_task.get_sync_session")
    def test_committing_lock_failed(self, mock_get_session):
        session = mock_get_session.return_value
        task = _make_manifest_task()
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [task]

        processing_lock = MagicMock()
        with patch("app.tasks.process_manifest_task.acquire_lock", side_effect=[processing_lock, None]) as mock_acquire, \
             patch("app.tasks.process_manifest_task.release_lock") as mock_release, \
             patch("app.tasks.process_manifest_task.GenerateManifest") as mock_gen_cls, \
             patch("app.tasks.process_manifest_task.upload_object"), \
             patch("app.tasks.process_manifest_task.resolve_object_key", return_value="k.mpd"):
            mock_gen_cls.return_value.generate_manifest.return_value = "/tmp/manifest/vid1/manifest_abc.mpd"
            from app.tasks.process_manifest_task import process_manifest_task
            result = process_manifest_task()

        assert result == {"processed": 0}
        assert mock_acquire.call_count == 2
        mock_release.assert_called_once_with(processing_lock)
        session.close.assert_called_once()

    @patch("app.tasks.process_manifest_task.get_sync_session")
    def test_refetch_status_changed(self, mock_get_session):
        session = mock_get_session.return_value
        task = _make_manifest_task()
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [task]
        refetched = _make_manifest_task(status=ManifestStatus.COMPLETED)
        session.get.return_value = refetched

        with patch("app.tasks.process_manifest_task.acquire_lock", return_value=MagicMock()), \
             patch("app.tasks.process_manifest_task.release_lock"), \
             patch("app.tasks.process_manifest_task.GenerateManifest") as mock_gen_cls, \
             patch("app.tasks.process_manifest_task.upload_object"), \
             patch("app.tasks.process_manifest_task.resolve_object_key", return_value="k.mpd"):
            mock_gen_cls.return_value.generate_manifest.return_value = "/tmp/manifest/vid1/manifest_abc.mpd"
            from app.tasks.process_manifest_task import process_manifest_task
            result = process_manifest_task()

        assert result == {"processed": 0}
        session.commit.assert_not_called()
        session.close.assert_called_once()

    @patch("app.tasks.process_manifest_task.get_sync_session")
    def test_failure_increments_retry(self, mock_get_session):
        session = mock_get_session.return_value
        task = _make_manifest_task(retries=0)
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [task]
        session.get.return_value = task

        with patch("app.tasks.process_manifest_task.acquire_lock", return_value=MagicMock()), \
             patch("app.tasks.process_manifest_task.release_lock"), \
             patch("app.tasks.process_manifest_task.GenerateManifest") as mock_gen_cls:
            mock_gen_cls.side_effect = Exception("generation failed")
            from app.tasks.process_manifest_task import process_manifest_task
            result = process_manifest_task()

        assert result == {"processed": 0}
        session.rollback.assert_called()
        assert task.num_of_retries == 1
        assert task.retry_after is not None
        session.commit.assert_called()
        session.close.assert_called_once()

    @patch("app.tasks.process_manifest_task.get_sync_session")
    def test_failure_terminal_at_max_retries(self, mock_get_session):
        session = mock_get_session.return_value
        task = _make_manifest_task(retries=4)
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [task]
        session.get.return_value = task

        with patch("app.tasks.process_manifest_task.acquire_lock", return_value=MagicMock()), \
             patch("app.tasks.process_manifest_task.release_lock"), \
             patch("app.tasks.process_manifest_task.GenerateManifest") as mock_gen_cls:
            mock_gen_cls.side_effect = Exception("generation failed")
            from app.tasks.process_manifest_task import process_manifest_task
            result = process_manifest_task()

        assert result == {"processed": 0}
        assert task.num_of_retries == 5
        assert task.retry_after is None
        assert task.status == ManifestStatus.FAILED.value
        session.commit.assert_called()
        session.close.assert_called_once()


# ---------------------------------------------------------------------------
# TestProcessOutboxTask
# ---------------------------------------------------------------------------

class TestProcessOutboxTask:

    @patch("app.tasks.process_outbox_task.get_sync_session")
    def test_no_pending_events(self, mock_get_session):
        session = mock_get_session.return_value
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = []

        from app.tasks.process_outbox_task import process_outbox_task
        result = process_outbox_task()

        assert result == {"processed": 0}
        session.close.assert_called_once()

    @patch("app.tasks.process_outbox_task.release_lock")
    @patch("app.tasks.process_outbox_task.acquire_lock")
    @patch("app.tasks.process_outbox_task.kafka_producer")
    @patch("app.tasks.process_outbox_task.get_sync_session")
    def test_successful_publish(self, mock_get_session, mock_kafka, mock_acquire, mock_release):
        session = mock_get_session.return_value
        outbox = _make_outbox()
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [outbox]
        session.get.return_value = outbox
        mock_acquire.return_value = MagicMock()

        from app.tasks.process_outbox_task import process_outbox_task
        result = process_outbox_task()

        assert result["processed"] == 1
        assert outbox.status == OutboxStatus.PROCESSED.value
        mock_kafka.publish.assert_called_once_with(outbox.topic, outbox.payload)
        session.commit.assert_called()
        session.close.assert_called_once()

    @patch("app.tasks.process_outbox_task.release_lock")
    @patch("app.tasks.process_outbox_task.acquire_lock", return_value=None)
    @patch("app.tasks.process_outbox_task.kafka_producer")
    @patch("app.tasks.process_outbox_task.get_sync_session")
    def test_lock_failed_skips(self, mock_get_session, mock_kafka, mock_acquire, mock_release):
        session = mock_get_session.return_value
        outbox = _make_outbox()
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [outbox]

        from app.tasks.process_outbox_task import process_outbox_task
        result = process_outbox_task()

        assert result == {"processed": 0}
        mock_kafka.publish.assert_not_called()
        mock_release.assert_not_called()
        session.close.assert_called_once()

    @patch("app.tasks.process_outbox_task.release_lock")
    @patch("app.tasks.process_outbox_task.acquire_lock")
    @patch("app.tasks.process_outbox_task.kafka_producer")
    @patch("app.tasks.process_outbox_task.get_sync_session")
    def test_failure_increments_retry(self, mock_get_session, mock_kafka, mock_acquire, mock_release):
        session = mock_get_session.return_value
        outbox = _make_outbox(num_of_retry=0)
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [outbox]
        session.get.return_value = outbox
        mock_acquire.return_value = MagicMock()
        mock_kafka.publish.side_effect = Exception("kafka unavailable")

        from app.tasks.process_outbox_task import process_outbox_task
        result = process_outbox_task()

        assert result["processed"] == 0
        assert outbox.num_of_retry == 1
        assert outbox.retry_after is not None
        assert outbox.status == OutboxStatus.PENDING.value
        session.commit.assert_called()
        session.close.assert_called_once()

    @patch("app.tasks.process_outbox_task.release_lock")
    @patch("app.tasks.process_outbox_task.acquire_lock")
    @patch("app.tasks.process_outbox_task.kafka_producer")
    @patch("app.tasks.process_outbox_task.get_sync_session")
    def test_no_brokers_available_reraises(self, mock_get_session, mock_kafka, mock_acquire, mock_release):
        session = mock_get_session.return_value
        outbox = _make_outbox()
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [outbox]
        session.get.return_value = outbox
        mock_acquire.return_value = MagicMock()
        mock_kafka.publish.side_effect = _FakeNoBrokersAvailable("no brokers")

        from app.tasks.process_outbox_task import process_outbox_task
        with pytest.raises(_FakeNoBrokersAvailable):
            process_outbox_task()

        session.rollback.assert_not_called()
        mock_release.assert_called_once()
        session.close.assert_called_once()

    @patch("app.tasks.process_outbox_task.release_lock")
    @patch("app.tasks.process_outbox_task.acquire_lock")
    @patch("app.tasks.process_outbox_task.kafka_producer")
    @patch("app.tasks.process_outbox_task.get_sync_session")
    def test_terminal_failure_with_manifest_id(self, mock_get_session, mock_kafka, mock_acquire, mock_release):
        session = mock_get_session.return_value
        outbox = _make_outbox(
            payload={
                "origin_service": "manifest_service",
                "manifest_id": "manifest:1",
                "video_id": "vid1",
            },
            num_of_retry=4,
        )
        manifest_task = _make_manifest_task(
            status=ManifestStatus.COMPLETED,
            published=True,
        )
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [outbox]
        mock_acquire.return_value = MagicMock()
        mock_kafka.publish.side_effect = Exception("kafka unavailable")

        def session_get_side_effect(model, pk):
            if model is Outbox:
                return outbox
            if model is ManifestTask:
                return manifest_task
            return None

        session.get.side_effect = session_get_side_effect

        from app.tasks.process_outbox_task import process_outbox_task
        result = process_outbox_task()

        assert result["processed"] == 0
        assert outbox.status == OutboxStatus.FAILED.value
        assert outbox.num_of_retry == 5
        assert outbox.retry_after is None
        assert manifest_task.status == ManifestStatus.FAILED.value
        assert manifest_task.published is False
        session.commit.assert_called()
        session.close.assert_called_once()

    @patch("app.tasks.process_outbox_task.release_lock")
    @patch("app.tasks.process_outbox_task.acquire_lock")
    @patch("app.tasks.process_outbox_task.kafka_producer")
    @patch("app.tasks.process_outbox_task.get_sync_session")
    def test_terminal_failure_without_manifest_id(self, mock_get_session, mock_kafka, mock_acquire, mock_release):
        session = mock_get_session.return_value
        outbox = _make_outbox(
            payload={"origin_service": "manifest_service", "video_id": "vid1"},
            num_of_retry=4,
        )
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [outbox]
        session.get.return_value = outbox
        mock_acquire.return_value = MagicMock()
        mock_kafka.publish.side_effect = Exception("kafka unavailable")

        from app.tasks.process_outbox_task import process_outbox_task
        result = process_outbox_task()

        assert result["processed"] == 0
        assert outbox.status == OutboxStatus.FAILED.value
        assert outbox.num_of_retry == 5
        assert outbox.retry_after is None
        session.commit.assert_called()
        session.close.assert_called_once()


# ---------------------------------------------------------------------------
# TestProcessFailedManifestTasks
# ---------------------------------------------------------------------------

class TestProcessFailedManifestTasks:

    @patch("app.tasks.process_failed_manifest_tasks.get_sync_session")
    def test_no_failed_tasks(self, mock_get_session):
        session = mock_get_session.return_value
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = []

        from app.tasks.process_failed_manifest_tasks import process_failed_manifest_tasks
        result = process_failed_manifest_tasks()

        assert result == {"processed": 0}
        session.close.assert_called_once()

    @patch("app.tasks.process_failed_manifest_tasks.cleanup_manifest")
    @patch("app.tasks.process_failed_manifest_tasks.release_lock")
    @patch("app.tasks.process_failed_manifest_tasks.acquire_lock")
    @patch("app.tasks.process_failed_manifest_tasks.get_sync_session")
    def test_successful_publish(self, mock_get_session, mock_acquire, mock_release, mock_cleanup):
        session = mock_get_session.return_value
        task = _make_manifest_task(status=ManifestStatus.FAILED, published=False)
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [task]
        session.get.return_value = task
        mock_acquire.return_value = MagicMock()

        from app.tasks.process_failed_manifest_tasks import process_failed_manifest_tasks
        result = process_failed_manifest_tasks()

        assert result["processed"] == 1
        mock_cleanup.assert_called_once_with("vid1")
        assert task.published is True
        session.add.assert_called_once()
        added = session.add.call_args[0][0]
        assert isinstance(added, Outbox)
        assert added.topic == "manifest.failed"
        assert added.payload["origin_service"] == "manifest_service"
        assert added.payload["manifest_id"] == task.id
        assert added.payload["video_id"] == "vid1"
        session.commit.assert_called()
        session.close.assert_called_once()
        assert mock_acquire.call_count == 2
        assert mock_release.call_count == 2

    @patch("app.tasks.process_failed_manifest_tasks.cleanup_manifest")
    @patch("app.tasks.process_failed_manifest_tasks.release_lock")
    @patch("app.tasks.process_failed_manifest_tasks.acquire_lock")
    @patch("app.tasks.process_failed_manifest_tasks.get_sync_session")
    def test_processing_lock_failed(self, mock_get_session, mock_acquire, mock_release, mock_cleanup):
        session = mock_get_session.return_value
        task = _make_manifest_task(status=ManifestStatus.FAILED, published=False)
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [task]
        mock_acquire.return_value = None

        from app.tasks.process_failed_manifest_tasks import process_failed_manifest_tasks
        result = process_failed_manifest_tasks()

        assert result == {"processed": 0}
        mock_cleanup.assert_not_called()
        mock_release.assert_not_called()
        session.add.assert_not_called()
        session.close.assert_called_once()

    @patch("app.tasks.process_failed_manifest_tasks.cleanup_manifest")
    @patch("app.tasks.process_failed_manifest_tasks.release_lock")
    @patch("app.tasks.process_failed_manifest_tasks.acquire_lock")
    @patch("app.tasks.process_failed_manifest_tasks.get_sync_session")
    def test_committing_lock_failed(self, mock_get_session, mock_acquire, mock_release, mock_cleanup):
        session = mock_get_session.return_value
        task = _make_manifest_task(status=ManifestStatus.FAILED, published=False)
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [task]

        processing_lock = MagicMock()
        mock_acquire.side_effect = [processing_lock, None]

        from app.tasks.process_failed_manifest_tasks import process_failed_manifest_tasks
        result = process_failed_manifest_tasks()

        assert result == {"processed": 0}
        mock_cleanup.assert_called_once_with("vid1")
        session.add.assert_not_called()
        mock_release.assert_called_once_with(processing_lock)
        session.close.assert_called_once()

    @patch("app.tasks.process_failed_manifest_tasks.cleanup_manifest")
    @patch("app.tasks.process_failed_manifest_tasks.release_lock")
    @patch("app.tasks.process_failed_manifest_tasks.acquire_lock")
    @patch("app.tasks.process_failed_manifest_tasks.get_sync_session")
    def test_error_swallowed_continues(self, mock_get_session, mock_acquire, mock_release, mock_cleanup):
        session = mock_get_session.return_value
        task = _make_manifest_task(status=ManifestStatus.FAILED, published=False)
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [task]
        mock_acquire.return_value = MagicMock()
        mock_cleanup.side_effect = Exception("cleanup failed")

        from app.tasks.process_failed_manifest_tasks import process_failed_manifest_tasks
        result = process_failed_manifest_tasks()

        assert result == {"processed": 0}
        session.rollback.assert_called()
        session.add.assert_not_called()
        session.close.assert_called_once()


# ---------------------------------------------------------------------------
# TestProcessCompletedManifestTask
# ---------------------------------------------------------------------------

class TestProcessCompletedManifestTask:

    @patch("app.tasks.process_completed_manifest_task.get_sync_session")
    def test_no_completed_tasks(self, mock_get_session):
        session = mock_get_session.return_value
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = []

        from app.tasks.process_completed_manifest_task import process_completed_manifest_task
        result = process_completed_manifest_task()

        assert result == {"processed": 0}
        session.close.assert_called_once()

    @patch("app.tasks.process_completed_manifest_task.cleanup_manifest")
    @patch("app.tasks.process_completed_manifest_task.release_lock")
    @patch("app.tasks.process_completed_manifest_task.acquire_lock")
    @patch("app.tasks.process_completed_manifest_task.get_sync_session")
    def test_successful_publish(self, mock_get_session, mock_acquire, mock_release, mock_cleanup):
        session = mock_get_session.return_value
        task = _make_manifest_task(
            status=ManifestStatus.COMPLETED,
            published=False,
            manifest_url="http://minio:9000/manifest/vid1/manifest_abc.mpd",
        )
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [task]
        session.get.return_value = task
        mock_acquire.return_value = MagicMock()

        from app.tasks.process_completed_manifest_task import process_completed_manifest_task
        result = process_completed_manifest_task()

        assert result["processed"] == 1
        mock_cleanup.assert_called_once_with("vid1")
        assert task.published is True
        session.add.assert_called_once()
        added = session.add.call_args[0][0]
        assert isinstance(added, Outbox)
        assert added.topic == "manifest.completed"
        assert added.payload["origin_service"] == "manifest_service"
        assert added.payload["video_id"] == "vid1"
        assert added.payload["manifest_id"] == task.id
        assert added.payload["manifest_url"] == "http://minio:9000/manifest/vid1/manifest_abc.mpd"
        session.commit.assert_called()
        session.close.assert_called_once()
        assert mock_acquire.call_count == 2
        assert mock_release.call_count == 2

    @patch("app.tasks.process_completed_manifest_task.cleanup_manifest")
    @patch("app.tasks.process_completed_manifest_task.release_lock")
    @patch("app.tasks.process_completed_manifest_task.acquire_lock")
    @patch("app.tasks.process_completed_manifest_task.get_sync_session")
    def test_processing_lock_failed(self, mock_get_session, mock_acquire, mock_release, mock_cleanup):
        session = mock_get_session.return_value
        task = _make_manifest_task(status=ManifestStatus.COMPLETED, published=False)
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [task]
        mock_acquire.return_value = None

        from app.tasks.process_completed_manifest_task import process_completed_manifest_task
        result = process_completed_manifest_task()

        assert result == {"processed": 0}
        mock_cleanup.assert_not_called()
        mock_release.assert_not_called()
        session.add.assert_not_called()
        session.close.assert_called_once()

    @patch("app.tasks.process_completed_manifest_task.cleanup_manifest")
    @patch("app.tasks.process_completed_manifest_task.release_lock")
    @patch("app.tasks.process_completed_manifest_task.acquire_lock")
    @patch("app.tasks.process_completed_manifest_task.get_sync_session")
    def test_committing_lock_failed(self, mock_get_session, mock_acquire, mock_release, mock_cleanup):
        session = mock_get_session.return_value
        task = _make_manifest_task(status=ManifestStatus.COMPLETED, published=False)
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [task]

        processing_lock = MagicMock()
        mock_acquire.side_effect = [processing_lock, None]

        from app.tasks.process_completed_manifest_task import process_completed_manifest_task
        result = process_completed_manifest_task()

        assert result == {"processed": 0}
        mock_cleanup.assert_called_once_with("vid1")
        session.add.assert_not_called()
        mock_release.assert_called_once_with(processing_lock)
        session.close.assert_called_once()

    @patch("app.tasks.process_completed_manifest_task.cleanup_manifest")
    @patch("app.tasks.process_completed_manifest_task.release_lock")
    @patch("app.tasks.process_completed_manifest_task.acquire_lock")
    @patch("app.tasks.process_completed_manifest_task.get_sync_session")
    def test_error_swallowed_continues(self, mock_get_session, mock_acquire, mock_release, mock_cleanup):
        session = mock_get_session.return_value
        task = _make_manifest_task(status=ManifestStatus.COMPLETED, published=False)
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [task]
        mock_acquire.return_value = MagicMock()
        mock_cleanup.side_effect = Exception("cleanup failed")

        from app.tasks.process_completed_manifest_task import process_completed_manifest_task
        result = process_completed_manifest_task()

        assert result == {"processed": 0}
        session.rollback.assert_called()
        session.add.assert_not_called()
        session.close.assert_called_once()
