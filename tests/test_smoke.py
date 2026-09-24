"""Smoke tests for the manifest service."""

import sys
from unittest.mock import MagicMock

sys.modules["asyncpg"] = MagicMock()
sys.modules["aiokafka"] = MagicMock()
sys.modules["redis"] = MagicMock()
sys.modules["minio"] = MagicMock()


class _FakeNoBrokersAvailable(Exception):
    pass


_kafka_mock = MagicMock()
_kafka_errors_mock = MagicMock()
_kafka_errors_mock.NoBrokersAvailable = _FakeNoBrokersAvailable
sys.modules["kafka"] = _kafka_mock
sys.modules["kafka.errors"] = _kafka_errors_mock

from app.main import app  # noqa: E402
from app.celery_app import celery_app  # noqa: E402


class TestFastAPIApp:
    def test_app_title(self):
        assert app.title == "Manifest Service"

    def test_app_version(self):
        assert app.version == "1.0.0"

    def test_health_router_included(self):
        paths = [r.path for r in app.routes]
        assert "/health" in paths

    def test_ready_router_included(self):
        paths = [r.path for r in app.routes]
        assert "/ready" in paths


class TestCeleryConfig:
    def test_celery_has_all_beat_schedules(self):
        schedules = celery_app.conf.beat_schedule
        expected = [
            "process-outbox-every-10-seconds",
            "process-manifest-every-15-seconds",
            "process-failed-manifest-every-15-seconds",
            "process-completed-manifest-every-15-seconds",
        ]
        for name in expected:
            assert name in schedules, f"Missing beat schedule: {name}"

    def test_outbox_schedule_interval(self):
        assert celery_app.conf.beat_schedule["process-outbox-every-10-seconds"]["schedule"] == 10.0

    def test_manifest_schedule_intervals(self):
        for name in (
            "process-manifest-every-15-seconds",
            "process-failed-manifest-every-15-seconds",
            "process-completed-manifest-every-15-seconds",
        ):
            assert celery_app.conf.beat_schedule[name]["schedule"] == 15.0

    def test_all_beats_routed_to_manifest_queue(self):
        for name, entry in celery_app.conf.beat_schedule.items():
            assert entry["options"]["queue"] == "manifest", f"{name} not on manifest queue"

    def test_task_routes(self):
        routes = celery_app.conf.task_routes
        for task_name in (
            "app.tasks.process_outbox_task",
            "app.tasks.process_manifest_task",
            "app.tasks.process_failed_manifest_tasks",
            "app.tasks.process_completed_manifest_task",
        ):
            assert routes[task_name]["queue"] == "manifest"

    def test_celery_prefetch_count(self):
        assert celery_app.conf.worker_prefetch_multiplier == 4

    def test_celery_acks_late(self):
        assert celery_app.conf.task_acks_late is True


class TestImports:
    def test_all_tasks_importable(self):
        from app.tasks.process_manifest_task import process_manifest_task
        from app.tasks.process_outbox_task import process_outbox_task
        from app.tasks.process_failed_manifest_tasks import process_failed_manifest_tasks
        from app.tasks.process_completed_manifest_task import process_completed_manifest_task

        assert callable(process_manifest_task)
        assert callable(process_outbox_task)
        assert callable(process_failed_manifest_tasks)
        assert callable(process_completed_manifest_task)

    def test_models_importable(self):
        from app.models.manifest_task import ManifestTask, ManifestStatus
        from app.models.outbox import Outbox, OutboxStatus

        assert ManifestTask is not None
        assert Outbox is not None
        assert ManifestStatus.PENDING.value == "PENDING"
        assert OutboxStatus.PENDING.value == "PENDING"

    def test_utils_importable(self):
        from app.utils import build_object_url, resolve_object_key, cleanup_manifest

        assert callable(build_object_url)
        assert callable(resolve_object_key)
        assert callable(cleanup_manifest)

    def test_generate_manifest_importable(self):
        from app.generating_manifest import GenerateManifest

        assert GenerateManifest is not None

    def test_consumer_importable(self):
        from app.consumer import consume_messages, start_consumer

        assert callable(consume_messages)
        assert callable(start_consumer)
