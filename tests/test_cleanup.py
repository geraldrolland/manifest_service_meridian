"""Tests for app.cleanup.cleanup_manifest and the MinIO delete helper."""

from unittest.mock import MagicMock, patch

import pytest


def _manifest(manifest_id="manifest:1", manifest_url=None):
    m = MagicMock()
    m.id = manifest_id
    m.manifest_url = manifest_url
    return m


class TestCleanupManifest:
    """cleanup_manifest(db, manifests): object delete + row delete + commit."""

    def test_deletes_object_and_rows_then_commits(self):
        from app.cleanup import cleanup_manifest

        manifests = [
            _manifest(
                "manifest:1",
                "http://minio:9000/manifest/vid1/manifest_a.mpd",
            ),
            _manifest(
                "manifest:2",
                "http://minio:9000/manifest/vid1/manifest_b.mpd",
            ),
        ]
        db = MagicMock()

        with patch("app.cleanup.delete_object") as mock_delete:
            cleanup_manifest(db, manifests)

        assert mock_delete.call_count == 2
        mock_delete.assert_any_call("vid1/manifest_a.mpd", "manifest")
        mock_delete.assert_any_call("vid1/manifest_b.mpd", "manifest")
        assert db.delete.call_count == 2
        db.delete.assert_any_call(manifests[0])
        db.delete.assert_any_call(manifests[1])
        db.commit.assert_called_once()

    def test_row_without_url_skips_object_delete(self):
        from app.cleanup import cleanup_manifest

        manifests = [_manifest("manifest:1", manifest_url=None)]
        db = MagicMock()

        with patch("app.cleanup.delete_object") as mock_delete:
            cleanup_manifest(db, manifests)

        mock_delete.assert_not_called()
        db.delete.assert_called_once_with(manifests[0])
        db.commit.assert_called_once()

    def test_unparseable_url_still_deletes_row(self):
        from app.cleanup import cleanup_manifest

        manifests = [_manifest("manifest:1", manifest_url="not-a-url")]
        db = MagicMock()

        with patch("app.cleanup.delete_object") as mock_delete:
            cleanup_manifest(db, manifests)

        mock_delete.assert_not_called()
        db.delete.assert_called_once()
        db.commit.assert_called_once()

    def test_empty_list_commits_without_side_effects(self):
        from app.cleanup import cleanup_manifest

        db = MagicMock()

        with patch("app.cleanup.delete_object") as mock_delete:
            cleanup_manifest(db, [])

        mock_delete.assert_not_called()
        db.delete.assert_not_called()
        db.commit.assert_called_once()

    def test_object_delete_failure_propagates_without_commit(self):
        from app.cleanup import cleanup_manifest

        manifests = [
            _manifest("manifest:1", "http://minio:9000/manifest/vid1/m.mpd")
        ]
        db = MagicMock()

        with patch(
            "app.cleanup.delete_object", side_effect=Exception("minio down")
        ):
            with pytest.raises(Exception, match="minio down"):
                cleanup_manifest(db, manifests)

        db.delete.assert_not_called()
        db.commit.assert_not_called()


class TestDeleteObject:
    """minio_client.delete_object: idempotent deletes, tolerant of absent keys."""

    def test_calls_remove_object(self):
        from app.minio_client import delete_object

        with patch("app.minio_client.client") as mock_client:
            delete_object("vid1/manifest_a.mpd", "manifest")

        mock_client.remove_object.assert_called_once_with(
            bucket_name="manifest", object_name="vid1/manifest_a.mpd"
        )

    def test_absent_object_is_tolerated(self):
        from app.minio_client import delete_object

        err = Exception("NoSuchKey")
        err.code = "NoSuchKey"
        with patch("app.minio_client.client") as mock_client:
            mock_client.remove_object.side_effect = err
            delete_object("vid1/gone.mpd", "manifest")  # must not raise

    def test_other_errors_propagate(self):
        from app.minio_client import delete_object

        err = Exception("AccessDenied")
        err.code = "AccessDenied"
        with patch("app.minio_client.client") as mock_client:
            mock_client.remove_object.side_effect = err
            with pytest.raises(Exception, match="AccessDenied"):
                delete_object("vid1/x.mpd", "manifest")
