"""Tests for app.utils module."""

import os
from unittest.mock import patch

from app.utils import build_object_url, resolve_object_key, cleanup_manifest


class TestBuildObjectUrl:
    """Tests for build_object_url."""

    @patch("app.utils.settings")
    def test_basic_url(self, mock_settings):
        mock_settings.minio_endpoint = "minio:9000"
        result = build_object_url("vid1/manifest_abc.mpd", "manifest")
        assert result == "http://minio:9000/manifest/vid1/manifest_abc.mpd"

    @patch("app.utils.settings")
    def test_custom_endpoint(self, mock_settings):
        mock_settings.minio_endpoint = "minio.local:9000"
        result = build_object_url("key.mpd", "bucket")
        assert result == "http://minio.local:9000/bucket/key.mpd"

    @patch("app.utils.settings")
    def test_empty_key(self, mock_settings):
        mock_settings.minio_endpoint = "minio:9000"
        result = build_object_url("", "bucket")
        assert result == "http://minio:9000/bucket/"


class TestResolveObjectKey:
    """Tests for resolve_object_key."""

    def test_standard_mpd_path(self):
        result = resolve_object_key(
            "/tmp/manifest/vid123/manifest_ab12cd34.mpd",
            "/tmp/manifest",
        )
        assert result == "vid123/manifest_ab12cd34.mpd"

    def test_windows_backslashes_m4s_uuid_stripped(self):
        result = resolve_object_key(
            "C:\\tmp\\manifest\\vid\\seg_a1b2c3d4.m4s",
            "/tmp/manifest",
        )
        assert result == "vid/seg.m4s"

    def test_prefix_reduced_to_last_segment(self):
        result = resolve_object_key(
            "/tmp/manifest/vid1/manifest_x.mpd",
            "/tmp/manifest",
        )
        assert result == "vid1/manifest_x.mpd"

    def test_prefix_not_found_fallback_three_parts(self):
        result = resolve_object_key("/data/foo/bar/x.mpd", "/nonexistent")
        assert result == "foo/bar/x.mpd"

    def test_prefix_not_found_fallback_basename(self):
        result = resolve_object_key("a/b.mpd", "/nonexistent")
        assert result == "b.mpd"

    def test_prefix_first_occurrence_when_appears_twice(self):
        result = resolve_object_key(
            "/tmp/manifest/a/manifest/x.mpd",
            "/tmp/manifest",
        )
        assert result == "a/manifest/x.mpd"

    def test_strips_uuid_from_m4s(self):
        result = resolve_object_key(
            "/tmp/manifest/vid1/seg_a1b2c3d4.m4s",
            "/tmp/manifest",
        )
        assert result == "vid1/seg.m4s"

    def test_no_strip_uppercase_hex(self):
        result = resolve_object_key(
            "/tmp/manifest/vid1/seg_ABCDEFGH.m4s",
            "/tmp/manifest",
        )
        assert result == "vid1/seg_ABCDEFGH.m4s"

    def test_no_strip_non_hex(self):
        result = resolve_object_key(
            "/tmp/manifest/vid1/seg_xyz1234.m4s",
            "/tmp/manifest",
        )
        assert result == "vid1/seg_xyz1234.m4s"

    def test_no_strip_nine_hex_chars(self):
        result = resolve_object_key(
            "/tmp/manifest/vid1/seg_a1b2c3d4e.m4s",
            "/tmp/manifest",
        )
        assert result == "vid1/seg_a1b2c3d4e.m4s"

    def test_no_strip_when_not_end_anchored(self):
        result = resolve_object_key(
            "/tmp/manifest/vid1/seg_a1b2c3d4.m4s.tmp",
            "/tmp/manifest",
        )
        assert result == "vid1/seg_a1b2c3d4.m4s.tmp"

    def test_no_strip_for_mpd_extension(self):
        result = resolve_object_key(
            "/tmp/manifest/vid1/seg_a1b2c3d4.mpd",
            "/tmp/manifest",
        )
        assert result == "vid1/seg_a1b2c3d4.mpd"


class TestCleanupManifest:
    """Tests for cleanup_manifest."""

    @patch("app.utils.shutil.rmtree")
    @patch("app.utils.os.path.isdir", return_value=True)
    def test_removes_existing_directory(self, mock_isdir, mock_rmtree):
        cleanup_manifest("vid1")
        expected = os.path.join("/tmp/manifest", "vid1")
        mock_isdir.assert_called_once_with(expected)
        mock_rmtree.assert_called_once_with(expected)

    @patch("app.utils.shutil.rmtree")
    @patch("app.utils.os.path.isdir", return_value=False)
    def test_missing_directory_is_noop(self, mock_isdir, mock_rmtree):
        cleanup_manifest("missing-video")
        expected = os.path.join("/tmp/manifest", "missing-video")
        mock_isdir.assert_called_once_with(expected)
        mock_rmtree.assert_not_called()
