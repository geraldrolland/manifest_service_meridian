"""Tests for app.generating_manifest.GenerateManifest."""

import os
import re
import xml.etree.ElementTree as ET

import pytest

from app.generating_manifest import GenerateManifest


def _make_generator(tmp_path, **overrides):
    kwargs = {
        "video_id": "vid1",
        "media_prefix": "vidsegments/vid1/",
        "output_dir": str(tmp_path),
        "segment_duration": 4,
        "renditions": {
            "720p": {"width": 1280, "height": 720, "bitrate": "800k"},
            "1080p": {"width": 1920, "height": 1080, "bitrate": "1.5M"},
        },
        "segment_prefix": "seg_",
        "video_duration": 62.4,
        "framerate": 30,
    }
    kwargs.update(overrides)
    return GenerateManifest(**kwargs)


def _local(tag):
    return tag.split("}")[-1]


def _parse(path):
    return ET.parse(path).getroot()


class TestGenerateManifest:

    def test_returns_path_under_video_dir(self, tmp_path):
        gen = _make_generator(tmp_path)
        path = gen.generate_manifest()
        assert path.startswith(os.path.join(str(tmp_path), "vid1"))
        assert re.search(r"manifest_[0-9a-f]{8}\.mpd$", path)
        assert os.path.isfile(path)

    def test_writes_valid_mpd_root(self, tmp_path):
        gen = _make_generator(tmp_path)
        path = gen.generate_manifest()
        root = _parse(path)
        assert _local(root.tag) == "MPD"
        assert root.get("type") == "static"
        assert root.get("mediaPresentationDuration") == "PT62.4S"
        assert root.get("minBufferTime") == "PT2S"
        assert root.get("profiles") == "urn:mpeg:dash:profile:full:2011"

    def test_video_adaptation_set_has_frame_rate(self, tmp_path):
        gen = _make_generator(tmp_path, framerate=29.97)
        path = gen.generate_manifest()
        root = _parse(path)
        video_sets = [
            e for e in root.iter() if _local(e.tag) == "AdaptationSet" and e.get("contentType") == "video"
        ]
        assert len(video_sets) == 1
        assert video_sets[0].get("frameRate") == "29.97"
        assert video_sets[0].get("mimeType") == "video/mp4"

    def test_one_representation_per_rendition(self, tmp_path):
        gen = _make_generator(tmp_path)
        path = gen.generate_manifest()
        root = _parse(path)
        video_set = next(
            e for e in root.iter() if _local(e.tag) == "AdaptationSet" and e.get("contentType") == "video"
        )
        reps = [e for e in video_set if _local(e.tag) == "Representation"]
        assert len(reps) == 2
        ids = {r.get("id") for r in reps}
        assert ids == {"720p", "1080p"}
        by_id = {r.get("id"): r for r in reps}
        assert by_id["720p"].get("width") == "1280"
        assert by_id["720p"].get("height") == "720"
        assert by_id["720p"].get("bandwidth") == "800000"
        assert by_id["1080p"].get("bandwidth") == "1500000"

    def test_audio_adaptation_set_always_present(self, tmp_path):
        gen = _make_generator(tmp_path)
        path = gen.generate_manifest()
        root = _parse(path)
        audio_sets = [
            e for e in root.iter() if _local(e.tag) == "AdaptationSet" and e.get("contentType") == "audio"
        ]
        assert len(audio_sets) == 1
        audio_reps = [e for e in audio_sets[0] if _local(e.tag) == "Representation"]
        assert len(audio_reps) == 1
        assert audio_reps[0].get("id") == "audio"
        assert audio_reps[0].get("bandwidth") == "128000"

    def test_segment_template_values(self, tmp_path):
        gen = _make_generator(tmp_path, segment_duration=4, media_prefix="vidsegments/vid1/")
        path = gen.generate_manifest()
        root = _parse(path)
        templates = [e for e in root.iter() if _local(e.tag) == "SegmentTemplate"]
        assert len(templates) >= 3
        for t in templates:
            assert t.get("timescale") == "90000"
            assert t.get("duration") == str(4 * 90000)
            assert t.get("startNumber") == "1"
            assert t.get("initialization") == "/vidsegments/vid1/$RepresentationID$/init.mp4"
            assert t.get("media") == "/vidsegments/vid1/$RepresentationID$/seg_$Number$.m4s"

    def test_media_prefix_trailing_slash_stripped(self, tmp_path):
        gen = _make_generator(tmp_path, media_prefix="vidsegments/vid1/")
        path = gen.generate_manifest()
        root = _parse(path)
        t = next(e for e in root.iter() if _local(e.tag) == "SegmentTemplate")
        assert t.get("initialization").startswith("/vidsegments/vid1/")
        assert not t.get("initialization").startswith("//")

    def test_media_prefix_without_trailing_slash(self, tmp_path):
        gen = _make_generator(tmp_path, media_prefix="vidsegments/vid1")
        path = gen.generate_manifest()
        root = _parse(path)
        t = next(e for e in root.iter() if _local(e.tag) == "SegmentTemplate")
        assert t.get("initialization") == "/vidsegments/vid1/$RepresentationID$/init.mp4"

    def test_bandwidth_int_passthrough(self, tmp_path):
        gen = _make_generator(
            tmp_path,
            renditions={"720p": {"width": 1280, "height": 720, "bitrate": 800000}},
        )
        path = gen.generate_manifest()
        root = _parse(path)
        rep = next(e for e in root.iter() if _local(e.tag) == "Representation" and e.get("id") == "720p")
        assert rep.get("bandwidth") == "800000"

    def test_bandwidth_numeric_string(self, tmp_path):
        gen = _make_generator(
            tmp_path,
            renditions={"720p": {"width": 1280, "height": 720, "bitrate": "800000"}},
        )
        path = gen.generate_manifest()
        root = _parse(path)
        rep = next(e for e in root.iter() if _local(e.tag) == "Representation" and e.get("id") == "720p")
        assert rep.get("bandwidth") == "800000"

    def test_two_calls_different_filenames(self, tmp_path):
        gen = _make_generator(tmp_path)
        path1 = gen.generate_manifest()
        path2 = gen.generate_manifest()
        assert path1 != path2
        assert os.path.isfile(path1)
        assert os.path.isfile(path2)

    def test_invalid_video_duration_raises(self, tmp_path):
        gen = _make_generator(tmp_path, video_duration=0)
        with pytest.raises(ValueError, match="video_duration"):
            gen.generate_manifest()

    def test_invalid_segment_duration_raises(self, tmp_path):
        gen = _make_generator(tmp_path, segment_duration=0)
        with pytest.raises(ValueError, match="segment_duration"):
            gen.generate_manifest()

    def test_empty_renditions_raises(self, tmp_path):
        gen = _make_generator(tmp_path, renditions={})
        with pytest.raises(ValueError, match="renditions"):
            gen.generate_manifest()
