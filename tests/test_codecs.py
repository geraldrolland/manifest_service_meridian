"""Tests for app.codecs (codec strings from init segments)."""

import pytest

from app.codecs import (
    CodecError,
    audio_codec_from_init,
    codec_from_init,
    video_codec_from_init,
)
from tests.helpers import make_audio_init, make_video_init


class TestVideoCodecFromInit:

    def test_high_profile_level_40(self):
        assert video_codec_from_init(make_video_init(0x64, 0x00, 0x28)) == "avc1.640028"

    def test_high_profile_level_31(self):
        assert video_codec_from_init(make_video_init(0x64, 0x00, 0x1F)) == "avc1.64001F"

    def test_main_profile_level_30(self):
        assert video_codec_from_init(make_video_init(0x4D, 0x40, 0x1E)) == "avc1.4D401E"

    def test_missing_avcc_raises(self):
        with pytest.raises(CodecError, match="avcC"):
            video_codec_from_init(b"\x00\x00\x00\x10ftypisom\x00\x00\x00\x00")

    def test_empty_input_raises(self):
        with pytest.raises(CodecError):
            video_codec_from_init(b"")


class TestAudioCodecFromInit:

    def test_aac_lc(self):
        assert audio_codec_from_init(make_audio_init(2)) == "mp4a.40.2"

    def test_he_aac(self):
        assert audio_codec_from_init(make_audio_init(5)) == "mp4a.40.5"

    def test_skips_fullbox_version_and_flags(self):
        # Regression: a real esds opens with version+flags; treating the
        # version byte as a descriptor tag raised "expected tag 0x03".
        init = make_audio_init()
        esds_idx = init.index(b"esds")
        assert init[esds_idx + 4:esds_idx + 8] == bytes(4)
        assert audio_codec_from_init(init) == "mp4a.40.2"

    def test_missing_esds_raises(self):
        with pytest.raises(CodecError, match="esds"):
            audio_codec_from_init(make_video_init())

    def test_empty_input_raises(self):
        with pytest.raises(CodecError):
            audio_codec_from_init(b"")


class TestCodecFromInit:

    def test_video_kind(self):
        assert codec_from_init(make_video_init(), kind="video") == "avc1.640028"

    def test_audio_kind(self):
        assert codec_from_init(make_audio_init(), kind="audio") == "mp4a.40.2"

    def test_unknown_kind_raises(self):
        with pytest.raises(ValueError, match="unknown init segment kind"):
            codec_from_init(make_video_init(), kind="subtitle")
