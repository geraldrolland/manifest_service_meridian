"""DASH manifest generation for the manifest service.

Builds a static MPEG-DASH (.mpd) manifest from the manifest_metadata
published by media_processing_service's process_completed_jobs, and
writes it to {output_dir}/{video_id}/manifest_{uuid8}.mpd.
"""

import os
import uuid
import xml.etree.ElementTree as ET


class GenerateManifest:
    """Generates a static DASH manifest for a processed video."""

    TIMESCALE = 90000
    START_NUMBER = 1
    SEGMENT_ALIGNMENT = "true"
    MIN_BUFFER_TIME = "PT2S"
    AUDIO_BANDWIDTH = 128000
    DASH_NS = "urn:mpeg:dash:schema:mpd:2011"

    def __init__(
        self,
        video_id: str,
        media_prefix: str,
        output_dir: str,
        segment_duration: int,
        renditions: dict[str, dict],
        segment_prefix: str,
        video_duration: float,
        framerate: float,
    ):
        """Args:
            video_id: Video identifier.
            media_prefix: Segment prefix from manifest_metadata,
                e.g. "vidsegments/{video_id}/".
            output_dir: Base output directory, e.g. "/tmp/manifest".
            segment_duration: Segment length in seconds.
            renditions: Mapping of rendition name -> {width, height, bitrate}.
            segment_prefix: Segment filename prefix, e.g. "seg_".
            video_duration: Total video duration in seconds.
            framerate: Frame rate in FPS (adds frameRate attribute).
        """
        self.video_id = video_id
        self.media_prefix = media_prefix
        self.output_dir = output_dir
        self.segment_duration = segment_duration
        self.renditions = renditions
        self.segment_prefix = segment_prefix
        self.video_duration = video_duration
        self.framerate = framerate

    def generate_manifest(self) -> str:
        """Generate a static DASH manifest and write it to the output dir.

        Returns:
            Path to the generated .mpd file:
            {output_dir}/{video_id}/manifest_{uuid8}.mpd
        """
        self.__validate()

        video_dir = os.path.join(self.output_dir, self.video_id)
        os.makedirs(video_dir, exist_ok=True)
        manifest_path = os.path.join(
            video_dir, f"manifest_{uuid.uuid4().hex[:8]}.mpd"
        )

        mpd = self.__build_mpd()
        ET.ElementTree(mpd).write(
            manifest_path, encoding="utf-8", xml_declaration=True
        )

        return manifest_path

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def __validate(self) -> None:
        if self.video_duration <= 0:
            raise ValueError("video_duration must be > 0")
        if self.segment_duration <= 0:
            raise ValueError("segment_duration must be > 0")
        if not self.renditions:
            raise ValueError("renditions must not be empty")

    @staticmethod
    def __parse_bandwidth(bitrate: str | int) -> int:
        """Convert a bitrate like "800k" to bits per second (800000)."""
        if isinstance(bitrate, int):
            return bitrate
        s = str(bitrate).strip().lower()
        if s.endswith("k"):
            return int(float(s[:-1]) * 1000)
        if s.endswith("m"):
            return int(float(s[:-1]) * 1_000_000)
        return int(float(s))

    @staticmethod
    def __iso_duration(seconds: float) -> str:
        """Convert seconds to an ISO 8601 duration, e.g. 62.4 -> PT62.4S."""
        return f"PT{seconds:g}S"

    def __segment_template(self) -> ET.Element:
        """Build a SegmentTemplate element with path-absolute media URLs.

        Leading '/' makes references resolve from the MinIO host root,
        so they stay correct regardless of the MPD's own bucket/key
        (stored at manifest/{video_id}/manifest.mpd).
        """
        prefix = self.media_prefix.rstrip("/")
        seg_duration_ts = self.segment_duration * self.TIMESCALE
        return ET.Element(
            "SegmentTemplate",
            {
                "timescale": str(self.TIMESCALE),
                "duration": str(seg_duration_ts),
                "startNumber": str(self.START_NUMBER),
                "initialization": f"/{prefix}/$RepresentationID$/init.mp4",
                "media": (
                    f"/{prefix}/$RepresentationID$/"
                    f"{self.segment_prefix}$Number$.m4s"
                ),
            },
        )

    def __build_mpd(self) -> ET.Element:
        """Construct the full MPD ElementTree."""
        ET.register_namespace("", self.DASH_NS)
        duration = self.__iso_duration(self.video_duration)

        mpd = ET.Element(
            f"{{{self.DASH_NS}}}MPD",
            {
                "profiles": "urn:mpeg:dash:profile:full:2011",
                "type": "static",
                "mediaPresentationDuration": duration,
                "minBufferTime": self.MIN_BUFFER_TIME,
            },
        )
        period = ET.SubElement(mpd, "Period", {"duration": duration})

        # --- Video AdaptationSet ---
        video_attrs = {
            "contentType": "video",
            "mimeType": "video/mp4",
            "segmentAlignment": self.SEGMENT_ALIGNMENT,
            "startWithSAP": "1",
            "frameRate": f"{self.framerate:g}",
        }
        video_set = ET.SubElement(period, "AdaptationSet", video_attrs)

        for name, props in self.renditions.items():
            representation = ET.SubElement(
                video_set,
                "Representation",
                {
                    "id": name,
                    "width": str(props["width"]),
                    "height": str(props["height"]),
                    "bandwidth": str(self.__parse_bandwidth(props["bitrate"])),
                },
            )
            representation.append(self.__segment_template())

        # --- Audio AdaptationSet (always included) ---
        audio_set = ET.SubElement(
            period,
            "AdaptationSet",
            {
                "contentType": "audio",
                "mimeType": "audio/mp4",
                "segmentAlignment": self.SEGMENT_ALIGNMENT,
                "startWithSAP": "1",
            },
        )
        audio_representation = ET.SubElement(
            audio_set,
            "Representation",
            {
                "id": "audio",
                "bandwidth": str(self.AUDIO_BANDWIDTH),
            },
        )
        audio_representation.append(self.__segment_template())

        return mpd
