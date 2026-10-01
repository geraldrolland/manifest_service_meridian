"""Codec string extraction from CMAF init segments.

Parses codec identifiers out of the ``avcC`` (video) and ``esds`` (audio)
boxes of an init segment so the DASH manifest can carry the ``@codecs``
attribute. dash.js refuses representations without a codec string.

Example results: ``avc1.640028`` (H.264 High L4.0), ``mp4a.40.2`` (AAC-LC).
"""

import logging

logger = logging.getLogger(__name__)


class CodecError(RuntimeError):
    """Raised when a codec string cannot be derived from an init segment."""


_AUDIO_OBJECT_TYPES = {
    1: "mp4a.40.1",  # AAC Main
    2: "mp4a.40.2",  # AAC-LC
    5: "mp4a.40.5",  # HE-AAC
    23: "mp4a.40.23",  # AAC Scalable
    29: "mp4a.40.29",  # HE-AACv2
    42: "mp4a.40.42",  # xHE-AAC
}


def _find_box_payload(data: bytes, box_type: bytes, min_len: int = 4) -> bytes | None:
    """Return the payload of the first box whose type matches ``box_type``.

    Scans every occurrence so a stray byte pattern inside another payload
    is skipped unless its declared 4-byte size is plausible.
    """
    start = 0
    while True:
        idx = data.find(box_type, start)
        if idx < 0:
            return None
        if idx >= 4:
            size = int.from_bytes(data[idx - 4:idx], "big")
            payload_start = idx + 4
            payload_end = idx - 4 + size
            if (
                size >= 8
                and payload_end <= len(data)
                and payload_end - payload_start >= min_len
            ):
                return data[payload_start:payload_end]
        start = idx + 1


def _read_descriptor(buf: bytes, offset: int) -> tuple[int, bytes, int]:
    """Read one MPEG-4 descriptor (tag + variable-length size) at ``offset``."""
    if offset >= len(buf):
        raise CodecError("descriptor offset past end of buffer")
    tag = buf[offset]
    offset += 1
    length = 0
    for _ in range(4):
        if offset >= len(buf):
            raise CodecError("truncated descriptor length")
        byte = buf[offset]
        offset += 1
        length = (length << 7) | (byte & 0x7F)
        if not byte & 0x80:
            break
    else:
        raise CodecError("descriptor length field too long")
    end = offset + length
    if end > len(buf):
        raise CodecError("descriptor body extends past buffer")
    return tag, buf[offset:end], end


def video_codec_from_init(data: bytes) -> str:
    """Derive an RFC 6381 codec string from an H.264 ``avcC`` box.

    Args:
        data: Raw bytes of the init segment.

    Returns:
        Codec string, e.g. "avc1.640028".

    Raises:
        CodecError: If no usable avcC box is present.
    """
    payload = _find_box_payload(data, b"avcC", min_len=4)
    if payload is None:
        raise CodecError("avcC box not found in init segment")
    if payload[0] != 1:
        raise CodecError(f"unexpected avcC configurationVersion {payload[0]}")
    profile, compat, level = payload[1], payload[2], payload[3]
    codec = f"avc1.{profile:02X}{compat:02X}{level:02X}"
    logger.debug("video codec from init segment: %s", codec)
    return codec


def audio_codec_from_init(data: bytes) -> str:
    """Derive an RFC 6381 codec string from an ``esds`` (MPEG-4 audio) box.

    Only AAC (objectTypeIndication 0x40) is supported; the transcoder
    re-encodes audio with ``-c:a aac`` so that is the only expected case.

    Args:
        data: Raw bytes of the init segment.

    Returns:
        Codec string, e.g. "mp4a.40.2".

    Raises:
        CodecError: If no usable esds/AAC config is present.
    """
    esds = _find_box_payload(data, b"esds", min_len=8)
    if esds is None:
        raise CodecError("esds box not found in init segment")

    # esds is a FullBox: its payload opens with version(1) + flags(3) before
    # the first MPEG-4 descriptor. Skipping those keeps _read_descriptor from
    # mistaking the version byte for a descriptor tag.
    tag, es_body, _ = _read_descriptor(esds, 4)
    if tag != 0x03:
        raise CodecError(f"expected ES_Descriptor tag 0x03, got 0x{tag:02x}")
    if len(es_body) < 3:
        raise CodecError("ES_Descriptor too short")

    flags = es_body[2]
    offset = 3
    if flags & 0x80:  # StreamDependenceFlag -> ES_DependsOn (2 bytes)
        offset += 2
    if flags & 0x40:  # URL_Flag -> URLlength (1 byte) + URL
        if offset >= len(es_body):
            raise CodecError("truncated URL_Flag in ES_Descriptor")
        offset += 1 + es_body[offset]
    if flags & 0x20:  # OCRstreamFlag -> OCRES_ID (2 bytes)
        offset += 2

    tag, dec_body, _ = _read_descriptor(es_body, offset)
    if tag != 0x04:
        raise CodecError(f"expected DecoderConfigDescriptor tag 0x04, got 0x{tag:02x}")
    # objectTypeIndication(1) streamType(1) bufferSizeDB(3) maxBitrate(4) avgBitrate(4)
    if len(dec_body) <= 13:
        raise CodecError("DecoderConfigDescriptor too short")
    object_type_indication = dec_body[0]
    if object_type_indication != 0x40:
        raise CodecError(
            f"unsupported objectTypeIndication 0x{object_type_indication:02x} "
            "(only MPEG-4 audio/AAC is supported)"
        )

    tag, asc, _ = _read_descriptor(dec_body, 13)
    if tag != 0x05 or not asc:
        raise CodecError("DecoderSpecificInfo missing from DecoderConfigDescriptor")

    audio_object_type = asc[0] >> 3
    if audio_object_type == 31 and len(asc) >= 2:
        audio_object_type = ((asc[0] & 0x07) << 3) | (asc[1] >> 5)
    codec = _AUDIO_OBJECT_TYPES.get(audio_object_type, f"mp4a.40.{audio_object_type}")
    logger.debug("audio codec from init segment: %s", codec)
    return codec


def codec_from_init(data: bytes, kind: str = "video") -> str:
    """Derive a codec string from an init segment.

    Args:
        data: Raw bytes of the init segment.
        kind: "video" (avcC) or "audio" (esds).

    Returns:
        Codec string such as "avc1.640028" or "mp4a.40.2".

    Raises:
        ValueError: If ``kind`` is not "video" or "audio".
        CodecError: If the codec string cannot be derived.
    """
    if kind == "video":
        return video_codec_from_init(data)
    if kind == "audio":
        return audio_codec_from_init(data)
    raise ValueError(f"unknown init segment kind: {kind!r}")
