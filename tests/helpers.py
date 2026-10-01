"""Builders for synthetic CMAF init-segment bytes used in tests."""


def make_video_init(profile: int = 0x64, compat: int = 0x00, level: int = 0x28) -> bytes:
    """Build a minimal init segment containing an avcC box.

    Args:
        profile: AVCProfileIndication byte (0x64 = High).
        compat: profile_compatibility byte.
        level: AVCLevelIndication byte (0x28 = 4.0).

    Returns:
        Raw init segment bytes with ftyp + avcC boxes.
    """
    avcc_payload = bytes([1, profile, compat, level, 0xFF, 0xE1, 0x00, 0x00])
    avcc_box = (8 + len(avcc_payload)).to_bytes(4, "big") + b"avcC" + avcc_payload
    ftyp_box = (16).to_bytes(4, "big") + b"ftyp" + b"isom" + (0).to_bytes(4, "big")
    return ftyp_box + avcc_box


def make_audio_init(audio_object_type: int = 2) -> bytes:
    """Build a minimal init segment containing an esds box with AAC config.

    Args:
        audio_object_type: MPEG-4 audio object type (2 = AAC-LC, 5 = HE-AAC).

    Returns:
        Raw init segment bytes with ftyp + esds boxes.
    """
    asc = bytes([(audio_object_type << 3) & 0xFF, 0x90])
    dsi = bytes([0x05, len(asc)]) + asc
    dec_config_body = bytes([0x40, 0x15]) + bytes(3) + bytes(4) + bytes(4) + dsi
    dec_config = bytes([0x04, len(dec_config_body)]) + dec_config_body
    es_body = (1).to_bytes(2, "big") + bytes([0x00]) + dec_config
    es_descriptor = bytes([0x03, len(es_body)]) + es_body
    # esds is a FullBox: version(1) + flags(3) precede the descriptors.
    esds_payload = bytes(4) + es_descriptor
    esds_box = (8 + len(esds_payload)).to_bytes(4, "big") + b"esds" + esds_payload
    ftyp_box = (16).to_bytes(4, "big") + b"ftyp" + b"isom" + (0).to_bytes(4, "big")
    return ftyp_box + esds_box
