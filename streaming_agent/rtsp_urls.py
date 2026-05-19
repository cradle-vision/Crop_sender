"""RTSP URL helpers for streaming (substream = lower bitrate than main)."""

from __future__ import annotations

import re
from urllib.parse import urlparse, urlunparse

_HIK_CHANNEL_MAIN = re.compile(r"(/Streaming/Channels/\d+)1\b", re.IGNORECASE)
_HIK_CHANNEL_ANY = re.compile(r"/Streaming/Channels/\d+", re.IGNORECASE)


def to_substream_rtsp_url(url: str) -> str:
    """
    Rewrite RTSP URL to camera substream when possible.

    Hikvision: .../Channels/101 -> .../Channels/102 (channel N main -> N sub)
    Dahua: subtype=0 -> subtype=1
    Bare rtsp://host:554/ -> .../Streaming/Channels/102
    """
    raw = (url or "").strip()
    if not raw.lower().startswith("rtsp"):
        return url

    low = raw.lower()

    # Hikvision explicit main/sub channel id (101/201/301 -> 102/202/302)
    if _HIK_CHANNEL_MAIN.search(raw):
        return _HIK_CHANNEL_MAIN.sub(r"\g<1>2", raw)

    if _HIK_CHANNEL_ANY.search(raw):
        # Already a Channels/NNx path that is not main (e.g. 102) — leave as-is
        return raw

    # Dahua
    if "cam/realmonitor" in low:
        if "subtype=0" in low:
            return re.sub(r"subtype=0", "subtype=1", raw, flags=re.IGNORECASE)
        if "subtype=" not in low:
            sep = "&" if "?" in raw else "?"
            return f"{raw}{sep}channel=1&subtype=1"

    # Generic bare path — default Hikvision substream
    parsed = urlparse(raw)
    path = (parsed.path or "/").rstrip("/") or "/"
    if path in ("/", ""):
        return urlunparse(parsed._replace(path="/Streaming/Channels/102"))

    return raw
