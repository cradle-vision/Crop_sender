"""RTSP URL helpers for streaming (substream = lower bitrate than main)."""

from __future__ import annotations

import re
from urllib.parse import urlparse, urlunparse

_HIK_CHANNEL_MAIN = re.compile(r"(/Streaming/Channels/\d+)1\b", re.IGNORECASE)
_HIK_CHANNEL_ANY = re.compile(r"/Streaming/Channels/\d+", re.IGNORECASE)
# Cam-Webs / OEM: /1/h264major (main) -> /1/h264minor (sub). Names say h264 but major is often HEVC.
_CAM_WEBS_MAJOR = re.compile(r"/(\d+)/(h264|h265)major\b", re.IGNORECASE)
_CAM_WEBS_ANY = re.compile(r"/(\d+)/(h264|h265)(major|minor)\b", re.IGNORECASE)


def _cam_webs_minor_path(path: str) -> str | None:
    m = _CAM_WEBS_MAJOR.search(path)
    if not m:
        return None
    channel, codec = m.group(1), m.group(2).lower()
    return re.sub(
        _CAM_WEBS_MAJOR,
        f"/{channel}/{codec}minor",
        path,
        count=1,
    )


def to_substream_rtsp_url(url: str, *, vendor: str = "") -> str:
    """
    Rewrite RTSP URL to camera substream when possible.

    Hikvision: .../Channels/101 -> .../Channels/102 (channel N main -> N sub)
    Dahua: subtype=0 -> subtype=1
    Cam-Webs: /1/h264major -> /1/h264minor (or bare URL + vendor cam_webs)
    """
    raw = (url or "").strip()
    if not raw.lower().startswith("rtsp"):
        return url

    low = raw.lower()
    vend = (vendor or "").strip().lower()

    # Cam-Webs explicit major path or vendor hint on bare URL.
    parsed = urlparse(raw)
    path = parsed.path or "/"
    minor_path = _cam_webs_minor_path(path)
    if minor_path:
        return urlunparse(parsed._replace(path=minor_path))

    if vend in ("cam_webs", "cam-webs", "camwebs"):
        p = path.rstrip("/") or "/"
        if p in ("/", ""):
            return urlunparse(parsed._replace(path="/1/h264minor"))

    # Hikvision explicit main/sub channel id (101/201/301 -> 102/202/302)
    if _HIK_CHANNEL_MAIN.search(raw):
        return _HIK_CHANNEL_MAIN.sub(r"\g<1>2", raw)

    if _HIK_CHANNEL_ANY.search(raw):
        return raw

    # Dahua
    if "cam/realmonitor" in low:
        if "subtype=0" in low:
            return re.sub(r"subtype=0", "subtype=1", raw, flags=re.IGNORECASE)
        if "subtype=" not in low:
            sep = "&" if "?" in raw else "?"
            return f"{raw}{sep}channel=1&subtype=1"

    # Bare path: Hikvision default unless Cam-Webs vendor set above.
    if (parsed.path or "/").rstrip("/") in ("", "/"):
        return urlunparse(parsed._replace(path="/Streaming/Channels/102"))

    return raw


def is_cam_webs_style_main(url: str, vendor: str = "") -> bool:
    """Cam-Webs main paths — bare URL only when vendor says so (Hikvision also uses /)."""
    vend = (vendor or "").strip().lower()
    if vend in ("cam_webs", "cam-webs", "camwebs"):
        return True
    raw = (url or "").strip()
    if not raw.lower().startswith("rtsp"):
        return False
    path = (urlparse(raw).path or "/").rstrip("/")
    if path in ("", "/"):
        return False
    return bool(_CAM_WEBS_ANY.search(path) and "major" in path.lower())


def cam_webs_concurrent_stream_url(url: str) -> str:
    """
    Second RTSP session for live view while sender-crop holds main (/).
    Cam-Webs exposes a Hikvision-style alias at /Streaming/Channels/102.
    """
    raw = (url or "").strip()
    if not raw.lower().startswith("rtsp"):
        return url
    parsed = urlparse(raw)
    return urlunparse(parsed._replace(path="/Streaming/Channels/102"))


def cam_webs_major_url(url: str) -> str:
    """Bare Cam-Webs RTSP root → stable main path (/1/h264major)."""
    raw = (url or "").strip()
    if not raw.lower().startswith("rtsp"):
        return url
    parsed = urlparse(raw)
    path = (parsed.path or "/").rstrip("/")
    if path in ("", "/"):
        return urlunparse(parsed._replace(path="/1/h264major"))
    if _CAM_WEBS_ANY.search(path) and "major" in path.lower():
        return raw
    return raw


def is_likely_substream_url(url: str) -> bool:
    low = (url or "").lower()
    if "/streaming/channels/" in low and re.search(r"/\d+2\b", low):
        return True
    if _CAM_WEBS_ANY.search(low) and "minor" in low:
        return True
    if "subtype=1" in low:
        return True
    return False
