"""Load and validate agent configuration (YAML/JSON). Env overrides for secrets."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


def _env(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return v.strip() if v else default


def _env_int(key: str, default: int) -> int:
    try:
        v = os.getenv(key)
        return int(v) if v else default
    except (TypeError, ValueError):
        return default


def _env_float(key: str, default: float) -> float:
    try:
        v = os.getenv(key)
        return float(v) if v else default
    except (TypeError, ValueError):
        return default


def _env_bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


@dataclass
class MediaMTXConfig:
    api_url: str = "http://127.0.0.1:9997"
    """Public base for WHEP/WebRTC URLs shown to clients (no trailing slash)."""
    public_webrtc_base: str = "http://127.0.0.1:8889"
    rtsp_transport: str = "tcp"


@dataclass
class CameraEntry:
    id: str
    rtsp_url: str


@dataclass
class UpstreamConfig:
    """
    Центральный ingest (RTMP): агент открывает исходящее соединение — NAT/static IP на объекте не нужен.
    Плейсхолдеры в rtmp_url_template: {stream_key}, {agent_id}, {camera_id}
    """

    rtmp_url_template: str = ""
    """Шаблон URL для фронта (HLS и т.д.), тот же stream_key: https://cdn/w/{stream_key}.m3u8"""
    playback_url_template: str = ""
    ffmpeg_path: str = "ffmpeg"
    ffmpeg_extra_args: list[str] = field(default_factory=list)
    # True: libx264 вместо copy — новые монотонные PTS/DTS (камеры с «ломаными» таймстампами / RTP cseq).
    transcode: bool = False


@dataclass
class AgentConfig:
    agent_id: str = "edge-1"
    backend_url: str = "ws://127.0.0.1:8000/ws/agent"
    auth_token: str = ""
    heartbeat_interval_sec: float = 15.0
    reconnect_backoff_initial_sec: float = 2.0
    reconnect_backoff_max_sec: float = 60.0
    idle_grace_sec: float = 10.0
    # viewer_idle_stop: False = гасить только по stop_stream; True = ещё auto-stop при нуле зрителей (viewer_*)
    viewer_idle_stop: bool = True
    # delivery: local_webrtc | upstream_rtmp | both — см. README
    delivery: str = "local_webrtc"
    mediamtx: MediaMTXConfig = field(default_factory=MediaMTXConfig)
    upstream: UpstreamConfig = field(default_factory=UpstreamConfig)
    cameras: list[CameraEntry] = field(default_factory=list)

    def camera_rtsp_map(self) -> dict[str, str]:
        return {c.id: c.rtsp_url for c in self.cameras}


def _parse_camera(obj: dict[str, Any]) -> CameraEntry | None:
    cid = obj.get("id") or obj.get("camera_id")
    if not cid:
        return None
    cid = str(cid).strip()
    url = obj.get("rtsp_url") or obj.get("source") or obj.get("url")
    if not url or not str(url).strip():
        return None
    return CameraEntry(id=cid, rtsp_url=str(url).strip())


def _build_rtsp_url_from_camera_yaml(cam: dict[str, Any]) -> str:
    """
    Best-effort RTSP URL builder for sender's `config/cameras.yaml`.
    If `source` is already present and looks like a URL, it is used as-is.
    """
    src = cam.get("source")
    if isinstance(src, str) and "://" in src and src.strip():
        return src.strip()

    # Sender-crop fields:
    ip = cam.get("ip_address") or cam.get("device_ip") or ""
    username = cam.get("username") or cam.get("login") or ""
    password = cam.get("password") or ""
    port = cam.get("port") or 554

    rtsp_path = cam.get("rtsp_path") or "/"
    if rtsp_path is None:
        rtsp_path = "/"
    rtsp_path = str(rtsp_path).strip()
    if not rtsp_path.startswith("/"):
        rtsp_path = "/" + rtsp_path

    if not ip:
        return ""

    if username and password:
        return f"rtsp://{username}:{password}@{ip}:{port}{rtsp_path}"
    return f"rtsp://{ip}:{port}{rtsp_path}"


def _load_cameras_from_sender_cameras_yaml(path: str | Path) -> list[CameraEntry]:
    p = Path(path)
    if not p.is_file():
        # Support relative config paths from project root.
        root = Path(__file__).resolve().parent.parent
        alt = root / p
        if alt.is_file():
            p = alt
        else:
            raise FileNotFoundError(f"sender cameras.yaml not found: {path}")

    with open(p, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    cams = data.get("cameras") or []
    if not isinstance(cams, list):
        return []

    out: list[CameraEntry] = []
    for item in cams:
        if not isinstance(item, dict):
            continue
        enabled = item.get("enabled", True)
        if enabled is False:
            continue

        cid = item.get("camera_id") or item.get("id")
        if cid is None:
            continue
        cid_str = str(cid).strip()
        if not cid_str:
            continue

        rtsp_url = _build_rtsp_url_from_camera_yaml(item)
        if not rtsp_url:
            continue

        out.append(CameraEntry(id=cid_str, rtsp_url=rtsp_url))
    return out


def load_config(path: str | Path | None = None) -> AgentConfig:
    """
    Load YAML or JSON. Path: STREAMING_AGENT_CONFIG or arg or default
    `config/streaming-agent.yaml`.
    """
    if path is None:
        path = _env("STREAMING_AGENT_CONFIG", "config/streaming-agent.yaml")
    path = Path(path)
    if not path.is_file():
        root = Path(__file__).resolve().parent.parent
        alt = root / path
        if alt.is_file():
            path = alt
        else:
            ex = root / "streaming_agent" / "streaming-agent.yaml.example"
            if ex.is_file():
                path = ex
            else:
                raise FileNotFoundError(f"Streaming agent config not found: {path}")

    with open(path, encoding="utf-8") as f:
        raw = f.read()
    if path.suffix.lower() in (".json",):
        data = json.loads(raw)
    else:
        data = yaml.safe_load(raw) or {}

    if not isinstance(data, dict):
        raise ValueError("Config root must be an object")

    med = data.get("mediamtx") or {}
    if not isinstance(med, dict):
        med = {}

    up = data.get("upstream") or {}
    if not isinstance(up, dict):
        up = {}

    delivery = str(
        data.get("delivery")
        or _env("STREAMING_DELIVERY")
        or "local_webrtc"
    ).strip().lower()
    if delivery not in ("local_webrtc", "upstream_rtmp", "both"):
        delivery = "local_webrtc"

    cameras: list[CameraEntry] = []
    raw_cams = data.get("cameras") or []
    for item in raw_cams:
        if isinstance(item, dict):
            c = _parse_camera(item)
            if c:
                cameras.append(c)

    # If cameras[] missing/empty in streaming-agent config, reuse sender's config/cameras.yaml.
    if not cameras:
        sender_cameras_path = _env("CAMERAS_CONFIG_PATH", "config/cameras.yaml")
        cameras = _load_cameras_from_sender_cameras_yaml(sender_cameras_path)

    cfg = AgentConfig(
        agent_id=str(data.get("agent_id") or _env("STREAMING_AGENT_ID") or "edge-1"),
        backend_url=str(
            data.get("backend_url")
            or _env("STREAMING_BACKEND_URL")
            or "ws://127.0.0.1:8000/ws/agent"
        ),
        auth_token=str(data.get("auth_token") or _env("STREAMING_AGENT_TOKEN") or ""),
        heartbeat_interval_sec=_env_float(
            "STREAMING_HEARTBEAT_INTERVAL_SEC",
            float(data.get("heartbeat_interval_sec", 15.0)),
        ),
        reconnect_backoff_initial_sec=float(
            data.get("reconnect_backoff_initial_sec", 2.0)
        ),
        reconnect_backoff_max_sec=float(data.get("reconnect_backoff_max_sec", 60.0)),
        idle_grace_sec=_env_float(
            "STREAMING_IDLE_GRACE_SEC", float(data.get("idle_grace_sec", 10.0))
        ),
        viewer_idle_stop=bool(data.get("viewer_idle_stop", True)),
        delivery=delivery,
        mediamtx=MediaMTXConfig(
            api_url=str(
                med.get("api_url")
                or _env("MEDIAMTX_API_URL")
                or "http://127.0.0.1:9997"
            ).rstrip("/"),
            public_webrtc_base=str(
                med.get("public_webrtc_base")
                or _env("MEDIAMTX_PUBLIC_WEBRTC_BASE")
                or "http://127.0.0.1:8889"
            ).rstrip("/"),
            rtsp_transport=str(med.get("rtsp_transport") or "tcp"),
        ),
        upstream=UpstreamConfig(
            rtmp_url_template=str(
                up.get("rtmp_url_template")
                or _env("STREAMING_UPSTREAM_RTMP_URL_TEMPLATE")
                or ""
            ),
            playback_url_template=str(
                up.get("playback_url_template")
                or _env("STREAMING_UPSTREAM_PLAYBACK_URL_TEMPLATE")
                or ""
            ),
            ffmpeg_path=str(up.get("ffmpeg_path") or _env("STREAMING_FFMPEG_PATH") or "ffmpeg"),
            ffmpeg_extra_args=list(up.get("ffmpeg_extra_args") or []),
            transcode=_env_bool(
                "STREAMING_UPSTREAM_TRANSCODE",
                bool(up.get("transcode", False)),
            ),
        ),
        cameras=cameras,
    )

    # Env overrides for agent_id / backend if still default
    if _env("STREAMING_AGENT_ID"):
        cfg.agent_id = _env("STREAMING_AGENT_ID")
    if _env("STREAMING_BACKEND_URL"):
        cfg.backend_url = _env("STREAMING_BACKEND_URL")
    if _env("STREAMING_AGENT_TOKEN"):
        cfg.auth_token = _env("STREAMING_AGENT_TOKEN")
    if os.getenv("STREAMING_VIEWER_IDLE_STOP") is not None:
        cfg.viewer_idle_stop = _env_bool("STREAMING_VIEWER_IDLE_STOP", True)
    if _env("STREAMING_DELIVERY"):
        d = _env("STREAMING_DELIVERY").strip().lower()
        if d in ("local_webrtc", "upstream_rtmp", "both"):
            cfg.delivery = d

    return cfg


def path_name_for_camera(camera_id: str) -> str:
    """Sanitize camera id for MediaMTX path segment."""
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in camera_id.strip())
    return safe or "cam"
