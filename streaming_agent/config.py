"""Load and validate agent configuration (YAML/JSON). Env overrides for secrets."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from streaming_agent.rtsp_urls import (
    cam_webs_major_url,
    is_cam_webs_style_main,
    is_likely_substream_url,
    to_substream_rtsp_url,
)


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
    # Local RTSP relay: camera → MediaMTX → FFmpeg (stable; FFmpeg restarts skip camera).
    rtsp_relay_enabled: bool = True
    rtsp_listen_base: str = "rtsp://127.0.0.1:8554"


@dataclass
class CameraEntry:
    id: str
    rtsp_url: str
    # Original main/capture URL before substream rewrite (for fallback when sub is disabled).
    main_rtsp_url: str = ""
    # Optional per-camera live-view URL (overrides auto substream detection).
    streaming_rtsp_url: str = ""
    # hikvision | cam_webs — guides substream path when source is a bare RTSP URL.
    rtsp_vendor: str = ""
    # Non-empty = downscale during live transcode (main-stream fallback for Cam-Webs etc.).
    streaming_scale_filter: str = ""
    # Resolved transport for camera pull (relay): tcp | udp | auto → tcp/udp at probe time.
    streaming_rtsp_transport: str = ""


# Lightweight PTS fix only — no resize (native camera resolution, any H.264/HEVC/MJPEG).
_DEFAULT_PTS_FILTER = "setpts=PTS-STARTPTS"
# When no real substream: read main (often HEVC 1080p) and downscale while transcoding.
# fps=3 BEFORE scale: drop 30fps HEVC frames early (huge CPU save on Cam-Webs main).
_STREAMING_FALLBACK_SCALE = (
    "setpts=PTS-STARTPTS,fps=3,"
    "scale=640:360:force_original_aspect_ratio=decrease:flags=fast_bilinear,"
    "pad=ceil(iw/2)*2:ceil(ih/2)*2,format=yuv420p,setsar=1"
)


@dataclass
class TranscodeConfig:
    """Light libx264 live push: ultrafast, native resolution, works with H.264/HEVC inputs."""

    preset: str = "ultrafast"
    tune: str = "zerolatency"
    profile: str = "baseline"
    pix_fmt: str = "yuv420p"
    # Substream live view @ ~3 fps — realistic for store uplink (adjust per site).
    maxrate: str = "2M"
    bufsize: str = "4M"
    gop: int = 30
    keyint_min: int = 30
    bf: int = 0
    threads: int = 0  # 0 = ffmpeg auto
    fps: int = 3
    crf: int = 28
    level: str = "4.1"
    # Empty = no scaling; only setpts for timestamp stability.
    scale_filter: str = ""
    x264_params: str = "scenecut=0:repeat-headers=1:aud=1"
    max_muxing_queue_size: int = 2048


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
    # libx264 re-encode — stable PTS/DTS for RTMP ingest (default on).
    transcode: bool = True
    transcode_opts: TranscodeConfig = field(default_factory=TranscodeConfig)


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
    # Upstream FFmpeg auto-restart (edge store: retry until camera/RTMP recovers).
    restart_base_delay_sec: float = 6.0
    restart_max_delay_sec: float = 90.0
    rtmp_republish_cooldown_sec: float = 15.0
    # delivery: local_webrtc | upstream_rtmp | both — см. README
    delivery: str = "local_webrtc"
    mediamtx: MediaMTXConfig = field(default_factory=MediaMTXConfig)
    upstream: UpstreamConfig = field(default_factory=UpstreamConfig)
    cameras: list[CameraEntry] = field(default_factory=list)
    # Live view uses camera substream (Hikvision /102, Dahua subtype=1) — lower CPU/bandwidth than main.
    use_substream: bool = True

    def camera_rtsp_map(self) -> dict[str, str]:
        return {c.id: c.rtsp_url for c in self.cameras}

    def camera_scale_map(self) -> dict[str, str]:
        return {
            c.id: c.streaming_scale_filter
            for c in self.cameras
            if c.streaming_scale_filter.strip()
        }

    def camera_transport_map(self) -> dict[str, str]:
        return {
            c.id: c.streaming_rtsp_transport
            for c in self.cameras
            if c.streaming_rtsp_transport.strip() in ("tcp", "udp")
        }

    def camera_main_rtsp_map(self) -> dict[str, str]:
        return {
            c.id: (c.main_rtsp_url or c.rtsp_url).strip()
            for c in self.cameras
            if (c.main_rtsp_url or c.rtsp_url).strip()
        }


def _parse_transcode_opts(obj: dict[str, Any] | None) -> TranscodeConfig:
    o = obj if isinstance(obj, dict) else {}
    gop = _env_int("STREAMING_TRANSCODE_GOP", int(o.get("gop") or 30))
    scale_raw = o.get("scale_filter")
    if scale_raw is None:
        scale_raw = _env("STREAMING_TRANSCODE_SCALE_FILTER")
    scale_filter = str(scale_raw) if scale_raw is not None else ""
    return TranscodeConfig(
        preset=str(o.get("preset") or _env("STREAMING_TRANSCODE_PRESET") or "ultrafast"),
        tune=str(o.get("tune") or _env("STREAMING_TRANSCODE_TUNE") or "zerolatency"),
        profile=str(o.get("profile") or _env("STREAMING_TRANSCODE_PROFILE") or "baseline"),
        pix_fmt=str(o.get("pix_fmt") or "yuv420p"),
        maxrate=str(o.get("maxrate") or _env("STREAMING_TRANSCODE_MAXRATE") or "2M"),
        bufsize=str(o.get("bufsize") or _env("STREAMING_TRANSCODE_BUFSIZE") or "4M"),
        gop=gop,
        keyint_min=int(o.get("keyint_min") or o.get("gop") or gop),
        bf=int(o.get("bf") or 0),
        threads=_env_int("STREAMING_TRANSCODE_THREADS", int(o.get("threads") or 0)),
        fps=_env_int("STREAMING_TRANSCODE_FPS", int(o.get("fps") or 3)),
        crf=_env_int("STREAMING_TRANSCODE_CRF", int(o.get("crf") or 28)),
        level=str(o.get("level") or _env("STREAMING_TRANSCODE_LEVEL") or "4.1"),
        scale_filter=scale_filter,
        x264_params=str(
            o.get("x264_params")
            or _env("STREAMING_TRANSCODE_X264_PARAMS")
            or "scenecut=0:repeat-headers=1:aud=1"
        ),
        max_muxing_queue_size=_env_int(
            "STREAMING_TRANSCODE_MUX_QUEUE",
            int(o.get("max_muxing_queue_size") or 2048),
        ),
    )


def _probe_rtsp_video(url: str, ffmpeg_path: str = "ffmpeg") -> tuple[int, int, str]:
    """Return (width, height, codec_name) for first video stream; 0,0,'' on failure."""
    ffprobe = shutil.which("ffprobe") or "ffprobe"
    cmd = [
        ffprobe,
        "-v",
        "error",
        "-rtsp_transport",
        "tcp",
        "-timeout",
        "5000000",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=codec_name,width,height",
        "-of",
        "csv=p=0:s=,",
        url,
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=12, check=False)
        line = (r.stdout or "").strip().splitlines()[0] if r.stdout else ""
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 3:
            return 0, 0, ""
        codec, w, h = parts[0], int(parts[1] or 0), int(parts[2] or 0)
        return w, h, codec
    except Exception:
        return 0, 0, ""


def _substream_is_usable(url: str, ffmpeg_path: str) -> tuple[bool, int, int, str]:
    w, h, codec = _probe_rtsp_video(url, ffmpeg_path)
    return w > 0 and h > 0, w, h, codec


def _resolve_rtsp_transport(
    *,
    explicit: str,
    main: str,
    vendor: str,
    scale: str,
    stream_url: str,
) -> str:
    """Per-camera RTSP transport for relay pull (auto: UDP for Cam-Webs/downscale, else TCP)."""
    t = (explicit or "").strip().lower()
    if t in ("tcp", "udp"):
        return t
    vend = (vendor or "").strip().lower()
    if vend in ("hikvision", "hik", "dahua"):
        return "tcp"
    if scale.strip() or is_cam_webs_style_main(main, vendor):
        return "udp"
    if is_likely_substream_url(stream_url):
        return "tcp"
    return "tcp"


def _resolve_streaming_rtsp_url(
    c: CameraEntry,
    *,
    use_substream: bool,
    ffmpeg_path: str,
) -> CameraEntry:
    main = (c.main_rtsp_url or c.rtsp_url).strip()
    vendor = (c.rtsp_vendor or "").strip()
    scale = (c.streaming_scale_filter or "").strip()

    if c.streaming_rtsp_url.strip():
        stream = c.streaming_rtsp_url.strip()
        print(
            f"[Streaming Config] camera {c.id}: explicit streaming URL {_redact_rtsp(stream)}"
        )
    elif use_substream:
        stream = to_substream_rtsp_url(main, vendor=vendor)
        if stream != main:
            print(
                f"[Streaming Config] camera {c.id}: substream URL {_redact_rtsp(stream)}"
            )
    else:
        stream = main

    if use_substream and stream != main:
        ok, w, h, codec = _substream_is_usable(stream, ffmpeg_path)
        if not ok:
            cam_webs = to_substream_rtsp_url(main, vendor="cam_webs")
            if cam_webs != stream:
                ok2, w2, h2, codec2 = _substream_is_usable(cam_webs, ffmpeg_path)
                if ok2:
                    print(
                        f"[Streaming Config] camera {c.id}: Cam-Webs substream "
                        f"{_redact_rtsp(cam_webs)} ({codec2} {w2}x{h2})"
                    )
                    stream = cam_webs
                else:
                    print(
                        f"[Streaming Config] camera {c.id}: substream unavailable "
                        f"({codec or 'no signal'} {w}x{h}; Cam-Webs {codec2 or 'n/a'} "
                        f"{w2}x{h2}); main + downscale 640x360 {_redact_rtsp(main)}"
                    )
                    stream = cam_webs_major_url(main)
                    vendor = vendor or "cam_webs"
                    scale = _STREAMING_FALLBACK_SCALE
            else:
                print(
                    f"[Streaming Config] camera {c.id}: substream unavailable "
                    f"({codec or 'no signal'} {w}x{h}); main + downscale 640x360 "
                    f"{_redact_rtsp(main)}"
                )
                stream = cam_webs_major_url(main)
                if stream != main:
                    vendor = vendor or "cam_webs"
                scale = _STREAMING_FALLBACK_SCALE
        elif w >= 1280 and h >= 720:
            # Full HD on /102 alias — Cam-Webs bare main + sender on / (not Hik substream).
            from urllib.parse import urlparse as _urlparse

            main_path = (_urlparse(main).path or "/").rstrip("/")
            bare_main = main_path in ("", "/")
            cam_webs = to_substream_rtsp_url(main, vendor="cam_webs")
            cam_webs_concurrent = bare_main and "/streaming/channels/102" in stream.lower()
            if is_cam_webs_style_main(main, vendor) or cam_webs_concurrent:
                scale = scale or _STREAMING_FALLBACK_SCALE
                vendor = vendor or "cam_webs"
                print(
                    f"[Streaming Config] camera {c.id}: Cam-Webs concurrent stream "
                    f"{_redact_rtsp(stream)} ({codec} {w}x{h}) + downscale — "
                    f"sender-crop uses main {_redact_rtsp(main)}"
                )
            elif cam_webs != stream:
                ok2, w2, h2, codec2 = _substream_is_usable(cam_webs, ffmpeg_path)
                if ok2 and w2 > 0 and (w2 < w or h2 < h):
                    print(
                        f"[Streaming Config] camera {c.id}: Cam-Webs substream "
                        f"{_redact_rtsp(cam_webs)} ({codec2} {w2}x{h2})"
                    )
                    stream = cam_webs
                else:
                    stream = cam_webs_major_url(main)
                    vendor = vendor or "cam_webs"
                    scale = _STREAMING_FALLBACK_SCALE
                    print(
                        f"[Streaming Config] camera {c.id}: no usable substream "
                        f"({codec} {w}x{h}); main + downscale 640x360 "
                        f"{_redact_rtsp(stream)}"
                    )
            else:
                print(
                    f"[Streaming Config] camera {c.id}: substream is full resolution "
                    f"({codec} {w}x{h}); main + downscale 640x360 {_redact_rtsp(main)}"
                )
                stream = cam_webs_major_url(main)
                if stream != main:
                    vendor = vendor or "cam_webs"
                scale = _STREAMING_FALLBACK_SCALE

    transport = _resolve_rtsp_transport(
        explicit=c.streaming_rtsp_transport,
        main=main,
        vendor=vendor,
        scale=scale,
        stream_url=stream,
    )

    return CameraEntry(
        id=c.id,
        rtsp_url=stream,
        main_rtsp_url=main,
        streaming_rtsp_url=c.streaming_rtsp_url,
        rtsp_vendor=vendor,
        streaming_scale_filter=scale,
        streaming_rtsp_transport=transport,
    )


def _apply_substream_to_cameras(
    cameras: list[CameraEntry],
    use_substream: bool,
    ffmpeg_path: str = "ffmpeg",
) -> list[CameraEntry]:
    return [
        _resolve_streaming_rtsp_url(c, use_substream=use_substream, ffmpeg_path=ffmpeg_path)
        for c in cameras
    ]


def _redact_rtsp(url: str) -> str:
    if "@" not in url:
        return url
    try:
        head, tail = url.split("://", 1)
        host = tail.split("@", 1)[-1]
        return f"{head}://***@{host}"
    except Exception:
        return url


def _camera_entry_from_yaml_item(item: dict[str, Any], rtsp_url: str) -> CameraEntry:
    cid = str(item.get("camera_id") or item.get("id") or "").strip()
    vendor = str(item.get("rtsp_vendor") or item.get("streaming_vendor") or "").strip()
    streaming = str(item.get("streaming_rtsp_url") or item.get("streaming_source") or "").strip()
    scale = str(item.get("streaming_scale_filter") or "").strip()
    transport = str(
        item.get("streaming_rtsp_transport") or item.get("rtsp_transport") or ""
    ).strip()
    return CameraEntry(
        id=cid,
        rtsp_url=rtsp_url,
        main_rtsp_url=rtsp_url,
        streaming_rtsp_url=streaming,
        rtsp_vendor=vendor,
        streaming_scale_filter=scale,
        streaming_rtsp_transport=transport,
    )


def _parse_camera(obj: dict[str, Any]) -> CameraEntry | None:
    cid = obj.get("id") or obj.get("camera_id")
    if not cid:
        return None
    cid = str(cid).strip()
    url = obj.get("rtsp_url") or obj.get("source") or obj.get("url")
    if not url or not str(url).strip():
        return None
    return _camera_entry_from_yaml_item(obj, str(url).strip())


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

        out.append(_camera_entry_from_yaml_item(item, rtsp_url))
    return out


def load_config(
    path: str | Path | None = None, *, probe_cameras: bool = True
) -> AgentConfig:
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
        or "upstream_rtmp"
    ).strip().lower()
    if delivery not in ("local_webrtc", "upstream_rtmp", "both"):
        delivery = "upstream_rtmp"

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
            or "wss://api.retailsolution.ai/ws/agents"
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
        restart_base_delay_sec=_env_float(
            "STREAMING_RESTART_BASE_DELAY_SEC",
            float(data.get("restart_base_delay_sec", 6.0)),
        ),
        restart_max_delay_sec=_env_float(
            "STREAMING_RESTART_MAX_DELAY_SEC",
            float(data.get("restart_max_delay_sec", 90.0)),
        ),
        rtmp_republish_cooldown_sec=_env_float(
            "STREAMING_RTMP_REPUBLISH_COOLDOWN_SEC",
            float(data.get("rtmp_republish_cooldown_sec", 15.0)),
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
            rtsp_relay_enabled=_env_bool(
                "STREAMING_RTSP_RELAY_ENABLED",
                bool(med.get("rtsp_relay_enabled", True)),
            ),
            rtsp_listen_base=str(
                med.get("rtsp_listen_base")
                or _env("STREAMING_RTSP_RELAY_BASE")
                or "rtsp://127.0.0.1:8554"
            ).rstrip("/"),
        ),
        upstream=UpstreamConfig(
            rtmp_url_template=str(
                up.get("rtmp_url_template")
                or _env("STREAMING_UPSTREAM_RTMP_URL_TEMPLATE")
                or "rtmp://64.227.62.36:1935/live/{stream_key}"
            ),
            playback_url_template=str(
                up.get("playback_url_template")
                or _env("STREAMING_UPSTREAM_PLAYBACK_URL_TEMPLATE")
                or "https://stream.retailsolution.ai/live/{stream_key}/index.m3u8"
            ),
            ffmpeg_path=str(up.get("ffmpeg_path") or _env("STREAMING_FFMPEG_PATH") or "ffmpeg"),
            ffmpeg_extra_args=list(up.get("ffmpeg_extra_args") or []),
            transcode=_env_bool(
                "STREAMING_UPSTREAM_TRANSCODE",
                bool(up.get("transcode", True)),
            ),
            transcode_opts=_parse_transcode_opts(up.get("transcode_opts")),
        ),
        cameras=cameras,
        use_substream=_env_bool(
            "STREAMING_USE_SUBSTREAM",
            bool(data.get("use_substream", True)),
        ),
    )

    if probe_cameras:
        cfg.cameras = _apply_substream_to_cameras(
            cfg.cameras,
            cfg.use_substream,
            ffmpeg_path=cfg.upstream.ffmpeg_path,
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


def relay_path_for_camera(camera_id: str) -> str:
    """MediaMTX path for upstream RTSP relay (separate from WebRTC path names)."""
    return f"up_{path_name_for_camera(camera_id)}"


def relay_rtsp_url(listen_base: str, camera_id: str) -> str:
    base = (listen_base or "rtsp://127.0.0.1:8554").rstrip("/")
    return f"{base}/{relay_path_for_camera(camera_id)}"
