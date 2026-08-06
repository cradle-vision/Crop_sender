import os
import json
import time
import signal
import sys
import threading
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse
from typing import Dict, Union, Optional

import numpy as np

try:
    import requests

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False
try:
    from dotenv import load_dotenv
    _root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    load_dotenv(os.path.join(_root, ".env"))
except ImportError:
    pass
from snapshot_capture_agent import SnapshotCaptureAgent
from kafka_sender_agent import KafkaSenderAgent
from camera_manager import CameraManager
from person_crop import detect_persons, crop_persons, is_available as person_detector_available
from jpeg_utils import encode_jpeg_bgr
from roi_command_server import RoiCommandServer
from roi_local_http import RoiLocalHttpServer
from store_tracking import StoreTrackingPublisher

# Project root on sys.path for agent_control (sender/main_agent.py entrypoint).
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
try:
    from agent_control import AgentControlClient, ControlCallbacks
except ImportError:
    AgentControlClient = None  # type: ignore
    ControlCallbacks = None  # type: ignore

_STREAM_TYPES = frozenset({"rtsp", "http", "file"})


def _capture_transport_type(camera_type: str, source: Union[int, str, None]) -> str:
    """Map backend types (entry/exit) to actual capture transport."""
    if camera_type in _STREAM_TYPES:
        return camera_type
    if isinstance(source, int):
        return "rtsp"
    src = str(source).strip() if source is not None else ""
    low = src.lower()
    if low.startswith("rtsp://") or low.startswith("rtsps://"):
        return "rtsp"
    if low.startswith("http://") or low.startswith("https://"):
        return "http"
    return "rtsp"


def _env(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return v.strip() if v else default


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes")


def _env_float(key: str, default: float) -> float:
    try:
        v = os.getenv(key)
        return float(v) if v else default
    except (ValueError, TypeError):
        return default


def _env_int(key: str, default: int, min_v: int = 0, max_v: int = 2147483647) -> int:
    try:
        raw = os.getenv(key)
        v = int(float(raw)) if raw not in (None, "") else int(default)
    except (ValueError, TypeError):
        v = int(default)
    return max(min_v, min(max_v, v))


class MainAgent:
    """Main coordinator agent. Configuration from .env only."""

    def __init__(self, cameras_config_path: str = None):
        self.running = False
        self._snapshot_sent = set()
        self._snapshot_last_attempt: Dict[str, float] = {}
        self._snapshot_retry_interval = max(10.0, _env_float("BACKEND_SNAPSHOT_RETRY_SEC", 30.0))
        self._snapshot_upload_timeout = max(5.0, _env_float("BACKEND_SNAPSHOT_UPLOAD_TIMEOUT_SEC", 30.0))
        self._sender_debug = _env_bool("SENDER_DEBUG", False)
        self._debug_no_person_interval = max(2.0, _env_float("SENDER_DEBUG_NO_PERSON_INTERVAL_SEC", 15.0))
        self._debug_miss_state: Dict[str, dict] = {}
        self._backend_bearer_token = None
        self.backend_snapshot_base_url = None
        self._roi_poll_interval = _env_float('ROI_POLL_INTERVAL_SEC', 0.0)
        self._roi_last_sync_at: Optional[str] = None
        self._roi_sync_stop = threading.Event()
        self._roi_sync_thread: Optional[threading.Thread] = None
        self._roi_sync_lock = threading.Lock()
        self._roi_command_server: Optional[RoiCommandServer] = None
        self._roi_http_server: Optional[RoiLocalHttpServer] = None
        self._roi_socket_path = _env('SENDER_ROI_SOCKET_PATH') or '/app/config/sender-roi.sock'
        self._roi_http_port = _env_int('SENDER_ROI_HTTP_PORT', 18765, 1, 65535)
        backend_timeout = _env_float('BACKEND_CAMERAS_TIMEOUT', 10.0)

        # Camera manager: from backend API or from file
        backend_cameras_url = _env('BACKEND_CAMERAS_URL')
        cameras_config = _env('CAMERAS_CONFIG_PATH') or cameras_config_path or 'cameras.yaml'
        cameras_config = self._resolve_path(cameras_config)
        self.camera_manager = CameraManager(config_file=cameras_config if not backend_cameras_url else "", auto_save=False)
        if backend_cameras_url:
            token = _env('BACKEND_CAMERAS_TOKEN')
            if not token:
                username = _env('BACKEND_CAMERAS_USERNAME')
                password = _env('BACKEND_CAMERAS_PASSWORD')
                if username and password:
                    parsed = urlparse(backend_cameras_url)
                    token_url = _env('BACKEND_TOKEN_URL') or urlunparse((parsed.scheme, parsed.netloc, '/token', '', '', ''))
                    if token_url:
                        token = CameraManager.fetch_backend_token(
                            token_url, username.strip(), password.strip(),
                            timeout=backend_timeout,
                            scope=_env('BACKEND_TOKEN_SCOPE') or 'camera:read',
                        )
            if token:
                self._backend_bearer_token = str(token).strip()
            raw = CameraManager._fetch_cameras_from_backend_raw(backend_cameras_url, backend_timeout, token or None)
            try:
                parsed_for_snapshot = urlparse(backend_cameras_url)
                if parsed_for_snapshot.scheme and parsed_for_snapshot.netloc:
                    self.backend_snapshot_base_url = f"{parsed_for_snapshot.scheme}://{parsed_for_snapshot.netloc}"
            except Exception:
                self.backend_snapshot_base_url = None
            if raw is not None:
                _cameras_dir = os.path.dirname(cameras_config)
                _raw_path = os.path.join(_cameras_dir, 'backend_cameras.json') if _cameras_dir else 'backend_cameras.json'
                try:
                    with open(_raw_path, 'w', encoding='utf-8') as f:
                        json.dump(raw, f, ensure_ascii=False, indent=2)
                    print(f"[Main Agent] Raw backend response saved to {_raw_path}")
                except Exception as e:
                    print(f"[Main Agent] Could not save raw JSON to {_raw_path}: {e}")
                data = CameraManager._normalize_backend_response(raw)
                self.camera_manager.load_cameras_from_data(data)
                self.camera_manager.config_file = cameras_config
                n = len(self.camera_manager.cameras)
                if self.camera_manager.save_cameras():
                    print(f"[Main Agent] OK: {n} camera(s) from backend saved to {cameras_config}")
                else:
                    print(f"[Main Agent] Cameras loaded from backend ({n}), save to {cameras_config} failed")
            else:
                print(f"[Main Agent] Backend request failed (no response). Check URL and token.")
                fallback_path = os.path.normpath(os.path.join(os.path.dirname(cameras_config), 'config', 'cameras.yaml')) if cameras_config and os.path.isdir(cameras_config) else cameras_config
                if fallback_path and os.path.isfile(fallback_path):
                    self.camera_manager.config_file = fallback_path
                    self.camera_manager.load_cameras()

        self._roi_last_sync_at = self._initial_roi_sync_cursor()

        bootstrap_servers = (
            _env('KAFKA_BOOTSTRAP_SERVERS')
            or _env('SENDER_KAFKA_BOOTSTRAP_SERVERS')
            or '64.227.62.36:9092'
        )
        bootstrap_servers = str(bootstrap_servers).replace('http://', '').replace('https://', '').rstrip('/')

        topic = _env('KAFKA_TOPIC') or 'snapshots'
        jpeg_quality = 100
        print(f"[Main Agent] Kafka: {bootstrap_servers}, topic={topic}")

        minio_enabled = _env_bool('MINIO_ENABLED', True)
        minio_config = {
            'enabled': minio_enabled,
            'endpoint': (
                _env('SENDER_MINIO_ENDPOINT')
                or _env('MINIO_ENDPOINT')
                or '64.227.62.36:9005'
            ),
            'bucket': _env('MINIO_BUCKET') or 'visitorstorage',
            'access_key': _env('MINIO_ACCESS_KEY') or 'minioadmin',
            'secret_key': _env('MINIO_SECRET_KEY') or 'minioadmin',
            'secure': _env_bool('MINIO_SECURE', False),
        }
        for key, env_key in (('bucket', 'MINIO_BUCKET'), ('access_key', 'MINIO_ACCESS_KEY'), ('secret_key', 'MINIO_SECRET_KEY')):
            if _env(env_key):
                minio_config[key] = _env(env_key)
        minio_config['endpoint'] = str(minio_config['endpoint']).replace('http://', '').replace('https://', '').rstrip('/')

        buffer_max_bytes_raw = _env('KAFKA_BUFFER_MAX_BYTES')
        buffer_max_bytes = None
        if buffer_max_bytes_raw:
            try:
                buffer_max_bytes = int(buffer_max_bytes_raw)
            except (ValueError, TypeError):
                buffer_max_bytes = None

        resilience_config = {
            'reconnect_sec': max(2.0, _env_float('KAFKA_RECONNECT_SEC', 15.0)),
            'offline_log_sec': max(15.0, _env_float('KAFKA_OFFLINE_LOG_SEC', 60.0)),
            'broker_check_sec': max(3.0, _env_float('KAFKA_BROKER_CHECK_SEC', 10.0)),
            'delivery_timeout_sec': max(3.0, _env_float('KAFKA_DELIVERY_TIMEOUT_SEC', 10.0)),
            'minio_connect_timeout_sec': max(3.0, _env_float('MINIO_CONNECT_TIMEOUT_SEC', 10.0)),
            'minio_read_timeout_sec': max(5.0, _env_float('MINIO_READ_TIMEOUT_SEC', 60.0)),
            'buffer_enabled': _env_bool('KAFKA_BUFFER_ENABLED', True),
            'buffer_max_items': _env_int('KAFKA_BUFFER_MAX_ITEMS', 340000, 1, 10000000),
            'buffer_dir': _env('KAFKA_BUFFER_DIR') or '/app/config/kafka_buffer',
            'buffer_replay_batch': _env_int('KAFKA_BUFFER_REPLAY_BATCH', 20, 1, 1000),
            'buffer_replay_max_rounds': _env_int('KAFKA_BUFFER_REPLAY_MAX_ROUNDS', 30, 1, 500),
            'drain_poll_sec': max(1.0, _env_float('KAFKA_DRAIN_POLL_SEC', 2.0)),
            'buffer_max_bytes': buffer_max_bytes,
            'local_spill_enabled': _env_bool('KAFKA_LOCAL_SPILL_ENABLED', True),
            'local_spill_dir': _env('KAFKA_LOCAL_SPILL_DIR') or '/app/config/kafka_spill',
            'drain_backlog_before_live': _env_bool('KAFKA_DRAIN_BACKLOG_BEFORE_LIVE', True),
            'replay_minio_workers': _env_int('KAFKA_REPLAY_MINIO_WORKERS', 8, 1, 32),
            'replay_prefetch_ahead': _env_int('KAFKA_REPLAY_PREFETCH_AHEAD', 32, 1, 500),
            'replay_minio_retries': _env_int('KAFKA_REPLAY_MINIO_RETRIES', 3, 1, 10),
        }
        print(
            f"[Main Agent] Kafka buffer: enabled={resilience_config['buffer_enabled']}, "
            f"max_items={resilience_config['buffer_max_items']}, "
            f"replay_batch={resilience_config['buffer_replay_batch']}, "
            f"replay_minio_workers={resilience_config['replay_minio_workers']}, "
            f"replay_prefetch={resilience_config['replay_prefetch_ahead']}, "
            f"dir={resilience_config['buffer_dir']}, "
            f"local_spill={resilience_config['local_spill_enabled']}"
        )

        self.sender_agent = KafkaSenderAgent(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            jpeg_quality=jpeg_quality,
            minio_config=minio_config if minio_config.get('enabled') else None,
            resilience_config=resilience_config,
        )
        self.store_tracking = StoreTrackingPublisher(self.sender_agent)

        # Detection: CPU person detection only (person_detect binary)
        self.person_conf = _env_float('PERSON_CONF', 0.60)
        self.person_iou = _env_float('PERSON_IOU', 0.30)
        self.person_model_path = _env('PERSON_MODEL_PATH') or None
        if not person_detector_available():
            raise RuntimeError(
                "Person detector (person_detect binary + model) not found. "
                "See cpu-person-detection/ and PERSON_MODEL_PATH."
            )
        print("[Main Agent] Using person detection (cpu-person-detection binary)")
        print("[Main Agent] RTSP capture: using FFmpeg pipe only")

        # Decode/sample rate: FFmpeg fps= filter from DEFAULT_FPS in .env (per-store). 0 = unlimited.
        self.capture_fps = max(0.0, _env_float("DEFAULT_FPS", 3.0))
        self.processing_queue_max = _env_int("PROCESSING_QUEUE_MAX", 200, 1, 500)
        self.processing_workers = _env_int("PROCESSING_WORKERS", 3, 1, 16)
        print(
            f"[Main Agent] DEFAULT_FPS={self.capture_fps} "
            f"(FFmpeg fps filter; 0 = unlimited), "
            f"PROCESSING_QUEUE_MAX={self.processing_queue_max}, "
            f"PROCESSING_WORKERS={self.processing_workers}"
        )
        if self._sender_debug:
            print(
                f"[Main Agent] SENDER_DEBUG=1: person hits logged; "
                f"'no crop' summary every {self._debug_no_person_interval:.0f}s per camera "
                f"(SENDER_DEBUG_NO_PERSON_INTERVAL_SEC)"
            )

        # Capture agents for each camera
        self.capture_agents: Dict[str, SnapshotCaptureAgent] = {}
        self._init_cameras()
        self._control_client = None
        
        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _maybe_start_agent_control(self) -> None:
        """WS desired-state client when streaming-agent is not owning /ws/agents."""
        if AgentControlClient is None:
            return
        enabled = _env_bool("AGENT_CONTROL_ENABLED", True)
        if not enabled:
            return
        if not _env_bool("AGENT_CONTROL_FORCE", False):
            # Allow streaming-agent (same compose up) to claim the shared marker first.
            wait_sec = _env_float("AGENT_CONTROL_OWNER_WAIT_SEC", 3.0)
            if wait_sec > 0:
                time.sleep(wait_sec)
            try:
                from agent_control.ws_owner import current_ws_owner

                owner = current_ws_owner()
            except ImportError:
                owner = None
            if owner == "streaming":
                print(
                    "[Main Agent] agent control skipped (streaming-agent owns WS via marker); "
                    "set AGENT_CONTROL_FORCE=1 to override"
                )
                return
            if _env_bool("STREAMING_AGENT_ENABLED", False):
                print(
                    "[Main Agent] agent control skipped (STREAMING_AGENT_ENABLED); "
                    "set AGENT_CONTROL_FORCE=1 to override"
                )
                return
        agent_id = _env("STREAMING_AGENT_ID") or _env("AGENT_ID")
        token = _env("STREAMING_AGENT_TOKEN") or _env("AGENT_TOKEN")
        backend_url = (
            _env("STREAMING_BACKEND_URL")
            or _env("AGENT_BACKEND_URL")
            or "wss://api.retailsolution.ai/ws/agents"
        )
        if not agent_id or not token:
            print(
                "[Main Agent] agent control disabled: set STREAMING_AGENT_ID, "
                "STREAMING_AGENT_TOKEN"
            )
            return
        env_path = _env("AGENT_ENV_PATH") or os.path.join(_ROOT, ".env")
        callbacks = ControlCallbacks(
            on_reload_cameras=self._reload_cameras_from_backend,
            request_restart=self._request_control_restart,
        )
        self._control_client = AgentControlClient(
            backend_url=backend_url,
            agent_id=agent_id,
            auth_token=token,
            env_path=env_path,
            callbacks=callbacks,
            heartbeat_interval_sec=_env_float("STREAMING_HEARTBEAT_INTERVAL_SEC", 15.0),
        )
        self._control_client.start_background()

    def _watch_streaming_ws_owner(self) -> None:
        """If streaming-agent starts later, drop our WS client to avoid dual sessions."""
        try:
            from agent_control.ws_owner import current_ws_owner
        except ImportError:
            return
        while self.running:
            time.sleep(5.0)
            if current_ws_owner() != "streaming":
                continue
            if not self._control_client:
                return
            print(
                "[Main Agent] streaming-agent claimed WS; stopping sender agent control"
            )
            try:
                self._control_client.stop()
            except Exception as e:
                print(f"[Main Agent] Error stopping agent control: {e}")
            self._control_client = None
            return

    def _request_control_restart(self) -> None:
        """Exit after control apply so Docker restart policy reloads env."""
        print("[Main Agent] control plane requested process restart")
        try:
            self.stop()
        except Exception as e:
            print(f"[Main Agent] stop before restart failed: {e}")
        os._exit(0)

    def _reload_cameras_from_backend(self) -> None:
        """Re-fetch cameras from BACKEND_CAMERAS_URL or reload cameras.yaml, then sync capture."""
        print("[Main Agent] reload_cameras requested")
        backend_cameras_url = _env("BACKEND_CAMERAS_URL")
        cameras_config = _env("CAMERAS_CONFIG_PATH") or "cameras.yaml"
        cameras_config = self._resolve_path(cameras_config)
        backend_timeout = _env_float("BACKEND_CAMERAS_TIMEOUT", 10.0)
        if backend_cameras_url:
            token = self._backend_bearer_token or _env("BACKEND_CAMERAS_TOKEN") or None
            raw = CameraManager._fetch_cameras_from_backend_raw(
                backend_cameras_url, backend_timeout, token
            )
            if raw is None:
                raise RuntimeError("backend cameras fetch failed")
            data = CameraManager._normalize_backend_response(raw)
            self.camera_manager.load_cameras_from_data(data)
            self.camera_manager.config_file = cameras_config
            self.camera_manager.save_cameras()
            print(f"[Main Agent] reload_cameras: {len(self.camera_manager.cameras)} camera(s) from backend")
        else:
            self.camera_manager.config_file = cameras_config
            self.camera_manager.reload()
            print(f"[Main Agent] reload_cameras: {len(self.camera_manager.cameras)} camera(s) from file")
        self._sync_capture_agents()

    def _frame_callback_for(self, camera_id: str):
        def callback(frame, timestamp):
            self._on_frame_captured(frame, timestamp, camera_id)
        return callback

    def _build_capture_agent(self, camera) -> Optional["SnapshotCaptureAgent"]:
        transport = _capture_transport_type(camera.type, camera.source)
        if transport in ("rtsp", "http"):
            source = self.camera_manager._build_source_url(camera)
        else:
            source = camera.source
        if not source or (isinstance(source, str) and not source.strip()):
            print(
                f"[Main Agent] Camera {camera.camera_id} has no stream URL, skipping capture "
                "(add ddns_rtsp_url/ddns_stream_url in backend)"
            )
            return None
        return SnapshotCaptureAgent(
            source=source,
            fps=self.capture_fps,
            width=camera.width,
            height=camera.height,
            camera_id=camera.camera_id,
            camera_type=_capture_transport_type(camera.type, source),
            processing_queue_max=self.processing_queue_max,
            processing_workers=self.processing_workers,
        )

    def _start_capture_agent(self, camera_id: str, capture_agent: "SnapshotCaptureAgent") -> None:
        try:
            capture_agent.start(callback=self._frame_callback_for(camera_id))
            print(f"[Main Agent] Capture for camera {camera_id} started")
        except Exception as e:
            print(f"[Main Agent] Error starting camera {camera_id}: {e}")

    def _sync_capture_agents(self) -> None:
        """Start/stop/restart capture agents to match current camera_manager state."""
        desired: Dict[str, SnapshotCaptureAgent] = {}
        for camera in self.camera_manager.get_enabled_cameras():
            if not camera.enabled:
                continue
            agent = self._build_capture_agent(camera)
            if agent is None:
                continue
            desired[camera.camera_id] = agent

        for camera_id in list(self.capture_agents.keys()):
            if camera_id not in desired:
                try:
                    self.capture_agents[camera_id].stop()
                except Exception as e:
                    print(f"[Main Agent] Error stopping removed camera {camera_id}: {e}")
                del self.capture_agents[camera_id]
                print(f"[Main Agent] Removed capture for camera {camera_id}")

        for camera_id, new_agent in desired.items():
            existing = self.capture_agents.get(camera_id)
            if existing is not None and str(existing.source) == str(new_agent.source):
                continue
            if existing is not None:
                try:
                    existing.stop()
                except Exception as e:
                    print(f"[Main Agent] Error restarting camera {camera_id}: {e}")
                print(f"[Main Agent] Restarting capture for camera {camera_id} (source changed)")
            self.capture_agents[camera_id] = new_agent
            print(
                f"[Main Agent] {'Updated' if existing else 'Initialized'} camera: {camera_id} "
                f"(capture: {new_agent.camera_type}, source: {new_agent.source})"
            )
            if self.running:
                self._start_capture_agent(camera_id, new_agent)

    def _init_cameras(self):
        """Initialize all cameras from CameraManager (cameras.yaml or backend)."""
        cameras = self.camera_manager.get_enabled_cameras()
        if not cameras:
            print("[Main Agent] WARNING: No cameras. Set CAMERAS_CONFIG_PATH or BACKEND_CAMERAS_URL and cameras.yaml")
        self._sync_capture_agents()

    def _resolve_path(self, path: str) -> str:
        """Resolve path: if relative and not found in cwd, try project root (parent of sender/)."""
        if os.path.isabs(path) and os.path.isfile(path):
            return path
        if os.path.isfile(path):
            return os.path.abspath(path)
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        fallback = os.path.join(root, path)
        if os.path.isfile(fallback):
            return fallback
        return path

    def _signal_handler(self, signum, frame):
        """Signal handler for graceful shutdown"""
        print("\n[Main Agent] Received shutdown signal...")
        self.stop()
        sys.exit(0)
    
    def start(self):
        """Start all agents"""
        print("[Main Agent] Starting snapshot sending system...")
        
        # Connect MinIO (required) and Kafka (optional — capture continues if broker is down)
        if not self.sender_agent.connect():
            print("[Main Agent] Failed to connect to MinIO (and local spill is disabled)")
            return False

        self.sender_agent.start_auto_reconnect()
        self._maybe_start_agent_control()

        # Start capture for all cameras
        for camera_id, capture_agent in self.capture_agents.items():
            self._start_capture_agent(camera_id, capture_agent)
        if not self.capture_agents:
            print("[Main Agent] No active cameras for capture")
            return False
        
        self.running = True
        if self._control_client and not _env_bool("AGENT_CONTROL_FORCE", False):
            threading.Thread(
                target=self._watch_streaming_ws_owner,
                daemon=True,
                name="ws-owner-watch",
            ).start()
        self._start_roi_command_server()
        self._start_roi_poll_fallback()
        print(f"[Main Agent] System started and running. Active cameras: {len(self.capture_agents)}")
        
        # Main loop
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
        
        return True
    
    def _on_frame_captured(self, frame, timestamp: float, camera_id: str):
        """Detect persons, crop, publish each crop to Kafka (CPU person detection only), plus one-time initial snapshot to backend."""
        if not self.running:
            return
        camera = self.camera_manager.get_camera(camera_id) if hasattr(self, "camera_manager") else None
        if (
            camera
            and getattr(camera, "company_id", None)
            and camera_id not in self._snapshot_sent
        ):
            now = time.monotonic()
            last = self._snapshot_last_attempt.get(camera_id, 0.0)
            if now - last >= self._snapshot_retry_interval:
                self._snapshot_last_attempt[camera_id] = now
                threading.Thread(
                    target=self._initial_snapshot_worker,
                    args=(frame.copy(), timestamp, camera_id, camera),
                    daemon=True,
                    name=f"snapshot-{camera_id}",
                ).start()
        line_params = None
        if camera:
            try:
                vals = (
                    getattr(camera, "line_x1", None),
                    getattr(camera, "line_y1", None),
                    getattr(camera, "line_x2", None),
                    getattr(camera, "line_y2", None),
                    getattr(camera, "inside_x", None),
                    getattr(camera, "inside_y", None),
                )
                is_active = getattr(camera, "line_active", True)
                if is_active and all(v is not None for v in vals):
                    line_params = tuple(int(v) for v in vals)
            except Exception as e:
                print(f"[Main Agent] Line config parse error for camera {camera_id}: {e}")
        rects = detect_persons(
            frame,
            model_path=self.person_model_path,
            conf_threshold=self.person_conf,
            iou_threshold=self.person_iou,
            line_params=line_params,
        )
        crops = crop_persons(frame, rects)
        h, w = frame.shape[:2]

        # Optional company/building/camera metadata for MinIO path and Kafka payload
        company_id = getattr(camera, "company_id", None) if camera else None
        building_id = getattr(camera, "building_id", None) if camera else None
        company_name = getattr(camera, "company_name", None) if camera else None
        building_name = getattr(camera, "building_name", None) if camera else None
        camera_name = getattr(camera, "name", None) if camera else None

        # Indoor tracking: IoU tracks → store_tracking_positions + triton_face_pipeline.
        # Bboxes are already in original frame pixels (detect_persons stdin-bgr, no resize).
        try:
            self.store_tracking.process_frame(
                frame,
                timestamp,
                camera_id,
                company_id,
                building_id,
                rects,
            )
        except Exception as e:
            print(f"[Main Agent] store tracking error camera={camera_id}: {e}")

        if self._sender_debug:
            if crops:
                print(
                    f"[DEBUG] camera={camera_id} person_hit boxes={len(rects)} crops={len(crops)} "
                    f"frame={w}x{h} PERSON_CONF={self.person_conf} PERSON_IOU={self.person_iou}"
                )
                self._debug_miss_state.pop(camera_id, None)
            elif rects:
                print(
                    f"[DEBUG] camera={camera_id} boxes={len(rects)} but 0 crops (unexpected); "
                    f"frame={w}x{h}"
                )
            else:
                st = self._debug_miss_state.setdefault(
                    camera_id,
                    {"since_log": time.monotonic(), "frames": 0},
                )
                st["frames"] += 1
                now = time.monotonic()
                if now - st["since_log"] >= self._debug_no_person_interval:
                    trip = "on" if line_params is not None else "off"
                    print(
                        f"[DEBUG] camera={camera_id} no_crop_summary: {st['frames']} frame(s) in "
                        f"~{self._debug_no_person_interval:.0f}s (no boxes above threshold, detector empty, "
                        f"or tripwire filtered) frame={w}x{h} tripwire={trip} "
                        f"PERSON_CONF={self.person_conf} PERSON_IOU={self.person_iou}"
                    )
                    st["since_log"] = now
                    st["frames"] = 0

        if not crops:
            return
        for crop_img in crops:
            self.sender_agent.send_snapshot(
                crop_img,
                timestamp,
                camera_id,
                company_id,
                building_id,
                company_name,
                building_name,
                camera_name,
            )

    def _initial_roi_sync_cursor(self) -> str:
        latest = None
        for camera in self.camera_manager.cameras.values():
            ts = getattr(camera, "roi_updated_at", None)
            if ts and (latest is None or str(ts) > str(latest)):
                latest = str(ts)
        if latest:
            return latest
        return datetime.now(timezone.utc).isoformat()

    def _company_id_for_backend(self) -> Optional[str]:
        for camera in self.camera_manager.get_enabled_cameras():
            company_id = getattr(camera, "company_id", None)
            if company_id:
                return str(company_id).strip()
        return None

    def _backend_auth_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self._backend_bearer_token:
            headers["Authorization"] = f"Bearer {self._backend_bearer_token}"
        return headers

    def _start_roi_command_server(self) -> None:
        if _env_bool('SENDER_ROI_IPC_ENABLED', True):
            self._roi_command_server = RoiCommandServer(self, socket_path=self._roi_socket_path)
            if not self._roi_command_server.start():
                print("[Main Agent] ROI unix socket failed; check config volume permissions")
        if _env_bool('SENDER_ROI_HTTP_ENABLED', True):
            self._roi_http_server = RoiLocalHttpServer(self, port=self._roi_http_port)
            self._roi_http_server.start()

    def _start_roi_poll_fallback(self) -> None:
        if self._roi_poll_interval > 0:
            self._start_roi_sync_thread()

    def _start_roi_sync_thread(self) -> None:
        if not self.backend_snapshot_base_url or not HAS_REQUESTS or not self._backend_bearer_token:
            if self.backend_snapshot_base_url and not self._backend_bearer_token:
                print("[Main Agent] ROI HTTP fallback disabled: no backend bearer token")
            return
        if self._roi_poll_interval <= 0:
            return
        if self._roi_sync_thread and self._roi_sync_thread.is_alive():
            return
        self._roi_sync_stop.clear()
        self._roi_sync_thread = threading.Thread(
            target=self._roi_sync_loop,
            daemon=True,
            name="roi-sync-fallback",
        )
        self._roi_sync_thread.start()
        print(f"[Main Agent] ROI HTTP fallback polling every {self._roi_poll_interval:.0f}s")

    def _roi_sync_loop(self) -> None:
        while self.running and not self._roi_sync_stop.is_set():
            try:
                self._poll_roi_changes()
            except Exception as e:
                print(f"[Main Agent] ROI fallback poll error: {e}")
            self._roi_sync_stop.wait(self._roi_poll_interval)

    def _poll_roi_changes(self) -> None:
        company_id = self._company_id_for_backend()
        if not company_id or not self.backend_snapshot_base_url or not self._backend_bearer_token:
            return
        base = self.backend_snapshot_base_url.rstrip("/")
        url = f"{base}/company/{company_id}/smartcamera/roi-sync"
        params = {}
        with self._roi_sync_lock:
            if self._roi_last_sync_at:
                params["since"] = self._roi_last_sync_at
        timeout = max(5.0, _env_float('BACKEND_CAMERAS_TIMEOUT', 10.0))
        resp = requests.get(
            url,
            headers=self._backend_auth_headers(),
            params=params,
            timeout=timeout,
            verify=False,
        )
        if resp.status_code == 401:
            print("[Main Agent] ROI sync unauthorized (check BACKEND_CAMERAS_TOKEN / username/password)")
            return
        if resp.status_code != 200:
            print(f"[Main Agent] ROI sync poll failed: HTTP {resp.status_code}")
            return
        items = resp.json()
        if not isinstance(items, list) or not items:
            return

        latest_sync = self._roi_last_sync_at
        applied = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            cam_id = str(item.get("smartcamera_id", "")).strip()
            if not cam_id:
                continue
            updated_at = item.get("updated_at")
            if updated_at and (latest_sync is None or str(updated_at) > str(latest_sync)):
                latest_sync = str(updated_at)
            if self.apply_roi_from_payload(item):
                applied += 1

        if latest_sync:
            with self._roi_sync_lock:
                self._roi_last_sync_at = latest_sync
        if applied:
            print(f"[Main Agent] ROI poll: applied line for {applied} camera(s) (no snapshot)")

    def apply_roi_from_payload(self, body: dict) -> bool:
        """Apply ROI/line from backend payload (WS or HTTP poll). Does not refresh snapshot."""
        if not isinstance(body, dict):
            return False
        cam_id = str(
            body.get("smartcamera_id") or body.get("camera_id") or ""
        ).strip()
        if not cam_id:
            return False
        camera_line = body.get("camera_line") or body.get("line")
        camera_roi = body.get("camera_roi")
        if not isinstance(camera_line, dict) and not isinstance(camera_roi, dict):
            line_keys = (
                "line_x1", "line_y1", "line_x2", "line_y2",
                "inside_x", "inside_y", "line_active", "updated_at",
            )
            flat = {k: body[k] for k in line_keys if k in body}
            if flat:
                camera_line = flat
        updated_at = body.get("updated_at")
        if updated_at:
            if isinstance(camera_line, dict):
                camera_line = dict(camera_line)
                camera_line.setdefault("updated_at", updated_at)
            elif isinstance(camera_roi, dict):
                camera_roi = dict(camera_roi)
                camera_roi.setdefault("updated_at", updated_at)
        applied = self.camera_manager.apply_roi_sync(
            cam_id,
            camera_line=camera_line if isinstance(camera_line, dict) else None,
            camera_roi=camera_roi if isinstance(camera_roi, dict) else None,
            persist=True,
        )
        if applied:
            print(f"[Main Agent] ROI applied for camera {cam_id} (saved to {self.camera_manager.config_file})")
        return applied

    def request_snapshot_refresh(self, camera_id: str) -> bool:
        """Queue fresh snapshot upload to backend. Does not change ROI/line."""
        cam_id = str(camera_id).strip()
        if not cam_id:
            return False
        self._snapshot_sent.discard(cam_id)
        self._snapshot_last_attempt.pop(cam_id, None)
        print(f"[Main Agent] Snapshot refresh queued for camera {cam_id}")
        return True

    def _initial_snapshot_worker(self, frame, timestamp: float, camera_id: str, camera) -> None:
        try:
            if self._send_initial_snapshot(frame, timestamp, camera_id, camera):
                self._snapshot_sent.add(camera_id)
        except Exception as e:
            print(f"[Main Agent] Initial snapshot upload error for camera {camera_id}: {e}")

    def _send_initial_snapshot(self, frame, timestamp: float, camera_id: str, camera) -> bool:
        """Send one original snapshot per camera to backend /company/{company_id}/smartcamera/{smartcamera_id}/snapshot.
        Returns True only on 200 OK (so caller can add to _snapshot_sent and skip retries)."""
        if not HAS_REQUESTS:
            print("[Main Agent] Python package 'requests' not installed; skip initial snapshot upload")
            return False
        if not self.backend_snapshot_base_url:
            print("[Main Agent] backend_snapshot_base_url is not set; skip initial snapshot upload")
            return False
        company_id = getattr(camera, "company_id", None)
        smartcamera_id = getattr(camera, "camera_id", None) or camera_id
        if not company_id or not smartcamera_id:
            print(f"[Main Agent] Missing company_id or smartcamera_id for camera {camera_id}; skip initial snapshot upload")
            return False
        try:
            company_id_str = str(company_id).strip()
            smartcamera_id_str = str(smartcamera_id).strip()
        except Exception:
            return False
        if not company_id_str or not smartcamera_id_str:
            print(f"[Main Agent] Empty company_id or smartcamera_id after str() for camera {camera_id}; skip initial snapshot upload")
            return False
        url = (
            self.backend_snapshot_base_url.rstrip("/")
            + f"/company/{company_id_str}/smartcamera/{smartcamera_id_str}/snapshot"
        )
        if not self._backend_bearer_token:
            print(f"[Main Agent] No backend bearer token; sending initial snapshot for camera {camera_id} without Authorization header")
        else:
            print(f"[Main Agent] Sending initial snapshot for camera {camera_id} -> {url} with bearer token")
        data = encode_jpeg_bgr(frame, quality=85)
        if data is None:
            print(f"[Main Agent] Failed to encode initial snapshot for camera {camera_id}")
            return False
        headers = None
        if self._backend_bearer_token:
            headers = {"Authorization": f"Bearer {self._backend_bearer_token}"}
        files = {
            "file": ("snapshot.jpg", data, "image/jpeg"),
        }
        try:
            resp = requests.post(url, headers=headers, files=files, timeout=self._snapshot_upload_timeout)
            if resp.status_code == 200:
                snapshot_url = None
                try:
                    body = resp.json()
                    if isinstance(body, dict) and body.get("snapshot_url"):
                        snapshot_url = body["snapshot_url"]
                except Exception:
                    pass
                if snapshot_url:
                    bucket = None
                    object_key = None
                    try:
                        parsed_snapshot = urlparse(str(snapshot_url))
                        path_parts = [p for p in parsed_snapshot.path.split("/") if p]
                        if path_parts:
                            bucket = path_parts[0]
                            object_key = "/".join(path_parts[1:]) if len(path_parts) > 1 else ""
                    except Exception:
                        bucket = None
                        object_key = None
                    if bucket is not None:
                        print(
                            f"[Main Agent] Initial snapshot uploaded for camera {camera_id}. "
                            f"bucket={bucket}, object_key={object_key}"
                        )
                    else:
                        print(f"[Main Agent] Initial snapshot uploaded for camera {camera_id}.")
                else:
                    print(f"[Main Agent] Initial snapshot uploaded for camera {camera_id} -> {url}")
                return True
            if resp.status_code == 401:
                print(f"[Main Agent] Unauthorized (401) for initial snapshot camera {camera_id}: invalid or missing bearer token (check BACKEND_CAMERAS_TOKEN / BACKEND_CAMERAS_USERNAME/PASSWORD)")
                return False
            txt = ""
            try:
                txt = resp.text[:200]
            except Exception:
                pass
            print(f"[Main Agent] Initial snapshot upload failed for camera {camera_id}: {resp.status_code} {txt}")
            return False
        except Exception as e:
            print(f"[Main Agent] Request error while uploading initial snapshot for camera {camera_id}: {e}")
            return False
    
    
    def stop(self):
        """Stop all agents"""
        print("[Main Agent] Stopping system...")
        self.running = False
        if self._control_client:
            try:
                self._control_client.stop()
            except Exception as e:
                print(f"[Main Agent] Error stopping agent control: {e}")
            self._control_client = None
        if self._roi_command_server:
            self._roi_command_server.stop()
        if self._roi_http_server:
            self._roi_http_server.stop()
        self._roi_sync_stop.set()
        if self._roi_sync_thread and self._roi_sync_thread.is_alive():
            self._roi_sync_thread.join(timeout=5.0)
        
        # Stop all capture agents
        for camera_id, capture_agent in self.capture_agents.items():
            try:
                capture_agent.stop()
            except Exception as e:
                print(f"[Main Agent] Error stopping camera {camera_id}: {e}")
        
        self.sender_agent.disconnect()
        
        print("[Main Agent] System stopped")
    
    def update_fps(self, new_fps: float, camera_id: str = None):
        """
        Set target capture FPS on agents (stored for next FFmpeg reconnect after stream errors).
        Args:
            new_fps: Target FPS for the fps= filter (0 = full decode rate on reconnect).
            camera_id: Camera identifier (if None, updates all cameras).
        """
        val = max(0.0, float(new_fps))
        self.capture_fps = val
        if camera_id:
            if camera_id in self.capture_agents:
                self.capture_agents[camera_id].update_fps(val)
            else:
                print(f"[Main Agent] Camera {camera_id} not found")
        else:
            for _, capture_agent in self.capture_agents.items():
                capture_agent.update_fps(val)


def main():
    """Main function"""
    agent = MainAgent()
    
    try:
        agent.start()
    except Exception as e:
        print(f"[Main Agent] Critical error: {e}")
        agent.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
