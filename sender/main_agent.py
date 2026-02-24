import os
import json
import time
import signal
import sys
from urllib.parse import urlparse, urlunparse
from typing import Dict
try:
    from dotenv import load_dotenv
    _root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    load_dotenv(os.path.join(_root, ".env"))
except ImportError:
    pass
from snapshot_capture_agent import SnapshotCaptureAgent
from kafka_sender_agent import KafkaSenderAgent
from camera_manager import CameraManager
from face_crop import detect_faces, crop_faces
from person_crop import detect_persons, crop_persons


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


class MainAgent:
    """Main coordinator agent. Configuration from .env only."""

    def __init__(self, cameras_config_path: str = None):
        self.running = False
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
            raw = CameraManager._fetch_cameras_from_backend_raw(backend_cameras_url, backend_timeout, token or None)
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

        # Kafka — приоритет: SENDER_KAFKA_* (из docker-compose/.env), затем KAFKA_*
        bootstrap_servers = _env('KAFKA_BOOTSTRAP_SERVERS') or 'localhost:9092'
        bootstrap_servers = str(bootstrap_servers).replace('http://', '').replace('https://', '').rstrip('/')

        topic = _env('KAFKA_TOPIC') or 'snapshots'
        jpeg_quality = int(_env_float('KAFKA_JPEG_QUALITY', 85))
        print(f"[Main Agent] Kafka: {bootstrap_servers}, topic={topic}")

        # MinIO — только из .env
        minio_enabled = _env_bool('MINIO_ENABLED', True)
        minio_config = {
            'enabled': minio_enabled,
            'endpoint': _env('SENDER_MINIO_ENDPOINT') or _env('MINIO_ENDPOINT') or 'localhost:9000',
            'bucket': _env('MINIO_BUCKET') or 'crops',
            'access_key': _env('MINIO_ACCESS_KEY') or 'minioadmin',
            'secret_key': _env('MINIO_SECRET_KEY') or 'minioadmin',
            'secure': _env_bool('MINIO_SECURE', False),
        }
        for key, env_key in (('bucket', 'MINIO_BUCKET'), ('access_key', 'MINIO_ACCESS_KEY'), ('secret_key', 'MINIO_SECRET_KEY')):
            if _env(env_key):
                minio_config[key] = _env(env_key)
        minio_config['endpoint'] = str(minio_config['endpoint']).replace('http://', '').replace('https://', '').rstrip('/')

        self.sender_agent = KafkaSenderAgent(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            jpeg_quality=jpeg_quality,
            minio_config=minio_config if minio_config.get('enabled') else None,
        )

        # Detection — только из .env
        self.detection_type = (_env('DETECTION_TYPE') or 'person').lower()
        self.person_conf = _env_float('PERSON_CONF', 0.4)
        self.person_iou = _env_float('PERSON_IOU', 0.5)
        self.person_model_path = _env('PERSON_MODEL_PATH') or None
        self.rtsp_use_ffmpeg_pipe = _env_bool('RTSP_USE_FFMPEG_PIPE', True)
        if self.detection_type == 'person':
            from person_crop import is_available
            if not is_available():
                print("[Main Agent] Person detector (person_detect binary) not found, falling back to face detection")
                self.detection_type = 'face'
            else:
                print("[Main Agent] Using person detection (cpu-person-detection binary)")
        if self.detection_type == 'face':
            print("[Main Agent] Using face detection (OpenCV Haar)")
        if self.rtsp_use_ffmpeg_pipe:
            print("[Main Agent] RTSP capture: using FFmpeg pipe (avoids RTP/decoding errors)")
        
        # Capture agents for each camera
        self.capture_agents: Dict[str, SnapshotCaptureAgent] = {}
        self._init_cameras()
        
        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _init_cameras(self):
        """Initialize all cameras from CameraManager (cameras.yaml or backend)."""
        cameras = self.camera_manager.get_enabled_cameras()
        if not cameras:
            print("[Main Agent] WARNING: No cameras. Set CAMERAS_CONFIG_PATH or BACKEND_CAMERAS_URL and cameras.yaml")
        
        # Initialize capture agents
        for camera in cameras:
            if not camera.enabled:
                print(f"[Main Agent] Camera {camera.camera_id} disabled")
                continue
            
            # Build source URL for IP cameras
            if camera.type in ['rtsp', 'http']:
                source = self.camera_manager._build_source_url(camera)
            else:
                source = camera.source
            
            # Skip capture for cameras without URL (saved in cameras.yaml; will work when backend adds ddns_rtsp_url/ddns_stream_url)
            if not source or (isinstance(source, str) and not source.strip()):
                print(f"[Main Agent] Camera {camera.camera_id} has no stream URL, skipping capture (add ddns_rtsp_url/ddns_stream_url in backend)")
                continue
            
            # FPS: camera-specific env (e.g. CAM1_FPS), then DEFAULT_FPS, then cameras.yaml
            fps = camera.fps
            env_fps_key = f"{camera.camera_id.upper()}_FPS"
            default_fps_key = "DEFAULT_FPS"
            
            if os.getenv(env_fps_key):
                try:
                    fps = float(os.getenv(env_fps_key))
                    print(f"[Main Agent] FPS for {camera.camera_id} from env {env_fps_key}: {fps}")
                except ValueError:
                    pass
            elif os.getenv(default_fps_key):
                try:
                    fps = float(os.getenv(default_fps_key))
                    print(f"[Main Agent] Default FPS from env {default_fps_key}: {fps}")
                except ValueError:
                    pass
            
            use_ffmpeg_pipe = self.rtsp_use_ffmpeg_pipe if camera.type == 'rtsp' else None
            capture_agent = SnapshotCaptureAgent(
                source=source,
                fps=fps,
                width=camera.width,
                height=camera.height,
                camera_id=camera.camera_id,
                camera_type=camera.type,
                use_ffmpeg_pipe_rtsp=use_ffmpeg_pipe
            )
            
            self.capture_agents[camera.camera_id] = capture_agent
            print(f"[Main Agent] Initialized camera: {camera.camera_id} "
                  f"({camera.name}, type: {camera.type}, source: {source})")
        
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
        
        # Connect to Kafka (create producer)
        if not self.sender_agent.connect():
            print("[Main Agent] Failed to connect to Kafka")
            return False
        
        # Start capture for all cameras
        for camera_id, capture_agent in self.capture_agents.items():
            try:
                # Create callback with proper closure for camera_id
                def make_callback(cam_id):
                    def callback(frame, timestamp):
                        self._on_frame_captured(frame, timestamp, cam_id)
                    return callback
                
                capture_agent.start(callback=make_callback(camera_id))
                print(f"[Main Agent] Capture for camera {camera_id} started")
            except Exception as e:
                print(f"[Main Agent] Error starting camera {camera_id}: {e}")
        
        if not self.capture_agents:
            print("[Main Agent] No active cameras for capture")
            return False
        
        self.running = True
        print(f"[Main Agent] System started and running. Active cameras: {len(self.capture_agents)}")
        
        # Main loop
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
        
        return True
    
    def _on_frame_captured(self, frame, timestamp: float, camera_id: str):
        """Detect people (face or person), crop, publish each crop to Kafka."""
        if not self.running:
            return
        if self.detection_type == 'person':
            rects = detect_persons(
                frame,
                model_path=self.person_model_path,
                conf_threshold=self.person_conf,
                iou_threshold=self.person_iou,
            )
            crops = crop_persons(frame, rects)
        else:
            rects = detect_faces(frame)
            crops = crop_faces(frame, rects)
        if not crops:
            return
        for crop_img in crops:
            self.sender_agent.send_snapshot(crop_img, timestamp, camera_id)
    
    def stop(self):
        """Stop all agents"""
        print("[Main Agent] Stopping system...")
        self.running = False
        
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
        Update frame rate
        
        Args:
            new_fps: New frame rate
            camera_id: Camera identifier (if None, updates all cameras)
        """
        if camera_id:
            if camera_id in self.capture_agents:
                print(f"[Main Agent] Updating FPS for camera {camera_id}: {new_fps}")
                self.capture_agents[camera_id].update_fps(new_fps)
            else:
                print(f"[Main Agent] Camera {camera_id} not found")
        else:
            print(f"[Main Agent] Updating FPS for all cameras: {new_fps}")
            for cam_id, capture_agent in self.capture_agents.items():
                capture_agent.update_fps(new_fps)


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
