"""
Main Coordinator Agent
Camera → detect people (face or person) → crop → [MinIO] → Kafka → backend → Triton.
"""

import os
import yaml
import time
import signal
import sys
from typing import Dict
try:
    from dotenv import load_dotenv
    # Load .env from project root (parent of sender/)
    _root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    load_dotenv(os.path.join(_root, ".env"))
except ImportError:
    pass
from snapshot_capture_agent import SnapshotCaptureAgent
from kafka_sender_agent import KafkaSenderAgent
from camera_manager import CameraManager
from face_crop import detect_faces, crop_faces
from person_crop import detect_persons, crop_persons


class MainAgent:
    """Main coordinator agent"""
    
    def __init__(self, config_path: str = "config.yaml", cameras_config_path: str = None):
        """
        Initialize main agent
        
        Args:
            config_path: Path to config file
            cameras_config_path: Path to cameras config file (if None, uses cameras.yaml)
        """
        self.config = self._load_config(config_path)
        self.running = False
        
        # Camera manager: from backend API (on startup fetch → save to cameras.yaml) or from file
        backend_cameras_url = os.getenv('BACKEND_CAMERAS_URL') or self.config.get('backend', {}).get('cameras_url')
        cameras_config = os.getenv('CAMERAS_CONFIG_PATH') or cameras_config_path or self.config.get('cameras_config', 'cameras.yaml')
        cameras_config = self._resolve_path(cameras_config)
        self.camera_manager = CameraManager(config_file=cameras_config if not backend_cameras_url else "", auto_save=False)
        if backend_cameras_url:
            backend_cfg = self.config.get('backend', {})
            token = os.getenv('BACKEND_CAMERAS_TOKEN') or backend_cfg.get('cameras_token')
            data = CameraManager.fetch_cameras_from_backend(
                backend_cameras_url,
                timeout=float(backend_cfg.get('cameras_timeout', 10)),
                token=token,
            )
            if data:
                self.camera_manager.load_cameras_from_data(data)
                self.camera_manager.config_file = cameras_config
                if self.camera_manager.save_cameras():
                    print(f"[Main Agent] Cameras fetched from backend and saved to {cameras_config}")
                else:
                    print(f"[Main Agent] Cameras loaded from backend: {backend_cameras_url}")
            else:
                print(f"[Main Agent] Backend cameras failed, falling back to file: {cameras_config}")
                fallback_path = cameras_config
                if cameras_config and os.path.isdir(cameras_config):
                    fallback_path = os.path.normpath(os.path.join(os.path.dirname(cameras_config), 'config', 'cameras.yaml'))
                if fallback_path and os.path.isfile(fallback_path):
                    self.camera_manager.config_file = fallback_path
                    self.camera_manager.load_cameras()
        
        kafka_config = self.config.get('kafka', {})
        self.agent_config = self.config.get('agent', {})
        rtsp_config = self.config.get('rtsp', {})
        # RTSP: use FFmpeg pipe (env overrides config)
        _env_ffmpeg = os.getenv("RTSP_USE_FFMPEG_PIPE", "").strip().lower() in ("1", "true", "yes")
        self.rtsp_use_ffmpeg_pipe = _env_ffmpeg if os.getenv("RTSP_USE_FFMPEG_PIPE") is not None else rtsp_config.get('use_ffmpeg_pipe', False)
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS') or kafka_config.get('bootstrap_servers', 'localhost:9092')
        bootstrap_servers = str(bootstrap_servers).replace('http://', '').replace('https://', '').rstrip('/')
        topic = os.getenv('KAFKA_TOPIC') or kafka_config.get('topic', 'snapshots')
        print(f"[Main Agent] Kafka: {bootstrap_servers}, topic={topic}")

        # MinIO (optional): env overrides config
        minio_config = dict(self.config.get('minio', {}))
        if os.getenv('MINIO_ENABLED') is not None:
            minio_config['enabled'] = os.getenv('MINIO_ENABLED', '').strip().lower() in ('1', 'true', 'yes')
        for key, env_key in (('endpoint', 'MINIO_ENDPOINT'), ('bucket', 'MINIO_BUCKET'),
                             ('access_key', 'MINIO_ACCESS_KEY'), ('secret_key', 'MINIO_SECRET_KEY')):
            if os.getenv(env_key):
                minio_config[key] = os.getenv(env_key)
        if os.getenv('MINIO_SECURE') is not None:
            minio_config['secure'] = os.getenv('MINIO_SECURE', '').strip().lower() in ('1', 'true', 'yes')

        # Kafka sender: publishes crops to topic (optionally upload to MinIO first)
        self.sender_agent = KafkaSenderAgent(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            jpeg_quality=kafka_config.get('jpeg_quality', 85),
            minio_config=minio_config if minio_config.get('enabled') else None,
        )
        
        # Detection: face (Haar) or person (cpu-person-detection binary)
        det_config = self.config.get('detection', {})
        self.detection_type = (os.getenv('DETECTION_TYPE') or det_config.get('type', 'person')).lower()
        self.person_conf = float(det_config.get('person_conf', 0.4))
        self.person_iou = float(det_config.get('person_iou', 0.5))
        self.person_model_path = os.getenv('PERSON_MODEL_PATH') or det_config.get('person_model_path')
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
        """Initialize all cameras from configuration"""
        # First try to load from CameraManager
        cameras = self.camera_manager.get_enabled_cameras()
        
        # If cameras not found in CameraManager, use old configuration
        if not cameras:
            cameras_config = self.config.get('cameras', [])
            
            # Don't create default camera - user must configure IP cameras
            if not cameras_config:
                print("[Main Agent] WARNING: No cameras. Create cameras.yaml from cameras.yaml.example")
                cameras_config = []
            
            # Convert old configuration to CameraInfo
            from camera_manager import CameraInfo
            cameras = []
            for cam_config in cameras_config:
                if cam_config.get('enabled', True):
                    source = cam_config.get('source', len(cameras))
                    cam_type = cam_config.get('type', 'usb')
                    
                    camera = CameraInfo(
                        camera_id=cam_config.get('camera_id', f"camera_{len(cameras)}"),
                        name=cam_config.get('name', cam_config.get('camera_id', '')),
                        source=source,
                        type=cam_type,
                        fps=cam_config.get('fps', 10.0),
                        width=cam_config.get('width', 640),
                        height=cam_config.get('height', 480),
                        enabled=True
                    )
                    cameras.append(camera)
        
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
            
            # Override FPS priority: 1) Camera-specific env var, 2) DEFAULT_FPS env, 3) config.yaml default_fps, 4) cameras.yaml
            fps = camera.fps
            env_fps_key = f"{camera.camera_id.upper()}_FPS"
            default_fps_key = "DEFAULT_FPS"
            
            # Check camera-specific environment variable first
            if os.getenv(env_fps_key):
                try:
                    fps = float(os.getenv(env_fps_key))
                    print(f"[Main Agent] FPS for {camera.camera_id} overridden from env {env_fps_key}: {fps}")
                except ValueError:
                    print(f"[Main Agent] Invalid FPS value in {env_fps_key}, using config: {camera.fps}")
            # Check DEFAULT_FPS environment variable
            elif os.getenv(default_fps_key):
                try:
                    fps = float(os.getenv(default_fps_key))
                    print(f"[Main Agent] Using default FPS from env {default_fps_key}: {fps}")
                except ValueError:
                    print(f"[Main Agent] Invalid FPS value in {default_fps_key}, using config: {camera.fps}")
            # Check config.yaml default_fps
            elif self.agent_config.get('default_fps') is not None:
                try:
                    fps = float(self.agent_config.get('default_fps'))
                    print(f"[Main Agent] Using default FPS from config.yaml: {fps}")
                except (ValueError, TypeError):
                    print(f"[Main Agent] Invalid FPS value in config.yaml, using cameras.yaml: {camera.fps}")
            
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

    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        config_path = self._resolve_path(config_path)
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            print(f"[Main Agent] Configuration loaded from {config_path}")
            return config
        except Exception as e:
            print(f"[Main Agent] Configuration load error: {e}")
            return {}
    
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
