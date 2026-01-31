"""
Main Coordinator Agent
Camera → detect people (face or person) → crop → send crop to gRPC server.
"""

import yaml
import time
import signal
import sys
import os
from typing import Dict
from snapshot_capture_agent import SnapshotCaptureAgent
from grpc_sender_agent import GrpcSenderAgent
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
        
        # Camera manager: cameras from cameras.yaml (resolve path from project root if needed)
        cameras_config = cameras_config_path or self.config.get('cameras_config', 'cameras.yaml')
        cameras_config = self._resolve_path(cameras_config)
        self.camera_manager = CameraManager(config_file=cameras_config, auto_save=False)
        
        grpc_config = self.config.get('grpc', {})
        self.agent_config = self.config.get('agent', {})
        rtsp_config = self.config.get('rtsp', {})
        # RTSP: use FFmpeg pipe (env overrides config)
        _env_ffmpeg = os.getenv("RTSP_USE_FFMPEG_PIPE", "").strip().lower() in ("1", "true", "yes")
        self.rtsp_use_ffmpeg_pipe = _env_ffmpeg if os.getenv("RTSP_USE_FFMPEG_PIPE") is not None else rtsp_config.get('use_ffmpeg_pipe', False)
        server_address = os.getenv('GRPC_SERVER_ADDRESS') or grpc_config.get('server_address', 'localhost:50051')
        print(f"[Main Agent] gRPC server: {server_address}")
        
        # gRPC sender: sends cropped images to server
        self.sender_agent = GrpcSenderAgent(
            server_address=server_address,
            max_message_size=grpc_config.get('max_message_size', 4194304),
            timeout=self.agent_config.get('timeout', 5.0)
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
        
        # Connect to gRPC server
        if not self.sender_agent.connect():
            print("[Main Agent] Failed to connect to gRPC server")
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
        """Detect people (face or person), crop, send each crop to gRPC server."""
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
            if hasattr(self.sender_agent, 'service_unimplemented') and self.sender_agent.service_unimplemented:
                if not hasattr(self, '_service_error_logged'):
                    print("[Main Agent] ✗ Service not implemented. Stopping.")
                    self._service_error_logged = True
                return
    
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
