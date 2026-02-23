"""
Camera Manager - camera configuration (cameras.yaml or backend API)
Supports IP cameras RTSP/HTTP and USB.
"""

import cv2
import yaml
import os
import json
import ssl
import urllib.request
from typing import List, Dict, Optional, Union
from dataclasses import dataclass, asdict, fields


@dataclass
class CameraInfo:
    """Camera information"""
    camera_id: str
    name: str
    source: Union[int, str]  # Index for USB or URL for IP
    type: str  # 'usb', 'rtsp', 'http', 'file'
    ip_address: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    rtsp_path: Optional[str] = None
    fps: float = 10.0
    width: int = 640
    height: int = 480
    enabled: bool = True
    description: str = ""


class CameraManager:
    """Camera manager"""
    
    def __init__(self, config_file: str = "cameras.yaml", auto_save: bool = True):
        """
        Initialize camera manager
        
        Args:
            config_file: Path to camera config file
            auto_save: Auto-save changes
        """
        self.config_file = config_file
        self.auto_save = auto_save
        self.cameras: Dict[str, CameraInfo] = {}
        if config_file and os.path.exists(config_file) and not os.path.isdir(config_file):
            self.load_cameras()
    
    def load_cameras_from_data(self, data: dict) -> None:
        """Load cameras from dict (same structure as YAML: { 'cameras': [ {...}, ... ] })."""
        self.cameras = {}
        cameras_list = data.get('cameras') if isinstance(data.get('cameras'), list) else (data if isinstance(data, list) else [])
        valid_keys = {f.name for f in fields(CameraInfo)}
        for cam_data in cameras_list:
            if not isinstance(cam_data, dict):
                continue
            if 'camera_id' in cam_data:
                camera_id = str(cam_data['camera_id']).encode('utf-8', errors='ignore').decode('utf-8').strip()
                if not camera_id:
                    continue
                cam_data = {**cam_data, 'camera_id': camera_id}
            filtered = {k: v for k, v in cam_data.items() if k in valid_keys}
            try:
                camera = CameraInfo(**filtered)
                self.cameras[camera.camera_id] = camera
            except Exception as e:
                print(f"[Camera Manager] Skip camera {cam_data.get('camera_id', '?')}: {e}")
        print(f"[Camera Manager] Loaded {len(self.cameras)} cameras from backend/data")
    
    @staticmethod
    def fetch_cameras_from_backend(url: str, timeout: float = 10.0) -> Optional[dict]:
        """
        GET url, expect JSON { 'cameras': [ { camera_id, name, source, type, ... } ] }.
        Returns dict or None on error.
        """
        try:
            req = urllib.request.Request(url, method='GET')
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                body = resp.read().decode('utf-8')
            data = json.loads(body)
            return data if isinstance(data, dict) else {'cameras': data}
        except Exception as e:
            print(f"[Camera Manager] Backend fetch error: {e}")
            return None
    
    def load_cameras(self):
        """Load cameras from config file"""
        if not self.config_file or not os.path.exists(self.config_file):
            if self.config_file:
                print(f"[Camera Manager] Config file not found: {self.config_file}")
                print(f"[Camera Manager] Create from example: cp cameras.yaml.example {self.config_file}")
            self.cameras = {}
            return
        if os.path.isdir(self.config_file):
            print(f"[Camera Manager] ERROR: {self.config_file} is a directory, not a file!")
            self.cameras = {}
            return
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}
            cameras_list = data.get('cameras', [])
            for cam_data in cameras_list:
                # Ensure camera_id is valid UTF-8
                if 'camera_id' in cam_data:
                    camera_id = str(cam_data['camera_id'])
                    camera_id = camera_id.encode('utf-8', errors='ignore').decode('utf-8')
                    if not camera_id:
                        continue
                    cam_data['camera_id'] = camera_id
                camera = CameraInfo(**cam_data)
                self.cameras[camera.camera_id] = camera
            print(f"[Camera Manager] Loaded {len(self.cameras)} cameras from {self.config_file}")
        except Exception as e:
            print(f"[Camera Manager] Config load error: {e}")
            self.cameras = {}
    
    def save_cameras(self):
        """Save cameras to config file"""
        try:
            cameras_list = [asdict(cam) for cam in self.cameras.values()]
            data = {'cameras': cameras_list}
            parent = os.path.dirname(self.config_file)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(self.config_file, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            
            print(f"[Camera Manager] Saved {len(self.cameras)} cameras to {self.config_file}")
            return True
        except Exception as e:
            print(f"[Camera Manager] Save error: {e}")
            return False
    
    def add_camera(self, camera: CameraInfo) -> bool:
        """
        Add camera
        
        Args:
            camera: Camera information
            
        Returns:
            True if successful
        """
        if camera.camera_id in self.cameras:
            print(f"[Camera Manager] Camera {camera.camera_id} already exists")
            return False
        
        self.cameras[camera.camera_id] = camera
        print(f"[Camera Manager] Added camera: {camera.camera_id} ({camera.name})")
        
        if self.auto_save:
            self.save_cameras()
        
        return True
    
    def remove_camera(self, camera_id: str) -> bool:
        """Remove camera"""
        if camera_id in self.cameras:
            del self.cameras[camera_id]
            print(f"[Camera Manager] Removed camera: {camera_id}")
            
            if self.auto_save:
                self.save_cameras()
            
            return True
        return False
    
    def get_camera(self, camera_id: str) -> Optional[CameraInfo]:
        """Get camera information"""
        return self.cameras.get(camera_id)
    
    def get_all_cameras(self) -> List[CameraInfo]:
        """Get list of all cameras"""
        return list(self.cameras.values())
    
    def get_enabled_cameras(self) -> List[CameraInfo]:
        """Get list of enabled cameras"""
        return [cam for cam in self.cameras.values() if cam.enabled]
    
    def update_camera(self, camera_id: str, **kwargs) -> bool:
        """Update camera parameters"""
        if camera_id not in self.cameras:
            return False
        
        camera = self.cameras[camera_id]
        for key, value in kwargs.items():
            if hasattr(camera, key):
                setattr(camera, key, value)
        
        if self.auto_save:
            self.save_cameras()
        
        return True
    
    def scan_usb_cameras(self, max_index: int = 10) -> List[int]:
        """
        Scan available USB cameras
        
        Args:
            max_index: Maximum index to check
            
        Returns:
            List of available camera indices
        """
        available = []
        print(f"[Camera Manager] Scanning USB cameras (0-{max_index-1})...")
        
        for i in range(max_index):
            cap = cv2.VideoCapture(i)
            if cap.isOpened():
                ret, _ = cap.read()
                if ret:
                    available.append(i)
                    print(f"  ✓ Found camera at index {i}")
                cap.release()
        
        print(f"[Camera Manager] Found {len(available)} USB cameras: {available}")
        return available
    
    # RTSP options for test_camera (same as snapshot_capture_agent to avoid RTP/decoding errors)
    _RTSP_FFMPEG_OPTS = "rtsp_transport=tcp|rtsp_flags=prefer_tcp|fflags=nobuffer|flags=low_delay|max_delay=5000000|stimeout=5000000"

    def test_camera(self, camera: CameraInfo) -> bool:
        """
        Test camera connection
        
        Args:
            camera: Camera information
            
        Returns:
            True if camera is available
        """
        source = self._build_source_url(camera)
        if camera.type == 'rtsp' and isinstance(source, str):
            if 'rtsp_transport=' not in source:
                source = source + ('&' if '?' in source else '?') + 'rtsp_transport=tcp'
        try:
            old_opts = os.environ.get("OPENCV_FFMPEG_CAPTURE_OPTIONS", "")
            if camera.type == 'rtsp':
                os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = self._RTSP_FFMPEG_OPTS
            try:
                cap = cv2.VideoCapture(source, cv2.CAP_FFMPEG if camera.type == 'rtsp' else 0)
            finally:
                os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = old_opts
            if not cap.isOpened():
                print(f"[Camera Manager] Failed to open camera {camera.camera_id}")
                return False
            
            ret, frame = cap.read()
            cap.release()

            if ret and frame is not None:
                print(f"[Camera Manager] ✓ Camera {camera.camera_id} available")
                return True
            else:
                print(f"[Camera Manager] ✗ Camera {camera.camera_id} not responding")
                return False
        except Exception as e:
            print(f"[Camera Manager] Camera test error {camera.camera_id}: {e}")
            return False

    def _build_source_url(self, camera: CameraInfo) -> Union[int, str]:
        """
        Build source URL for camera
        
        Args:
            camera: Camera information
            
        Returns:
            URL or source index
        """
        if camera.type == 'usb':
            return camera.source if isinstance(camera.source, int) else int(camera.source)
        if camera.type in ('rtsp', 'http') and isinstance(camera.source, str) and '://' in camera.source:
            return camera.source
        elif camera.type == 'rtsp':
            # RTSP URL format: rtsp://[username:password@]ip:port/path
            url = "rtsp://"
            if camera.username and camera.password:
                url += f"{camera.username}:{camera.password}@"
            url += f"{camera.ip_address}"
            if camera.port:
                url += f":{camera.port}"
            if camera.rtsp_path:
                url += camera.rtsp_path
            return url
        
        elif camera.type == 'http':
            # HTTP/MJPEG URL
            protocol = "http://"
            if camera.username and camera.password:
                url = f"{protocol}{camera.username}:{camera.password}@{camera.ip_address}"
            else:
                url = f"{protocol}{camera.ip_address}"
            if camera.port:
                url += f":{camera.port}"
            if camera.rtsp_path:
                url += camera.rtsp_path
            return url
        
        elif camera.type == 'file':
            return str(camera.source)
        
        return camera.source
    
    def create_rtsp_camera(self, camera_id: str, name: str, ip_address: str,
                          port: int = 554, rtsp_path: str = "/stream1",
                          username: str = None, password: str = None,
                          fps: float = 10.0, width: int = 640, height: int = 480) -> CameraInfo:
        """Create RTSP camera"""
        camera = CameraInfo(
            camera_id=camera_id,
            name=name,
            source="",  # Will be built automatically
            type="rtsp",
            ip_address=ip_address,
            port=port,
            username=username,
            password=password,
            rtsp_path=rtsp_path,
            fps=fps,
            width=width,
            height=height
        )
        camera.source = self._build_source_url(camera)
        return camera
    
    def create_http_camera(self, camera_id: str, name: str, ip_address: str,
                          port: int = 80, path: str = "/mjpeg",
                          username: str = None, password: str = None,
                          fps: float = 10.0, width: int = 640, height: int = 480) -> CameraInfo:
        """Create HTTP/MJPEG camera"""
        camera = CameraInfo(
            camera_id=camera_id,
            name=name,
            source="",  # Will be built automatically
            type="http",
            ip_address=ip_address,
            port=port,
            username=username,
            password=password,
            rtsp_path=path,
            fps=fps,
            width=width,
            height=height
        )
        camera.source = self._build_source_url(camera)
        return camera
    
    def create_usb_camera(self, camera_id: str, name: str, source_index: int,
                         fps: float = 10.0, width: int = 640, height: int = 480) -> CameraInfo:
        """Create USB camera"""
        return CameraInfo(
            camera_id=camera_id,
            name=name,
            source=source_index,
            type="usb",
            fps=fps,
            width=width,
            height=height
        )
    
    def create_file_camera(self, camera_id: str, name: str, file_path: str,
                          fps: float = 10.0, width: int = 640, height: int = 480) -> CameraInfo:
        """Create camera from video file"""
        return CameraInfo(
            camera_id=camera_id,
            name=name,
            source=file_path,
            type="file",
            fps=fps,
            width=width,
            height=height
        )
