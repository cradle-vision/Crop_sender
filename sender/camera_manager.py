import cv2
import yaml
import os
import json
import ssl
import urllib.request
import urllib.parse
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
    fps: float = 3.0
    width: int = 1920
    height: int = 1080
    enabled: bool = True
    description: str = ""
    # Optional metadata for MinIO pathing and backend
    company_id: Optional[str] = None
    building_id: Optional[str] = None
    company_name: Optional[str] = None
    building_name: Optional[str] = None
    # Optional ROI rectangle from backend (camera_roi)
    roi_x: Optional[int] = None
    roi_y: Optional[int] = None
    roi_width: Optional[int] = None
    roi_height: Optional[int] = None
    roi_active: Optional[bool] = None


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
        """Load cameras from dict (same structure as YAML or API: { 'cameras': [ {...}, ... ] })."""
        self.cameras = {}
        cameras_list = data.get('cameras')
        if not isinstance(cameras_list, list):
            for key in ('items', 'data', 'results', 'content', 'records'):
                cameras_list = data.get(key)
                if isinstance(cameras_list, list):
                    break
            else:
                cameras_list = data if isinstance(data, list) else []
        valid_keys = {f.name for f in fields(CameraInfo)}
        # Map backend format (backend_cameras.json: device_id, device_ip, login, password, ddns_rtsp_url, companyId, buildingId, companyName, buildingName, ...) to cameras.yaml (camera_id, source, ip_address, username, password, company_id, building_id, company_name, building_name, ...)
        def normalize(c: dict) -> dict:
            out = dict(c)
            # camelCase -> snake_case
            for camel, snake in [('cameraId', 'camera_id'), ('streamUrl', 'stream_url'), ('rtspUrl', 'rtsp_url'),
                                 ('ipAddress', 'ip_address'), ('rtspPath', 'rtsp_path')]:
                if camel in out and snake not in out:
                    out[snake] = out[camel]
            # company/building ids/names from backend (companyId/buildingId -> company_id/building_id, companyName/buildingName -> company_name/building_name)
            for camel, snake in [('companyId', 'company_id'), ('buildingId', 'building_id'),
                                 ('companyName', 'company_name'), ('buildingName', 'building_name')]:
                if camel in out and snake not in out:
                    out[snake] = out[camel]
            # camera_roi: optional ROI rectangle
            roi = out.get('camera_roi')
            if isinstance(roi, dict):
                try:
                    rx = int(roi.get('roi_x')) if roi.get('roi_x') is not None else None
                    ry = int(roi.get('roi_y')) if roi.get('roi_y') is not None else None
                    rw = int(roi.get('roi_width')) if roi.get('roi_width') is not None else None
                    rh = int(roi.get('roi_height')) if roi.get('roi_height') is not None else None
                except (TypeError, ValueError):
                    rx = ry = rw = rh = None
                is_active = roi.get('is_active')
                if rx is not None and ry is not None and rw is not None and rh is not None and rw > 0 and rh > 0:
                    out.setdefault('roi_x', rx)
                    out.setdefault('roi_y', ry)
                    out.setdefault('roi_width', rw)
                    out.setdefault('roi_height', rh)
                    if 'roi_active' not in out and is_active is not None:
                        out['roi_active'] = bool(is_active)
            # camera_id: БЕРЁМ ИМЕННО id из backend (основной ключ камеры),
            # а device_id используем только как fallback, если id нет.
            if 'id' in out:
                out['camera_id'] = str(out['id'])
            elif 'device_id' in out and 'camera_id' not in out:
                out['camera_id'] = str(out['device_id'])
            # ip_address: backend uses device_ip
            if 'device_ip' in out and out.get('device_ip') and not out.get('ip_address'):
                out['ip_address'] = str(out['device_ip']).strip()
            # username / password: backend uses login / password
            if 'login' in out and out.get('login') and not out.get('username'):
                out['username'] = str(out['login']).strip()
            if 'password' in out:
                out['password'] = str(out['password']) if out['password'] is not None else ''
            # source: prefer full URL from backend (ddns_rtsp_url, ddns_stream_url), else build from device_ip + login + password
            if 'ddns_rtsp_url' in out and out.get('ddns_rtsp_url'):
                out['source'] = out['ddns_rtsp_url']
            elif 'ddns_stream_url' in out and out.get('ddns_stream_url'):
                out['source'] = out['ddns_stream_url']
            elif 'stream_url' in out and out.get('stream_url'):
                out['source'] = out['stream_url']
            elif 'streamUrl' in out and out.get('streamUrl'):
                out['source'] = out['streamUrl']
            elif 'url' in out and out.get('url'):
                out['source'] = out['url']
            elif 'rtsp_url' in out and out.get('rtsp_url'):
                out['source'] = out['rtsp_url']
            elif 'rtspUrl' in out and out.get('rtspUrl'):
                out['source'] = out['rtspUrl']
            else:
                # Build RTSP URL from device_ip (ip_address), login (username), password, port 554
                ip = out.get('ip_address') or out.get('device_ip')
                login = out.get('username') or out.get('login') or ''
                pwd = out.get('password') or ''
                port = out.get('port') or 554
                path = (out.get('rtsp_path') or '/').strip()
                if not path.startswith('/'):
                    path = '/' + path
                if ip:
                    if login and pwd:
                        out['source'] = f"rtsp://{login}:{pwd}@{ip}:{port}{path}"
                    else:
                        out['source'] = f"rtsp://{ip}:{port}{path}"
                    if 'port' not in out or out.get('port') is None:
                        out['port'] = port
                else:
                    out['source'] = ''
            # type
            if 'camera_type' in out and out.get('camera_type') and 'type' not in out:
                out['type'] = out['camera_type']
            elif 'cameraType' in out and 'type' not in out:
                out['type'] = out['cameraType']
            elif 'kind' in out and 'type' not in out:
                out['type'] = out['kind']
            if 'type' not in out:
                out['type'] = 'rtsp' if 'rtsp' in str(out.get('source', '')).lower() else 'http'
            # name
            if 'label' in out and out.get('label'):
                out['name'] = out['label']
            if 'device_name' in out and (not out.get('name') or out.get('name') == 'camera'):
                out['name'] = (out['device_name'] or out.get('name') or '').strip() or out.get('camera_id') or 'camera'
            if not out.get('name'):
                out['name'] = out.get('camera_id') or out.get('cameraId') or 'camera'
            return out
        for cam_data in cameras_list:
            if not isinstance(cam_data, dict):
                continue
            cam_data = normalize(cam_data)
            if 'camera_id' in cam_data:
                camera_id = str(cam_data['camera_id']).encode('utf-8', errors='ignore').decode('utf-8').strip()
                if not camera_id:
                    continue
                cam_data = {**cam_data, 'camera_id': camera_id}
            filtered = {k: v for k, v in cam_data.items() if k in valid_keys}
            if not filtered.get('camera_id'):
                continue
            # Allow camera with only id/name (source can be empty until backend provides ddns_rtsp_url/ddns_stream_url)
            if not filtered.get('source'):
                filtered['source'] = ''
            if not filtered.get('type'):
                filtered['type'] = 'rtsp'
            if not filtered.get('name'):
                filtered['name'] = filtered.get('camera_id') or 'camera'
            try:
                camera = CameraInfo(**filtered)
                self.cameras[camera.camera_id] = camera
                print(f"[Camera Manager] Mapped backend camera: id={camera.camera_id}, name={camera.name}, source={repr(camera.source)[:60]}")
            except Exception as e:
                print(f"[Camera Manager] Skip camera {cam_data.get('camera_id', '?')}: {e}")
        if len(self.cameras) == 0 and cameras_list:
            first = cameras_list[0] if cameras_list and isinstance(cameras_list[0], dict) else None
            print(f"[Camera Manager] Backend returned 0 cameras after mapping. Top-level keys: {list(data.keys())}")
            if first:
                print(f"[Camera Manager] First item keys: {list(first.keys())} (add mapping in camera_manager if needed)")
        elif len(self.cameras) == 0:
            print(f"[Camera Manager] Backend returned no camera list (empty or unknown structure). Keys: {list(data.keys())}")
        print(f"[Camera Manager] Loaded {len(self.cameras)} cameras from backend/data")
    
    @staticmethod
    def fetch_cameras_from_backend(url: str, timeout: float = 10.0, token: Optional[str] = None) -> Optional[dict]:
        """
        GET url, expect JSON { 'cameras': [ ... ] } or { 'items': [ ... ] }.
        Returns normalized dict for load_cameras_from_data; raw response is in the tuple from fetch_cameras_from_backend_raw.
        Optional Bearer token: Authorization: Bearer <token>.
        """
        raw = CameraManager._fetch_cameras_from_backend_raw(url, timeout, token)
        if raw is None:
            return None
        return CameraManager._normalize_backend_response(raw)

    @staticmethod
    def _fetch_cameras_from_backend_raw(url: str, timeout: float, token: Optional[str]) -> Optional[dict]:
        """GET and return raw JSON (for saving to backend_cameras.json)."""
        try:
            req = urllib.request.Request(url, method='GET')
            req.add_header('Accept', 'application/json')
            if token:
                t = str(token).strip()
                req.add_header('Authorization', f'Bearer {t}')
                print(f"[Camera Manager] Requesting cameras with Bearer token ({len(t)} chars)")
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                body = resp.read().decode('utf-8')
            return json.loads(body)
        except Exception as e:
            print(f"[Camera Manager] Backend fetch error: {e}")
            return None

    @staticmethod
    def fetch_backend_token(
        token_url: str,
        username: str,
        password: str,
        timeout: float = 10.0,
        scope: str = 'camera:read',
    ) -> Optional[str]:
        """
        POST token_url with application/x-www-form-urlencoded:
        grant_type=&username=...&password=...&scope=camera:read&client_id=&client_secret=
        Returns access_token from JSON response (OAuth2-style).
        """
        try:
            data = urllib.parse.urlencode({
                'grant_type': '',
                'username': username,
                'password': password,
                'scope': scope or '',
                'client_id': '',
                'client_secret': '',
            }).encode('utf-8')
            req = urllib.request.Request(token_url, data=data, method='POST')
            req.add_header('Accept', 'application/json')
            req.add_header('Content-Type', 'application/x-www-form-urlencoded')
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                body = resp.read().decode('utf-8')
            out = json.loads(body)
            token = None
            if isinstance(out, str):
                token = out.strip() or None
            elif isinstance(out, dict):
                token = out.get('access_token') or out.get('token')
                if token is None and isinstance(out.get('data'), dict):
                    token = out['data'].get('access_token') or out['data'].get('token')
            if token:
                token = str(token).strip()
                print(f"[Camera Manager] Backend token obtained via username/password")
            else:
                print(f"[Camera Manager] Token response missing access_token. Keys: {list(out.keys()) if isinstance(out, dict) else 'not dict'}")
            return token
        except Exception as e:
            print(f"[Camera Manager] Token fetch error: {e}")
            return None

    @staticmethod
    def _normalize_backend_response(data: dict) -> Optional[dict]:
        """Convert backend response to { 'cameras': [ ... ] } for load_cameras_from_data."""
        if not isinstance(data, dict):
            return {'cameras': data if isinstance(data, list) else []}
        keys = list(data.keys())
        if 'cameras' in data and isinstance(data['cameras'], list):
            print(f"[Camera Manager] Backend response keys: {keys}, using 'cameras' list len={len(data['cameras'])}")
            return data
        if 'items' in data and isinstance(data['items'], list):
            print(f"[Camera Manager] Backend response keys: {keys}, using 'items' list len={len(data['items'])}")
            return {'cameras': data['items']}
        for key in ('data', 'results', 'content', 'records'):
            if key in data and isinstance(data[key], list):
                print(f"[Camera Manager] Backend response keys: {keys}, using '{key}' list len={len(data[key])}")
                return {'cameras': data[key]}
        if 'data' in data and isinstance(data['data'], dict):
            inner = data['data']
            if 'items' in inner and isinstance(inner['items'], list):
                print(f"[Camera Manager] Backend response keys: {keys}, using 'data.items' list len={len(inner['items'])}")
                return {'cameras': inner['items']}
            if 'content' in inner and isinstance(inner['content'], list):
                print(f"[Camera Manager] Backend response keys: {keys}, using 'data.content' list len={len(inner['content'])}")
                return {'cameras': inner['content']}
        print(f"[Camera Manager] Backend response keys: {keys}, no known list found")
        return data
    
    def load_cameras(self):
        """Load cameras from config file. If path is a directory (e.g. Docker mount), use ../config/cameras.yaml."""
        if not self.config_file:
            self.cameras = {}
            return
        load_path = self.config_file
        if os.path.isdir(self.config_file):
            load_path = os.path.normpath(os.path.join(os.path.dirname(self.config_file), 'config', 'cameras.yaml'))
            if not os.path.isfile(load_path):
                print(f"[Camera Manager] Config path is a directory; expected file at {load_path} (create or fetch from backend)")
                self.cameras = {}
                return
        if not os.path.exists(load_path):
            print(f"[Camera Manager] Config file not found: {load_path}")
            self.cameras = {}
            return
        try:
            with open(load_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}
            cameras_list = data.get('cameras', [])
            for cam_data in cameras_list:
                if 'camera_id' in cam_data:
                    camera_id = str(cam_data['camera_id'])
                    camera_id = camera_id.encode('utf-8', errors='ignore').decode('utf-8')
                    if not camera_id:
                        continue
                    cam_data['camera_id'] = camera_id
                camera = CameraInfo(**cam_data)
                self.cameras[camera.camera_id] = camera
            print(f"[Camera Manager] Loaded {len(self.cameras)} cameras from {load_path}")
        except Exception as e:
            print(f"[Camera Manager] Config load error: {e}")
            self.cameras = {}
    
    def save_cameras(self):
        """Save cameras to config file. If path is a directory, save to ../config/cameras.yaml."""
        try:
            save_path = self.config_file
            if save_path and os.path.isdir(save_path):
                save_path = os.path.normpath(os.path.join(os.path.dirname(save_path), 'config', 'cameras.yaml'))
            if not save_path:
                return False
            cameras_list = [asdict(cam) for cam in self.cameras.values()]
            data = {'cameras': cameras_list}
            parent = os.path.dirname(save_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(save_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            
            print(f"[Camera Manager] Saved {len(self.cameras)} cameras to {save_path}")
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
