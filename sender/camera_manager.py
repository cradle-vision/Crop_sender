import yaml
import os
import json
import ssl
import subprocess
import urllib.request
import urllib.parse
from typing import List, Dict, Optional, Union
from dataclasses import dataclass, asdict, fields


@dataclass
class CameraInfo:
    """Camera information"""
    camera_id: str
    name: str
    source: Union[int, str]  # URL/path for stream source
    type: str  # 'rtsp', 'http', 'file'
    ip_address: Optional[str] = None
    mac_address: Optional[str] = None
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
    # Optional tripwire line config (from cameras.yaml/backend)
    line_x1: Optional[int] = None
    line_y1: Optional[int] = None
    line_x2: Optional[int] = None
    line_y2: Optional[int] = None
    inside_x: Optional[int] = None
    inside_y: Optional[int] = None
    line_active: Optional[bool] = None
    roi_updated_at: Optional[str] = None
    # Exclude zones (pixel rects); applied when exclude_zones_active is not False
    exclude_zones: Optional[List] = None
    exclude_zones_active: Optional[bool] = None


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
        previous_macs = {
            cid: getattr(cam, "mac_address", None)
            for cid, cam in self.cameras.items()
            if getattr(cam, "mac_address", None)
        }
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
        def _parse_bool(value):
            if isinstance(value, bool):
                return value
            if value is None:
                return None
            if isinstance(value, (int, float)):
                return bool(value)
            if isinstance(value, str):
                s = value.strip().lower()
                if s in ("1", "true", "yes", "y", "on"):
                    return True
                if s in ("0", "false", "no", "n", "off"):
                    return False
            return None

        def _to_int(value):
            if value is None:
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        def _pick(src: dict, keys):
            for k in keys:
                if k in src and src.get(k) is not None:
                    return src.get(k)
            return None

        def _extract_line_config(src: dict) -> dict:
            line = {}
            line["line_x1"] = _to_int(_pick(src, ("line_x1", "lineX1", "x1", "start_x")))
            line["line_y1"] = _to_int(_pick(src, ("line_y1", "lineY1", "y1", "start_y")))
            line["line_x2"] = _to_int(_pick(src, ("line_x2", "lineX2", "x2", "end_x")))
            line["line_y2"] = _to_int(_pick(src, ("line_y2", "lineY2", "y2", "end_y")))
            line["inside_x"] = _to_int(_pick(src, ("inside_x", "insideX", "inside_point_x", "insidePointX", "ix")))
            line["inside_y"] = _to_int(_pick(src, ("inside_y", "insideY", "inside_point_y", "insidePointY", "iy")))
            line["line_active"] = _parse_bool(_pick(src, ("line_active", "lineActive", "is_active", "isActive", "active", "enabled")))
            return line

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
            # Optional line filter config:
            # - preferred: camera_line or line
            # - backend DB compatibility: camera_roi may also contain line_* fields
            top_line = _extract_line_config(out)
            line_obj = out.get('camera_line') or out.get('line') or out.get('camera_roi')
            nested_line = _extract_line_config(line_obj) if isinstance(line_obj, dict) else {}
            if isinstance(line_obj, dict) and line_obj.get('updated_at'):
                out['roi_updated_at'] = line_obj.get('updated_at')
            merged_line = {}
            for key in ("line_x1", "line_y1", "line_x2", "line_y2", "inside_x", "inside_y", "line_active"):
                merged_line[key] = top_line.get(key) if top_line.get(key) is not None else nested_line.get(key)
            if None not in (
                merged_line.get("line_x1"),
                merged_line.get("line_y1"),
                merged_line.get("line_x2"),
                merged_line.get("line_y2"),
                merged_line.get("inside_x"),
                merged_line.get("inside_y"),
            ):
                out.setdefault("line_x1", merged_line["line_x1"])
                out.setdefault("line_y1", merged_line["line_y1"])
                out.setdefault("line_x2", merged_line["line_x2"])
                out.setdefault("line_y2", merged_line["line_y2"])
                out.setdefault("inside_x", merged_line["inside_x"])
                out.setdefault("inside_y", merged_line["inside_y"])
            if "line_active" not in out and merged_line.get("line_active") is not None:
                    out["line_active"] = merged_line["line_active"]
            # Exclude zones from camera_roi / top-level (bootstrap from cameras API)
            roi_obj = out.get("camera_roi") if isinstance(out.get("camera_roi"), dict) else {}
            ez = out.get("exclude_zones")
            if ez is None and isinstance(roi_obj, dict):
                ez = roi_obj.get("exclude_zones")
            if ez is not None:
                try:
                    from person_crop import parse_exclude_zones
                except ImportError:
                    from sender.person_crop import parse_exclude_zones  # type: ignore
                out["exclude_zones"] = [list(z) for z in parse_exclude_zones(ez)]
            eza = out.get("exclude_zones_active")
            if eza is None and isinstance(roi_obj, dict):
                eza = roi_obj.get("exclude_zones_active")
            if eza is not None:
                parsed = _parse_bool(eza)
                if parsed is not None:
                    out["exclude_zones_active"] = parsed
            # camera_id: БЕРЁМ ИМЕННО id из backend (основной ключ камеры),
            # а device_id используем только как fallback, если id нет.
            if 'id' in out:
                out['camera_id'] = str(out['id'])
            elif 'device_id' in out and 'camera_id' not in out:
                out['camera_id'] = str(out['device_id'])
            # ip_address: backend uses device_ip
            if 'device_ip' in out and out.get('device_ip') and not out.get('ip_address'):
                out['ip_address'] = str(out['device_ip']).strip()
            mac_raw = _pick(out, (
                "mac_address", "macAddress", "device_mac", "deviceMac", "mac", "mac_addr",
            ))
            if mac_raw:
                try:
                    from camera_net import normalize_mac
                    out["mac_address"] = normalize_mac(str(mac_raw))
                except Exception:
                    out["mac_address"] = str(mac_raw).strip() or None
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

            source_lower = str(out.get('source') or '').strip().lower()
            if source_lower.startswith('rtsp://') or source_lower.startswith('rtsps://'):
                out['type'] = 'rtsp'
            elif source_lower.startswith('http://') or source_lower.startswith('https://'):
                out['type'] = 'http'
            elif source_lower:
                out['type'] = 'file'
            else:
                out['type'] = 'rtsp'
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
            if not filtered.get("mac_address"):
                filtered["mac_address"] = previous_macs.get(str(filtered.get("camera_id")))
            try:
                camera = CameraInfo(**filtered)
                self.cameras[camera.camera_id] = camera
                print(f"[Camera Manager] Mapped backend camera: id={camera.camera_id}, name={camera.name}, ip={camera.ip_address}, mac={getattr(camera, 'mac_address', None) or '-'}")
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
    
    def reload(self) -> None:
        """Reload cameras from config file (or leave to MainAgent for backend fetch)."""
        if self.config_file and os.path.isfile(self.config_file):
            self.load_cameras()
        else:
            raise RuntimeError("No cameras config file to reload")

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

    def apply_discovered_ip(self, camera_id: str, new_ip: str, mac_address: Optional[str] = None) -> bool:
        """Rewrite local RTSP/HTTP source after MAC-based IP rediscovery."""
        camera = self.cameras.get(camera_id)
        if not camera:
            return False
        new_ip = str(new_ip or "").strip()
        if not new_ip:
            return False
        try:
            from camera_net import normalize_mac, replace_host_in_url
        except ImportError:
            from sender.camera_net import normalize_mac, replace_host_in_url  # type: ignore
        old_ip = camera.ip_address
        camera.ip_address = new_ip
        if isinstance(camera.source, str) and camera.source:
            camera.source = replace_host_in_url(camera.source, old_ip, new_ip)
        else:
            camera.source = self._build_source_url(camera)
        if mac_address:
            camera.mac_address = normalize_mac(mac_address) or camera.mac_address
        if self.auto_save:
            self.save_cameras()
        print(
            f"[Camera Manager] Camera {camera_id} IP updated {old_ip} -> {new_ip} "
            f"mac={camera.mac_address or '-'}"
        )
        return True

    def apply_roi_sync(
        self,
        camera_id: str,
        *,
        camera_line: Optional[dict] = None,
        camera_roi: Optional[dict] = None,
        exclude_zones=None,
        exclude_zones_active=None,
        persist: bool = False,
    ) -> bool:
        """Apply ROI/line/exclude-zone changes from backend (overwrites current values)."""
        camera = self.cameras.get(camera_id)
        if not camera:
            return False

        line_obj: dict = {}
        if isinstance(camera_line, dict):
            line_obj.update(camera_line)
        if isinstance(camera_roi, dict):
            for key in (
                "line_x1", "line_y1", "line_x2", "line_y2",
                "inside_x", "inside_y", "line_active", "updated_at",
            ):
                if key in camera_roi and camera_roi.get(key) is not None:
                    line_obj[key] = camera_roi[key]

        updates = {}
        for key in ("line_x1", "line_y1", "line_x2", "line_y2", "inside_x", "inside_y", "line_active"):
            if key in line_obj:
                updates[key] = line_obj[key]
        if "updated_at" in line_obj and line_obj.get("updated_at"):
            updates["roi_updated_at"] = str(line_obj["updated_at"])

        # Prefer top-level exclude fields; fallback to camera_roi
        ez = exclude_zones
        eza = exclude_zones_active
        if ez is None and isinstance(camera_roi, dict) and "exclude_zones" in camera_roi:
            ez = camera_roi.get("exclude_zones")
        if eza is None and isinstance(camera_roi, dict) and "exclude_zones_active" in camera_roi:
            eza = camera_roi.get("exclude_zones_active")

        if ez is not None:
            try:
                from person_crop import parse_exclude_zones
            except ImportError:
                from sender.person_crop import parse_exclude_zones  # type: ignore
            updates["exclude_zones"] = [list(z) for z in parse_exclude_zones(ez)]
        if eza is not None:
            if isinstance(eza, bool):
                updates["exclude_zones_active"] = eza
            elif isinstance(eza, str):
                s = eza.strip().lower()
                if s in ("1", "true", "yes", "y", "on"):
                    updates["exclude_zones_active"] = True
                elif s in ("0", "false", "no", "n", "off"):
                    updates["exclude_zones_active"] = False
            else:
                updates["exclude_zones_active"] = bool(eza)

        if not updates:
            return False

        for key, value in updates.items():
            setattr(camera, key, value)
        if persist or self.auto_save:
            self.save_cameras()
        print(f"[Camera Manager] ROI/line sync for camera {camera_id}: {list(updates.keys())}")
        return True
    
    def scan_usb_cameras(self, max_index: int = 10) -> List[int]:
        """USB cameras are not supported in this project."""
        print("[Camera Manager] USB cameras are disabled")
        return []
    
    def test_camera(self, camera: CameraInfo) -> bool:
        """
        Test camera connection
        
        Args:
            camera: Camera information
            
        Returns:
            True if camera is available
        """
        source = self._build_source_url(camera)
        timeout = 20.0
        cmd = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error"]
        try:
            if camera.type == "rtsp":
                cmd += ["-rtsp_transport", "tcp", "-i", str(source), "-frames:v", "1", "-f", "null", "-"]
            else:
                cmd += ["-i", str(source), "-frames:v", "1", "-f", "null", "-"]
            r = subprocess.run(cmd, capture_output=True, timeout=timeout, check=False)
            if r.returncode == 0:
                print(f"[Camera Manager] ✓ Camera {camera.camera_id} available")
                return True
            err = (r.stderr or b"").decode(errors="replace")[:300]
            print(f"[Camera Manager] ✗ Camera {camera.camera_id} not responding: {err or r.returncode}")
            return False
        except subprocess.TimeoutExpired:
            print(f"[Camera Manager] ✗ Camera {camera.camera_id} test timed out")
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
