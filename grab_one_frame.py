import argparse
import os
import time
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import cv2
import yaml

try:
    import requests

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


def load_camera_from_yaml(cfg_path: str, camera_id: str | None = None) -> dict:
    with open(cfg_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    cameras = data.get("cameras") or []
    if not cameras:
        raise RuntimeError(f"No cameras in {cfg_path}")

    if camera_id:
        for cam in cameras:
            if str(cam.get("camera_id")) == str(camera_id):
                return cam
        raise RuntimeError(f"Camera with id={camera_id} not found in {cfg_path}")
    else:
        # first enabled camera, otherwise first one
        for cam in cameras:
            if cam.get("enabled", True):
                return cam
        return cameras[0]


def build_source_url(cam: dict) -> str:
    """
    Build RTSP/HTTP URL from camera config (similar to CameraManager._build_source_url).
    """
    src = cam.get("source")
    if isinstance(src, str) and "://" in src and src.strip():
        return src.strip()

    ip = cam.get("ip_address") or cam.get("device_ip")
    if not ip:
        raise RuntimeError("No ip_address/source in camera config")
    username = cam.get("username") or cam.get("login")
    password = cam.get("password")
    port = cam.get("port") or 554
    path = (cam.get("rtsp_path") or "/").strip()
    if not path.startswith("/"):
        path = "/" + path

    if username and password:
        return f"rtsp://{username}:{password}@{ip}:{port}{path}"
    else:
        return f"rtsp://{ip}:{port}{path}"


def _env(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return v.strip() if v else default


def _fetch_backend_token(token_url: str, username: str, password: str, timeout: float = 10.0) -> str | None:
    """Получить bearer-токен так же, как это делает backend (OAuth2-style)."""
    if not HAS_REQUESTS:
        return None
    try:
        data = {
            "grant_type": "",
            "username": username,
            "password": password,
            "scope": "camera:read",
            "client_id": "",
            "client_secret": "",
        }
        resp = requests.post(token_url, data=data, timeout=timeout)
        resp.raise_for_status()
        out = resp.json()
        if isinstance(out, str):
            token = out.strip() or None
        elif isinstance(out, dict):
            token = out.get("access_token") or out.get("token")
            if token is None and isinstance(out.get("data"), dict):
                token = out["data"].get("access_token") or out["data"].get("token")
        else:
            token = None
        return str(token).strip() if token else None
    except Exception:
        return None


def _backend_base_url_from_env() -> str | None:
    """Из BACKEND_CAMERAS_URL берём только scheme+host для snapshot-эндпоинта."""
    raw = _env("BACKEND_CAMERAS_URL")
    if not raw:
        return None
    try:
        parsed = urlparse(raw)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return None
    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Grab one original frame from camera configured in cameras.yaml"
    )
    parser.add_argument(
        "--config",
        default="config/cameras.yaml",
        help="Path to cameras.yaml (default: config/cameras.yaml)",
    )
    parser.add_argument(
        "--camera-id",
        help="camera_id from cameras.yaml; if not set, uses first enabled camera",
    )
    parser.add_argument(
        "--output",
        help="Output image path (default: frame_<camera_id>.jpg in current directory)",
    )
    args = parser.parse_args()

    cfg_path = Path(args.config)
    if not cfg_path.is_file():
        raise RuntimeError(f"Config file not found: {cfg_path}")

    cam_cfg = load_camera_from_yaml(str(cfg_path), args.camera_id)
    cam_id = str(cam_cfg.get("camera_id"))
    source = build_source_url(cam_cfg)

    print(f"[Grab] Using camera_id={cam_id}, source={source!r}")

    cap = cv2.VideoCapture(source, cv2.CAP_FFMPEG)
    if not cap.isOpened():
        raise RuntimeError(f"Failed to open video source: {source}")

    # warm-up a few frames
    for _ in range(5):
        ok, _ = cap.read()
        if not ok:
            time.sleep(0.1)

    ok, frame = cap.read()
    cap.release()
    if not ok or frame is None:
        raise RuntimeError("Failed to read frame from camera")

    out_path = Path(args.output) if args.output else Path(f"frame_{cam_id}.jpg")
    # save with max JPEG quality to see "as is"
    cv2.imwrite(str(out_path), frame, [int(cv2.IMWRITE_JPEG_QUALITY), 100])
    print(f"[Grab] Saved frame to {out_path}, shape={frame.shape}")

    company_id = cam_cfg.get("company_id")
    if not company_id:
        print("[Grab] company_id not set in cameras.yaml; skip backend snapshot upload")
        return
    base_url = _backend_base_url_from_env()
    if not base_url:
        print("[Grab] BACKEND_CAMERAS_URL is not set or invalid; skip backend snapshot upload")
        return
    if not HAS_REQUESTS:
        print("[Grab] Python package 'requests' not installed; skip backend snapshot upload")
        return

    company_id_str = str(company_id).strip()
    smartcamera_id_str = str(cam_id).strip()
    if not company_id_str or not smartcamera_id_str:
        print("[Grab] Empty company_id or camera_id; skip backend snapshot upload")
        return

    url = base_url.rstrip("/") + f"/company/{company_id_str}/smartcamera/{smartcamera_id_str}/snapshot"

    # Bearer-токен: BACKEND_CAMERAS_TOKEN или получаем через BACKEND_TOKEN_URL + BACKEND_CAMERAS_USERNAME/PASSWORD.
    token = _env("BACKEND_CAMERAS_TOKEN")
    if not token:
        username = _env("BACKEND_CAMERAS_USERNAME")
        password = _env("BACKEND_CAMERAS_PASSWORD")
        token_url = _env("BACKEND_TOKEN_URL")
        if username and password and token_url:
            token = _fetch_backend_token(token_url, username, password) or ""
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    ok_enc, buf = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 95])
    if not ok_enc:
        print("[Grab] Failed to encode frame as JPEG; skip backend snapshot upload")
        return
    data = buf.tobytes()
    files = {"file": ("snapshot.jpg", data, "image/jpeg")}
    try:
        print(f"[Grab] Sending snapshot to backend: {url}")
        resp = requests.post(url, headers=headers or None, files=files, timeout=10)
        print(f"[Grab] Backend response: {resp.status_code} {resp.text[:200]!r}")
    except Exception as e:
        print(f"[Grab] Error sending snapshot to backend: {e}")


if __name__ == "__main__":
    main()

