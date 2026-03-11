import argparse
import time
from pathlib import Path

import cv2
import yaml


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


if __name__ == "__main__":
    main()

