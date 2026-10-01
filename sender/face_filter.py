"""
YuNet face gate for person crops (model copied from backend tools/yunet-prefilter).

Person crop -> YuNet on the native crop (no resize). No face -> drop.
Each face -> region FACE_CROP_SCALE x the face box, cut from the full frame (no resize).
"""

import os
import threading
import time
from pathlib import Path
from typing import List, Optional, Tuple

import cv2
import numpy as np

# Parallelism comes from PROCESSING_WORKERS; OpenCV's own threading oversubscribes on small crops.
cv2.setNumThreads(1)

Face = Tuple[int, int, int, int, float]
Rect = Tuple[int, int, int, int]


def _env_bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None or not v.strip():
        return default
    return v.strip().lower() in ("1", "true", "yes")


def _env_float(key: str, default: float) -> float:
    try:
        v = os.getenv(key)
        return float(v) if v else default
    except (ValueError, TypeError):
        return default


def _default_model_path() -> Path:
    return Path(__file__).resolve().parent.parent / "yunet" / "models" / "face_detection_yunet_2023mar.onnx"


FACE_FILTER_ENABLED = _env_bool("FACE_FILTER_ENABLED", True)
FACE_SCORE_THRESHOLD = _env_float("FACE_SCORE_THRESHOLD", 0.6)
FACE_NMS_THRESHOLD = _env_float("FACE_NMS_THRESHOLD", 0.3)
FACE_CROP_SCALE = max(1.0, _env_float("FACE_CROP_SCALE", 2.5))
FACE_STATS_INTERVAL_SEC = max(5.0, _env_float("FACE_STATS_INTERVAL_SEC", 60.0))
YUNET_MODEL_PATH = os.getenv("YUNET_MODEL_PATH") or str(_default_model_path())

_tls = threading.local()


def is_available() -> bool:
    return os.path.isfile(YUNET_MODEL_PATH)


def _detector(w: int, h: int) -> cv2.FaceDetectorYN:
    det: Optional[cv2.FaceDetectorYN] = getattr(_tls, "detector", None)
    if det is None:
        det = cv2.FaceDetectorYN.create(
            YUNET_MODEL_PATH,
            "",
            (w, h),
            FACE_SCORE_THRESHOLD,
            FACE_NMS_THRESHOLD,
            5000,
        )
        _tls.detector = det
    det.setInputSize((w, h))
    return det


def detect_faces(crop: np.ndarray) -> List[Face]:
    """Faces as (x, y, w, h, score) in crop coordinates, score >= FACE_SCORE_THRESHOLD."""
    h, w = crop.shape[:2]
    if w < 8 or h < 8:
        return []
    _, faces = _detector(w, h).detect(crop)
    if faces is None or len(faces) == 0:
        return []
    out: List[Face] = []
    for f in faces:
        score = float(f[14])
        if score < FACE_SCORE_THRESHOLD:
            continue
        fx, fy, fw, fh = int(f[0]), int(f[1]), int(f[2]), int(f[3])
        if fw <= 0 or fh <= 0:
            continue
        out.append((fx, fy, fw, fh, score))
    return out


def face_crops(
    frame: np.ndarray,
    person_rect: Rect,
    faces: List[Face],
    scale: float = FACE_CROP_SCALE,
) -> List[np.ndarray]:
    """Cut a scale x face-sized box around each face from the full frame, clamped to frame bounds."""
    fh_img, fw_img = frame.shape[:2]
    ox, oy = person_rect[0], person_rect[1]
    out: List[np.ndarray] = []
    for fx, fy, fw, fh, _ in faces:
        cx = ox + fx + fw / 2.0
        cy = oy + fy + fh / 2.0
        half_w = fw * scale / 2.0
        half_h = fh * scale / 2.0
        x1 = max(0, int(cx - half_w))
        y1 = max(0, int(cy - half_h))
        x2 = min(fw_img, int(cx + half_w))
        y2 = min(fh_img, int(cy + half_h))
        if x2 <= x1 or y2 <= y1:
            continue
        out.append(frame[y1:y2, x1:x2].copy())
    return out


class FaceFilterStats:
    """Thread-safe counters, printed every FACE_STATS_INTERVAL_SEC."""

    def __init__(self, interval_sec: float = FACE_STATS_INTERVAL_SEC):
        self._lock = threading.Lock()
        self._interval = interval_sec
        self._since = time.monotonic()
        self._reset()

    def _reset(self) -> None:
        self.persons = 0
        self.with_face = 0
        self.dropped = 0
        self.faces_sent = 0
        self.yunet_ms_total = 0.0

    def record(self, with_face: bool, faces_sent: int, yunet_ms: float) -> None:
        with self._lock:
            self.persons += 1
            if with_face:
                self.with_face += 1
            else:
                self.dropped += 1
            self.faces_sent += faces_sent
            self.yunet_ms_total += yunet_ms
            now = time.monotonic()
            if now - self._since < self._interval:
                return
            elapsed = now - self._since
            avg_ms = self.yunet_ms_total / self.persons if self.persons else 0.0
            drop_pct = 100.0 * self.dropped / self.persons if self.persons else 0.0
            print(
                f"[FaceFilter] last {elapsed:.0f}s: person_crops={self.persons} "
                f"with_face={self.with_face} dropped={self.dropped} ({drop_pct:.1f}%) "
                f"face_crops_sent={self.faces_sent} yunet_avg_ms={avg_ms:.1f}"
            )
            self._since = now
            self._reset()
