"""
Person detection via CPU person detection binary (person_detect).
Uses cpu-person-detection/person_detection_linux_x64/person_detect: frame → temp file → subprocess → parse bbox.
"""

import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import List, Tuple, Optional

import cv2
import numpy as np

# Paths relative to project root (parent of sender/)
def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _find_person_detect_binary() -> Optional[Path]:
    """person_detect in cpu-person-detection/person_detection_linux_x64/ or cpu-person-detection/"""
    root = _project_root()
    for rel in ["person_detection_linux_x64/person_detect", "person_detect"]:
        p = root / "cpu-person-detection" / rel
        if p.exists():
            return p
    return None


def _find_model_path() -> Path:
    return _project_root() / "cpu-person-detection" / "models" / "person_detection_model.onnx"


# stdout: bbox (x1,y1,x2,y2)=(412,156,465,298) score=0.711719
_BBOX_PATTERN = re.compile(
    r"bbox \(x1,y1,x2,y2\)=\((\d+),(\d+),(\d+),(\d+)\) score=([\d.e+-]+)"
)


def detect_persons(
    frame: np.ndarray,
    model_path: Optional[str] = None,
    conf_threshold: float = 0.4,
    iou_threshold: float = 0.5,
) -> List[Tuple[int, int, int, int]]:
    """
    Detect persons using person_detect binary.
    Writes frame to temp file, runs person_detect, parses stdout.
    Returns list of (x1, y1, x2, y2).
    """
    binary = _find_person_detect_binary()
    if not binary:
        return []
    model = Path(model_path or os.environ.get("PERSON_MODEL_PATH") or _find_model_path())
    if not model.exists():
        return []
    h, w = frame.shape[:2]
    if w == 0 or h == 0:
        return []
    fd, tmp_path = tempfile.mkstemp(suffix=".jpg")
    try:
        os.close(fd)
        if not cv2.imwrite(tmp_path, frame):
            return []
        cmd = [
            str(binary),
            str(model),
            tmp_path,
            str(conf_threshold),
            str(iou_threshold),
        ]
        out = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=str(binary.parent),
        )
        if out.returncode != 0:
            err = (out.stderr or out.stdout or "").strip()
            if err:
                print(f"[PersonDetector] person_detect failed (code {out.returncode}): {err[:500]}")
            return []
        rects = []
        for m in _BBOX_PATTERN.finditer(out.stdout):
            x1, y1, x2, y2 = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
            score = float(m.group(5))
            if score < conf_threshold:
                continue
            x1 = max(0, min(x1, w - 1))
            y1 = max(0, min(y1, h - 1))
            x2 = max(0, min(x2, w))
            y2 = max(0, min(y2, h))
            if x2 > x1 and y2 > y1:
                rects.append((x1, y1, x2, y2))
        return rects
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def crop_persons(
    frame: np.ndarray,
    rects: List[Tuple[int, int, int, int]],
    padding: float = 0.1,
) -> List[np.ndarray]:
    """
    Crop person regions from frame. rects: (x1, y1, x2, y2). padding: fraction of bbox.
    Returns list of BGR images.
    """
    crops = []
    h_img, w_img = frame.shape[:2]
    for (x1, y1, x2, y2) in rects:
        w, h = x2 - x1, y2 - y1
        pad_w = int(w * padding)
        pad_h = int(h * padding)
        x1p = max(0, x1 - pad_w)
        y1p = max(0, y1 - pad_h)
        x2p = min(w_img, x2 + pad_w)
        y2p = min(h_img, y2 + pad_h)
        crop = frame[y1p:y2p, x1p:x2p].copy()
        if crop.size > 0:
            crops.append(crop)
    return crops


def is_available() -> bool:
    """Check if person_detect binary and model exist."""
    return (
        _find_person_detect_binary() is not None
        and _find_model_path().exists()
    )
