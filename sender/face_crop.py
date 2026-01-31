"""
Face detection and crop from snapshot.
Uses OpenCV Haar cascade (built-in).
"""

import cv2
import numpy as np
from typing import List, Tuple

# Default cascade path (opencv data)
CASCADE_PATH = getattr(cv2.data, "haarcascades", "") + "haarcascade_frontalface_default.xml"


def detect_faces(frame: np.ndarray, cascade_path: str = None, min_size: Tuple[int, int] = (30, 30)) -> List[Tuple[int, int, int, int]]:
    """
    Detect faces in BGR frame. Returns list of (x, y, w, h) rectangles.
    
    Args:
        frame: BGR image
        cascade_path: Path to Haar cascade XML (default: opencv frontalface)
        min_size: Minimum face size (w, h)
    
    Returns:
        List of (x, y, w, h)
    """
    path = cascade_path or CASCADE_PATH
    try:
        cascade = cv2.CascadeClassifier(path)
        if cascade.empty():
            return []
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        gray = cv2.equalizeHist(gray)
        faces = cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=min_size, flags=cv2.CASCADE_SCALE_IMAGE)
        return [(int(x), int(y), int(w), int(h)) for (x, y, w, h) in faces]
    except Exception:
        return []


def crop_faces(frame: np.ndarray, rects: List[Tuple[int, int, int, int]], padding: float = 0.2) -> List[np.ndarray]:
    """
    Crop face regions from frame. Optional padding (fraction of bbox).
    
    Args:
        frame: BGR image
        rects: List of (x, y, w, h)
        padding: Extra margin around face (0.2 = 20% each side)
    
    Returns:
        List of BGR cropped images
    """
    crops = []
    h_img, w_img = frame.shape[:2]
    for (x, y, w, h) in rects:
        pad_w = int(w * padding)
        pad_h = int(h * padding)
        x1 = max(0, x - pad_w)
        y1 = max(0, y - pad_h)
        x2 = min(w_img, x + w + pad_w)
        y2 = min(h_img, y + h + pad_h)
        crop = frame[y1:y2, x1:x2].copy()
        if crop.size > 0:
            crops.append(crop)
    return crops
