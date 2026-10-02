"""Full-frame NV12 from the GPU decoder. BGR is built only for the pixels that need it."""

from __future__ import annotations

import numpy as np

try:
    import cv2
except Exception:  # pragma: no cover
    cv2 = None


class Nv12Frame:
    """Original camera frame in NV12. width/height are the full image, not the 640 model input."""

    def __init__(self, data: np.ndarray, width: int, height: int):
        self.data = data
        self.width = int(width)
        self.height = int(height)

    @property
    def shape(self):
        return (self.height, self.width, 3)

    def copy(self) -> "Nv12Frame":
        return Nv12Frame(self.data.copy(), self.width, self.height)

    def to_bgr(self) -> np.ndarray:
        return nv12_rect_to_bgr(self.data, self.width, self.height, 0, 0, self.width, self.height)

    def region_bgr(self, x1: int, y1: int, x2: int, y2: int) -> np.ndarray:
        return nv12_rect_to_bgr(self.data, self.width, self.height, x1, y1, x2, y2)


def nv12_rect_to_bgr(
    data: np.ndarray,
    width: int,
    height: int,
    x1: int,
    y1: int,
    x2: int,
    y2: int,
) -> np.ndarray:
    """BGR of one rectangle from the original NV12 frame. Chroma is aligned, then trimmed back."""
    if cv2 is None:
        raise RuntimeError("OpenCV is required to convert NV12")
    x1 = max(0, min(int(x1), width))
    y1 = max(0, min(int(y1), height))
    x2 = max(x1, min(int(x2), width))
    y2 = max(y1, min(int(y2), height))
    if x2 <= x1 or y2 <= y1:
        return np.empty((0, 0, 3), dtype=np.uint8)
    x1e = x1 & ~1
    y1e = y1 & ~1
    x2e = min(width, (x2 + 1) & ~1)
    y2e = min(height, (y2 + 1) & ~1)
    if x2e <= x1e:
        x2e = min(width, x1e + 2)
    if y2e <= y1e:
        y2e = min(height, y1e + 2)
    y_plane = data[y1e:y2e, x1e:x2e]
    uv_plane = data[height + y1e // 2 : height + y2e // 2, x1e:x2e]
    packed = np.ascontiguousarray(np.vstack((y_plane, uv_plane)))
    bgr = cv2.cvtColor(packed, cv2.COLOR_YUV2BGR_NV12)
    return np.ascontiguousarray(bgr[y1 - y1e : y1 - y1e + (y2 - y1), x1 - x1e : x1 - x1e + (x2 - x1)])
