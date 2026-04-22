from __future__ import annotations

from typing import Optional

import numpy as np

try:
    from turbojpeg import TJPF_BGR, TurboJPEG

    _TURBO = TurboJPEG()
except Exception:
    _TURBO = None


def encode_jpeg_bgr(frame: np.ndarray, quality: int = 100) -> Optional[bytes]:
    """Encode BGR frame to JPEG bytes with TurboJPEG only."""
    q = max(1, min(int(quality), 100))
    try:
        if _TURBO is None:
            raise RuntimeError("TurboJPEG unavailable")
        return _TURBO.encode(frame, quality=q, pixel_format=TJPF_BGR)
    except Exception:
        print("[JPEG] Encode failed")
        return None
