"""
Person detection via CPU person detection binary.
Prefers bin/detect_main + LD_LIBRARY_PATH (no bash wrapper) to avoid fork storms:
the shell script uses process substitution 2> >(grep ...) and exhausts PID limits under load.
"""

import os
import re
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np

# Paths relative to project root (parent of sender/)
def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _resolve_detect_executable() -> Tuple[Optional[Path], Optional[Path]]:
    """Returns (detect_main_executable, lib_dir_for_ld_library_path)."""
    root = _project_root()
    pkg = root / "cpu-person-detection" / "person_detection_linux_x64"
    direct = pkg / "bin" / "detect_main"
    if direct.is_file():
        lib = pkg / "lib"
        return direct, lib if lib.is_dir() else None
    return None, None


def _cwd_for_executable(exe: Path) -> str:
    return str(exe.parent.parent)


def _env_with_bundled_lib(lib_dir: Optional[Path]) -> dict:
    env = os.environ.copy()
    if lib_dir is not None:
        lp = str(lib_dir)
        old = env.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = f"{lp}{os.pathsep}{old}" if old else lp
    return env


_STDERR_FILTER = re.compile(
    r"device_discovery|GPU device discovery failed|ReadFileContents Failed to open file"
)


def _filter_stderr(s: str) -> str:
    if not s:
        return ""
    lines = [ln for ln in s.splitlines() if not _STDERR_FILTER.search(ln)]
    return "\n".join(lines)


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
    line_params: Optional[Tuple[int, int, int, int, int, int]] = None,
) -> List[Tuple[int, int, int, int]]:
    """
    Detect persons using detect_main.
    Sends raw BGR frame through stdin to detect_main, parses stdout.
    Returns list of (x1, y1, x2, y2).
    """
    binary, lib_dir = _resolve_detect_executable()
    if not binary:
        return []
    model = Path(model_path or os.environ.get("PERSON_MODEL_PATH") or _find_model_path())
    if not model.exists():
        return []
    h, w = frame.shape[:2]
    if w == 0 or h == 0:
        return []
    frame_input = np.ascontiguousarray(frame, dtype=np.uint8)
    cmd = [
        str(binary),
        str(model),
        "--stdin-bgr",
        "--width",
        str(w),
        "--height",
        str(h),
    ]
    if line_params is not None:
        x1, y1, x2, y2, ix, iy = line_params
        cmd.extend(
            [
                "--line",
                str(int(x1)),
                str(int(y1)),
                str(int(x2)),
                str(int(y2)),
                "--inside_point",
                str(int(ix)),
                str(int(iy)),
            ]
        )
    cmd.extend([str(conf_threshold), str(iou_threshold)])

    out = subprocess.run(
        cmd,
        input=frame_input.tobytes(),
        capture_output=True,
        text=False,
        timeout=30,
        cwd=_cwd_for_executable(binary),
        env=_env_with_bundled_lib(lib_dir),
    )
    if out.returncode != 0:
        stderr_text = (out.stderr or b"").decode("utf-8", errors="replace")
        stdout_text = (out.stdout or b"").decode("utf-8", errors="replace")
        err = _filter_stderr((stderr_text or stdout_text).strip())
        if err:
            print(f"[PersonDetector] person_detect failed (code {out.returncode}): {err[:500]}")
        return []

    stdout_text = (out.stdout or b"").decode("utf-8", errors="replace")
    rects = []
    for m in _BBOX_PATTERN.finditer(stdout_text):
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
    """Check if detect binary and model exist."""
    exe, _ = _resolve_detect_executable()
    return exe is not None and _find_model_path().exists()
