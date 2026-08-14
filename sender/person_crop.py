"""
Person detection via CPU person detection binary.
Prefers bin/detect_main + LD_LIBRARY_PATH (no bash wrapper) to avoid fork storms:
the shell script uses process substitution 2> >(grep ...) and exhausts PID limits under load.

Keeps a long-lived detect_main per worker thread so the ONNX session is loaded once.
"""

import atexit
import os
import re
import subprocess
import threading
import time
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np

# Paths relative to project root (parent of sender/)
def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _resolve_detect_executable() -> Tuple[Optional[Path], Optional[Path]]:
    """Returns (detect_main_executable, lib_dir_for_ld_library_path)."""
    root = _project_root()
    # Prefer local build first (usually newest binary with latest CLI options).
    build_bin = root / "cpu-person-detection" / "build" / "detect_main"
    if build_bin.is_file():
        return build_bin, None

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
_STDIN_MODE_CACHE: dict[str, bool] = {}
_STDIN_MODE_WARNED = False
_STDIN_MODE_ERROR: Optional[str] = None
_TIMEOUT_SEC = float(os.environ.get("PERSON_DETECT_TIMEOUT_SEC", "120"))
_FRAME_END = "__DETECT_END__"
_tls = threading.local()
_all_sessions: list["_PersistentDetector"] = []
_all_sessions_lock = threading.Lock()
_persistent_logged = False


def _supports_stdin_bgr(binary: Path, lib_dir: Optional[Path]) -> bool:
    global _STDIN_MODE_ERROR
    cache_key = str(binary.resolve())
    cached = _STDIN_MODE_CACHE.get(cache_key)
    if cached is not None:
        return cached
    try:
        out = subprocess.run(
            [str(binary)],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=_cwd_for_executable(binary),
            env=_env_with_bundled_lib(lib_dir),
        )
        text = f"{out.stdout}\n{out.stderr}"
        if "error while loading shared libraries" in text:
            _STDIN_MODE_ERROR = "shared_libs"
        elif "--stdin-bgr" not in text:
            _STDIN_MODE_ERROR = "unsupported"
        else:
            _STDIN_MODE_ERROR = None
        supported = "--stdin-bgr" in text
    except Exception:
        _STDIN_MODE_ERROR = "execution_failed"
        supported = False
    _STDIN_MODE_CACHE[cache_key] = supported
    return supported


def _parse_rects(stdout_text: str, w: int, h: int, conf_threshold: float) -> List[Tuple[int, int, int, int]]:
    rects: List[Tuple[int, int, int, int]] = []
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


def _detect_cmd(
    binary: Path,
    model: Path,
    w: int,
    h: int,
    line_params: Optional[Tuple[int, int, int, int, int, int]],
    conf_threshold: float,
    iou_threshold: float,
) -> list:
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
    return cmd


class _PersistentDetector:
    """One detect_main process: load ONNX once, infer many frames."""

    def __init__(
        self,
        binary: Path,
        lib_dir: Optional[Path],
        model: Path,
        w: int,
        h: int,
        line_params: Optional[Tuple[int, int, int, int, int, int]],
        conf_threshold: float,
        iou_threshold: float,
    ):
        self.key = (w, h, line_params, conf_threshold, iou_threshold)
        self.w = w
        self.h = h
        self.conf_threshold = conf_threshold
        cmd = _detect_cmd(binary, model, w, h, line_params, conf_threshold, iou_threshold)
        self.proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=_cwd_for_executable(binary),
            env=_env_with_bundled_lib(lib_dir),
            bufsize=0,
        )
        self._stderr_chunks: list[bytes] = []
        self._stderr_thread = threading.Thread(target=self._drain_stderr, daemon=True)
        self._stderr_thread.start()
        self._lock = threading.Lock()

    def _drain_stderr(self) -> None:
        try:
            assert self.proc.stderr is not None
            while True:
                block = self.proc.stderr.read(4096)
                if not block:
                    break
                self._stderr_chunks.append(block)
                if sum(len(c) for c in self._stderr_chunks) > 16000:
                    del self._stderr_chunks[:-4]
        except Exception:
            pass

    def _stderr_tail(self) -> str:
        return _filter_stderr(b"".join(self._stderr_chunks).decode("utf-8", errors="replace").strip())

    def alive(self) -> bool:
        return self.proc.poll() is None and self.proc.stdin is not None and self.proc.stdout is not None

    def close(self) -> None:
        proc = self.proc
        if proc.stdin:
            try:
                proc.stdin.close()
            except Exception:
                pass
        if proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=2.0)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    def detect(self, frame: np.ndarray) -> Optional[List[Tuple[int, int, int, int]]]:
        """Returns rects, or None if the process died / protocol failed (caller should respawn)."""
        if not self.alive():
            return None
        payload = np.ascontiguousarray(frame, dtype=np.uint8).tobytes()
        expected = self.w * self.h * 3
        if len(payload) != expected:
            return []
        with self._lock:
            try:
                assert self.proc.stdin is not None
                assert self.proc.stdout is not None
                self.proc.stdin.write(payload)
                self.proc.stdin.flush()
            except BrokenPipeError:
                return None
            deadline = time.monotonic() + _TIMEOUT_SEC
            lines: list[str] = []
            while time.monotonic() < deadline:
                if self.proc.poll() is not None:
                    err = self._stderr_tail()
                    if err:
                        print(f"[PersonDetector] detect_main exited: {err[:500]}")
                    return None
                line = self.proc.stdout.readline()
                if not line:
                    return None
                text = line.decode("utf-8", errors="replace").strip()
                if text == _FRAME_END:
                    return _parse_rects("\n".join(lines), self.w, self.h, self.conf_threshold)
                if text:
                    lines.append(text)
            print(
                f"[PersonDetector] person_detect timeout after {_TIMEOUT_SEC}s "
                f"for frame {self.w}x{self.h} (persistent worker)"
            )
            self.close()
            return None


def _close_all_sessions() -> None:
    with _all_sessions_lock:
        sessions = list(_all_sessions)
        _all_sessions.clear()
    for s in sessions:
        try:
            s.close()
        except Exception:
            pass


atexit.register(_close_all_sessions)


def _session_for(
    binary: Path,
    lib_dir: Optional[Path],
    model: Path,
    w: int,
    h: int,
    line_params: Optional[Tuple[int, int, int, int, int, int]],
    conf_threshold: float,
    iou_threshold: float,
) -> _PersistentDetector:
    key = (w, h, line_params, conf_threshold, iou_threshold)
    sess: Optional[_PersistentDetector] = getattr(_tls, "detector", None)
    if sess is not None and sess.key == key and sess.alive():
        return sess
    if sess is not None:
        sess.close()
        with _all_sessions_lock:
            if sess in _all_sessions:
                _all_sessions.remove(sess)
    sess = _PersistentDetector(
        binary, lib_dir, model, w, h, line_params, conf_threshold, iou_threshold
    )
    _tls.detector = sess
    with _all_sessions_lock:
        _all_sessions.append(sess)
    global _persistent_logged
    if not _persistent_logged:
        _persistent_logged = True
        print(
            "[PersonDetector] Persistent detect_main (ONNX loaded once per worker, "
            f"reuse stdin frames). First worker {w}x{h}"
        )
    return sess


def detect_persons(
    frame: np.ndarray,
    model_path: Optional[str] = None,
    conf_threshold: float = 0.4,
    iou_threshold: float = 0.5,
    line_params: Optional[Tuple[int, int, int, int, int, int]] = None,
) -> List[Tuple[int, int, int, int]]:
    """
    Detect persons using a long-lived detect_main (model loaded once per worker).
    Returns list of (x1, y1, x2, y2).
    """
    binary, lib_dir = _resolve_detect_executable()
    if not binary:
        return []
    if not _supports_stdin_bgr(binary, lib_dir):
        global _STDIN_MODE_WARNED
        if not _STDIN_MODE_WARNED:
            if _STDIN_MODE_ERROR == "shared_libs":
                print(
                    "[PersonDetector] detect_main failed to start: missing shared libs. "
                    "Check LD_LIBRARY_PATH and packaged libs in cpu-person-detection/person_detection_linux_x64/lib."
                )
            elif _STDIN_MODE_ERROR == "unsupported":
                print(
                    "[PersonDetector] detect_main does not support --stdin-bgr. "
                    "Please rebuild cpu-person-detection and update the runtime binary."
                )
            else:
                print(
                    "[PersonDetector] detect_main check failed. "
                    "Unable to verify --stdin-bgr support."
                )
            _STDIN_MODE_WARNED = True
        return []
    model = Path(model_path or os.environ.get("PERSON_MODEL_PATH") or _find_model_path())
    if not model.exists():
        return []
    h, w = frame.shape[:2]
    if w == 0 or h == 0:
        return []

    sess = _session_for(binary, lib_dir, model, w, h, line_params, conf_threshold, iou_threshold)
    rects = sess.detect(frame)
    if rects is not None:
        return rects
    sess.close()
    with _all_sessions_lock:
        if sess in _all_sessions:
            _all_sessions.remove(sess)
    _tls.detector = None
    sess = _session_for(binary, lib_dir, model, w, h, line_params, conf_threshold, iou_threshold)
    rects = sess.detect(frame)
    return rects if rects is not None else []


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
