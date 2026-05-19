"""
Snapshot Capture Agent
Captures frames from camera via FFmpeg; samples with the fps filter (not -r on raw output).
Capture thread reads as fast as FFmpeg emits; a worker thread runs the callback so the pipe
does not stall.

Processing queue: bounded backlog between capture and detection. Larger maxsize tolerates
bursts and slow inference (fewer dropped frames) at the cost of higher latency and RAM; when
full, oldest pending frames are dropped so capture never blocks indefinitely.
"""

import os
import select
import subprocess
import time
import threading
from queue import Queue, Full, Empty
from typing import Callable, Optional, Tuple, Union

import numpy as np

_PROCESS_SENTINEL = object()

_DEFAULT_CAPTURE_FPS = 5.0
_MAX_PROCESSING_QUEUE = 500
_DEFAULT_PROCESSING_QUEUE_MAX = 100
_DEFAULT_RTSP_TIMEOUT_US = 5_000_000  # 5s socket I/O timeout (FFmpeg -timeout, microseconds)
_DEFAULT_RECONNECT_SEC = 3.0
_DEFAULT_FFPROBE_TIMEOUT_SEC = 8.0
_OFFLINE_LOG_INTERVAL_SEC = 60.0

# Cached RTSP socket timeout CLI flag: "-timeout" (most builds) or "-stimeout" (some newer).
_rtsp_timeout_cli_flag: Optional[str] = None
_rtsp_timeout_flag_logged = False


def _env_float(key: str, default: float) -> float:
    v = os.getenv(key)
    if v is None or not str(v).strip():
        return default
    try:
        return float(v)
    except ValueError:
        return default


def _env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or not str(v).strip():
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _rtsp_timeout_us_from_env() -> int:
    """CAPTURE_RTSP_TIMEOUT_US or legacy CAPTURE_RTSP_STIMEOUT_US."""
    if os.getenv("CAPTURE_RTSP_TIMEOUT_US"):
        return _env_int("CAPTURE_RTSP_TIMEOUT_US", _DEFAULT_RTSP_TIMEOUT_US)
    return _env_int("CAPTURE_RTSP_STIMEOUT_US", _DEFAULT_RTSP_TIMEOUT_US)


def _detect_rtsp_timeout_cli_flag() -> Optional[str]:
    """Pick -timeout or -stimeout from ffmpeg RTSP demuxer help (Docker often lacks -stimeout)."""
    override = (os.getenv("CAPTURE_RTSP_TIMEOUT_FLAG") or "").strip().lower()
    if override in ("none", "off", "0"):
        return None
    if override in ("timeout", "-timeout"):
        return "-timeout"
    if override in ("stimeout", "-stimeout"):
        return "-stimeout"
    try:
        r = subprocess.run(
            ["ffmpeg", "-hide_banner", "-h", "demuxer=rtsp"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        text = (r.stdout or "") + (r.stderr or "")
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("-stimeout ") or stripped.startswith("-stimeout\t"):
                return "-stimeout"
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("-timeout ") and "microsecond" in stripped.lower():
                return "-timeout"
        if "-timeout" in text:
            return "-timeout"
    except Exception:
        pass
    return "-timeout"


def _rtsp_socket_timeout_args(timeout_us: int) -> list[str]:
    global _rtsp_timeout_cli_flag, _rtsp_timeout_flag_logged
    if _rtsp_timeout_cli_flag is None:
        _rtsp_timeout_cli_flag = _detect_rtsp_timeout_cli_flag()
        if not _rtsp_timeout_flag_logged:
            _rtsp_timeout_flag_logged = True
            if _rtsp_timeout_cli_flag:
                print(
                    f"[Capture] RTSP socket timeout: {_rtsp_timeout_cli_flag} "
                    f"{timeout_us} us (CAPTURE_RTSP_TIMEOUT_US)"
                )
            else:
                print(
                    "[Capture] RTSP socket timeout flag disabled; "
                    "rely on CAPTURE_FRAME_STALL_SEC watchdog only"
                )
    if not _rtsp_timeout_cli_flag:
        return []
    return [_rtsp_timeout_cli_flag, str(timeout_us)]


class SnapshotCaptureAgent:
    """Agent for capturing snapshots from camera."""

    def __init__(
        self,
        source: Union[int, str] = 0,
        fps: float = _DEFAULT_CAPTURE_FPS,
        width: int = 640,
        height: int = 480,
        camera_id: str = "camera_0",
        camera_type: str = "rtsp",
        processing_queue_max: int = _DEFAULT_PROCESSING_QUEUE_MAX,
    ):
        self.source = source
        self.fps = max(0.0, float(fps))
        self.width = width
        self.height = height
        self.camera_id = camera_id
        self.camera_type = camera_type
        self.frame_interval = (1.0 / self.fps) if self.fps > 0 else 0.0

        pq = int(processing_queue_max)
        self.processing_queue_max = min(_MAX_PROCESSING_QUEUE, max(1, pq))

        self._input_for_ffmpeg: Optional[str] = None
        self.is_running = False
        self.capture_thread: Optional[threading.Thread] = None
        self.processing_thread: Optional[threading.Thread] = None
        self.processing_queue: Queue = Queue(maxsize=self.processing_queue_max)
        self.callback: Optional[Callable] = None
        self._ffmpeg_proc: Optional[subprocess.Popen] = None
        self._rtsp_timeout_us = _rtsp_timeout_us_from_env()
        self._reconnect_sec = max(1.0, _env_float("CAPTURE_RECONNECT_SEC", _DEFAULT_RECONNECT_SEC))
        self._ffprobe_timeout_sec = max(2.0, _env_float("CAPTURE_FFPROBE_TIMEOUT_SEC", _DEFAULT_FFPROBE_TIMEOUT_SEC))
        stall = _env_float("CAPTURE_FRAME_STALL_SEC", 0.0)
        if stall > 0:
            self._frame_stall_sec = stall
        elif self.fps > 0:
            self._frame_stall_sec = max(20.0, 5.0 + 3.0 / self.fps)
        else:
            self._frame_stall_sec = 25.0
        self._stream_healthy = False
        self._last_offline_log = 0.0
        print(
            f"[Capture Agent {self.camera_id}] Processing queue depth: {self.processing_queue_max} "
            f"(higher = more backlog tolerance, more latency/RAM if detection is slow)"
        )

    def start(self, callback: Optional[Callable] = None):
        if self.is_running:
            return
        self.callback = callback
        ctype = self.camera_type

        if ctype == "rtsp":
            self._input_for_ffmpeg = str(self.source).strip()
        elif ctype == "http":
            self._input_for_ffmpeg = str(self.source).strip()
        elif ctype == "file":
            self._input_for_ffmpeg = str(self.source).strip()
        else:
            self._input_for_ffmpeg = str(self.source).strip()

        self.is_running = True
        self.capture_thread = threading.Thread(target=self._capture_loop_ffmpeg, daemon=True)
        self.capture_thread.start()
        if self.callback is not None:
            self.processing_thread = threading.Thread(target=self._processing_loop, daemon=True)
            self.processing_thread.start()
        if self.fps > 0:
            print(f"[Capture Agent {self.camera_id}] Capture started (FFmpeg fps filter: {self.fps})")
        else:
            print(f"[Capture Agent {self.camera_id}] Capture started (full decode rate; high CPU/bandwidth)")

    def stop(self):
        self.is_running = False
        if self.capture_thread:
            self.capture_thread.join(timeout=3.0)
        if self._ffmpeg_proc is not None:
            try:
                self._ffmpeg_proc.terminate()
                self._ffmpeg_proc.wait(timeout=2.0)
            except Exception:
                try:
                    self._ffmpeg_proc.kill()
                except Exception:
                    pass
            self._ffmpeg_proc = None
        if self.processing_thread is not None:
            while True:
                try:
                    self.processing_queue.put_nowait(_PROCESS_SENTINEL)
                    break
                except Full:
                    try:
                        self.processing_queue.get_nowait()
                    except Empty:
                        pass
            self.processing_thread.join(timeout=10.0)
            self.processing_thread = None
        print(f"[Capture Agent {self.camera_id}] Capture stopped")

    def _rtsp_input_opts(self) -> list[str]:
        return ["-rtsp_transport", "tcp"] + _rtsp_socket_timeout_args(self._rtsp_timeout_us)

    def _probe_resolution(self) -> tuple[int, int]:
        inp = self._input_for_ffmpeg or str(self.source)
        cmd = [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height", "-of", "csv=s=x:p=0",
        ]
        if self.camera_type == "rtsp":
            cmd += self._rtsp_input_opts() + [inp]
        else:
            cmd.append(inp)
        try:
            out = subprocess.run(
                cmd, capture_output=True, text=True, timeout=self._ffprobe_timeout_sec, check=False
            )
            if out.returncode != 0:
                return 0, 0
            text = (out.stdout or "").strip()
            if "x" not in text:
                return 0, 0
            w_s, h_s = text.split("x", 1)
            w, h = int(w_s), int(h_s)
            if w > 0 and h > 0:
                return w, h
        except Exception:
            return 0, 0
        return 0, 0

    def _ffmpeg_decode_prefix(self) -> list[str]:
        inp = self._input_for_ffmpeg or str(self.source)
        base = ["ffmpeg", "-y", "-nostdin", "-loglevel", "error"]
        if self.camera_type == "rtsp":
            return base + self._rtsp_input_opts() + ["-i", inp]
        if self.camera_type == "http":
            return base + ["-rw_timeout", "5000000", "-i", inp]
        if self.camera_type == "file":
            return base + ["-i", inp]
        return base + ["-i", inp]

    def _ffmpeg_output_suffix(self) -> list[str]:
        """Output raw BGR24. Use fps filter for steady sampling; avoid -r on rawvideo (dup/drop)."""
        out: list[str] = []
        if self.fps > 0:
            out.extend(["-vf", f"fps={self.fps}"])
        out.extend(["-f", "rawvideo", "-pix_fmt", "bgr24", "-an", "-"])
        return out

    def _offer_processing(self, item: Tuple[float, np.ndarray]) -> None:
        """Enqueue for the worker; drop oldest pending frames if queue is full."""
        while True:
            try:
                self.processing_queue.put_nowait(item)
                return
            except Full:
                try:
                    self.processing_queue.get_nowait()
                except Empty:
                    pass

    def _processing_loop(self) -> None:
        while True:
            try:
                item = self.processing_queue.get(timeout=0.5)
            except Empty:
                continue
            if item is _PROCESS_SENTINEL:
                break
            ts, frame = item
            if self.callback is None:
                continue
            try:
                self.callback(frame, ts)
            except Exception as e:
                print(f"[Capture Agent {self.camera_id}] Callback error: {e}")

    def _read_exact_with_timeout(self, stream, size: int, timeout_sec: float) -> Optional[bytes]:
        """Read exactly `size` bytes or None on timeout/EOF (avoids blocking forever when camera is down)."""
        deadline = time.monotonic() + timeout_sec
        chunks: list[bytes] = []
        remaining = size
        while remaining > 0 and self.is_running:
            wait = deadline - time.monotonic()
            if wait <= 0:
                return None
            try:
                ready, _, _ = select.select([stream], [], [], min(wait, 1.0))
            except (ValueError, OSError):
                return None
            if not ready:
                continue
            chunk = stream.read(remaining)
            if not chunk:
                return None
            chunks.append(chunk)
            remaining -= len(chunk)
        if remaining > 0:
            return None
        return b"".join(chunks)

    def _stop_ffmpeg(self, reason: str = "") -> None:
        proc = self._ffmpeg_proc
        self._ffmpeg_proc = None
        if proc is None:
            return
        try:
            proc.terminate()
            proc.wait(timeout=2.0)
        except Exception:
            try:
                proc.kill()
                proc.wait(timeout=1.0)
            except Exception:
                pass
        if reason:
            print(f"[Capture Agent {self.camera_id}] {reason}")

    def _log_offline_periodic(self) -> None:
        now = time.time()
        if now - self._last_offline_log >= _OFFLINE_LOG_INTERVAL_SEC:
            self._last_offline_log = now
            print(
                f"[Capture Agent {self.camera_id}] Camera still offline; "
                f"retrying every {self._reconnect_sec:.0f}s"
            )

    def _mark_stream_lost(self, reason: str) -> None:
        if self._stream_healthy:
            print(f"[Capture Agent {self.camera_id}] Stream lost ({reason})")
        self._stream_healthy = False

    def _mark_stream_restored(self) -> None:
        if not self._stream_healthy:
            print(f"[Capture Agent {self.camera_id}] Stream restored — camera back online")
        self._stream_healthy = True
        self._last_offline_log = 0.0

    def _capture_loop_ffmpeg(self):
        while self.is_running:
            disconnect_reason = ""
            try:
                w, h = self._probe_resolution()
                if w <= 0 or h <= 0:
                    w, h = self.width, self.height
                    if not self._stream_healthy:
                        self._log_offline_periodic()
                frame_size = w * h * 3
                cmd = self._ffmpeg_decode_prefix() + self._ffmpeg_output_suffix()
                self._ffmpeg_proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=subprocess.DEVNULL,
                )
                assert self._ffmpeg_proc.stdout is not None
                got_frame = False
                while self.is_running and self._ffmpeg_proc.poll() is None:
                    raw = self._read_exact_with_timeout(
                        self._ffmpeg_proc.stdout, frame_size, self._frame_stall_sec
                    )
                    if raw is None:
                        disconnect_reason = (
                            "no frame within {:.0f}s (camera offline or stalled)".format(
                                self._frame_stall_sec
                            )
                        )
                        break
                    if len(raw) != frame_size:
                        disconnect_reason = "incomplete frame"
                        break
                    if not self.is_running:
                        break
                    got_frame = True
                    self._mark_stream_restored()
                    current_time = time.time()
                    if self.callback is not None:
                        frame = np.frombuffer(raw, dtype=np.uint8).reshape((h, w, 3)).copy()
                        self._offer_processing((current_time, frame))
                if self.is_running and not got_frame and not disconnect_reason:
                    code = self._ffmpeg_proc.poll() if self._ffmpeg_proc else None
                    disconnect_reason = f"ffmpeg exited (code={code})"
            except Exception as e:
                disconnect_reason = str(e)
                print(f"[Capture Agent {self.camera_id}] Capture error: {e}")
            finally:
                proc = self._ffmpeg_proc
                stderr_tail = ""
                if proc is not None and proc.stderr is not None:
                    try:
                        err_b = proc.stderr.read()
                        stderr_tail = (err_b or b"").decode(errors="replace").strip()[-400:]
                    except Exception:
                        pass
                self._stop_ffmpeg()
                if self.is_running and disconnect_reason:
                    self._mark_stream_lost(disconnect_reason)
                    if stderr_tail and not self._stream_healthy:
                        print(
                            f"[Capture Agent {self.camera_id}] FFmpeg: {stderr_tail}"
                        )
            if self.is_running:
                time.sleep(self._reconnect_sec)

    def update_fps(self, new_fps: float):
        self.fps = max(0.0, float(new_fps))
        self.frame_interval = (1.0 / self.fps) if self.fps > 0 else 0.0
        print(
            f"[Capture Agent {self.camera_id}] FPS set to {self.fps} "
            f"(applied on next FFmpeg reconnect after a stream error or restart)"
        )
