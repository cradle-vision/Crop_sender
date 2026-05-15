"""
Snapshot Capture Agent
Captures frames from camera via FFmpeg; samples with the fps filter (not -r on raw output).
Capture thread reads as fast as FFmpeg emits; a worker thread runs the callback so the pipe
does not stall.

Processing queue: bounded backlog between capture and detection. Larger maxsize tolerates
bursts and slow inference (fewer dropped frames) at the cost of higher latency and RAM; when
full, oldest pending frames are dropped so capture never blocks indefinitely.
"""

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

    def _probe_resolution(self) -> tuple[int, int]:
        inp = self._input_for_ffmpeg or str(self.source)
        cmd = [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height", "-of", "csv=s=x:p=0",
        ]
        if self.camera_type == "rtsp":
            cmd += ["-rtsp_transport", "tcp", inp]
        else:
            cmd.append(inp)
        try:
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=8, check=False)
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
            return base + ["-rtsp_transport", "tcp", "-i", inp]
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

    def _capture_loop_ffmpeg(self):
        error_count = 0
        while self.is_running:
            try:
                w, h = self._probe_resolution()
                if w <= 0 or h <= 0:
                    w, h = self.width, self.height
                frame_size = w * h * 3
                cmd = self._ffmpeg_decode_prefix() + self._ffmpeg_output_suffix()
                self._ffmpeg_proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL
                )
                assert self._ffmpeg_proc.stdout is not None
                while self.is_running and self._ffmpeg_proc.poll() is None:
                    raw = self._ffmpeg_proc.stdout.read(frame_size)
                    if len(raw) != frame_size:
                        break
                    if not self.is_running:
                        break
                    current_time = time.time()
                    if self.callback is not None:
                        frame = np.frombuffer(raw, dtype=np.uint8).reshape((h, w, 3)).copy()
                        self._offer_processing((current_time, frame))
                    error_count = 0
            except Exception as e:
                error_count += 1
                if error_count <= 3:
                    print(f"[Capture Agent {self.camera_id}] FFmpeg pipe error: {e}")
            finally:
                if self._ffmpeg_proc is not None:
                    try:
                        self._ffmpeg_proc.terminate()
                        self._ffmpeg_proc.wait(timeout=1.0)
                    except Exception:
                        pass
                    self._ffmpeg_proc = None
            if self.is_running:
                time.sleep(2.0)

    def update_fps(self, new_fps: float):
        self.fps = max(0.0, float(new_fps))
        self.frame_interval = (1.0 / self.fps) if self.fps > 0 else 0.0
        print(
            f"[Capture Agent {self.camera_id}] FPS set to {self.fps} "
            f"(applied on next FFmpeg reconnect after a stream error or restart)"
        )
