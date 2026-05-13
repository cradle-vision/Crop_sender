"""
Snapshot Capture Agent
Captures frames from camera with fixed FPS using FFmpeg only.
"""

import subprocess
import time
import threading
from queue import Queue
from typing import Optional, Callable, Union

import numpy as np

_PIPELINE_FPS = 10.0


class SnapshotCaptureAgent:
    """Agent for capturing snapshots from camera"""

    def __init__(
        self,
        source: Union[int, str] = 0,
        fps: float = _PIPELINE_FPS,
        width: int = 640,
        height: int = 480,
        camera_id: str = "camera_0",
        camera_type: str = "rtsp",
    ):
        self.source = source
        self.fps = _PIPELINE_FPS
        self.width = width
        self.height = height
        self.camera_id = camera_id
        self.camera_type = camera_type
        self.frame_interval = 1.0 / self.fps

        self._input_for_ffmpeg: Optional[str] = None
        self.is_running = False
        self.capture_thread: Optional[threading.Thread] = None
        self.frame_queue: Queue = Queue(maxsize=30)
        self.callback: Optional[Callable] = None
        self._ffmpeg_proc: Optional[subprocess.Popen] = None
        if fps != _PIPELINE_FPS:
            print(
                f"[Capture Agent {self.camera_id}] Requested FPS {fps} ignored. "
                f"Using fixed pipeline FPS: {_PIPELINE_FPS}"
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
        print(f"[Capture Agent {self.camera_id}] Capture started with FPS: {self.fps}")

    def stop(self):
        self.is_running = False
        if self.capture_thread:
            self.capture_thread.join(timeout=2.0)
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

    def _capture_loop_ffmpeg(self):
        error_count = 0
        while self.is_running:
            try:
                w, h = self._probe_resolution()
                if w <= 0 or h <= 0:
                    w, h = self.width, self.height
                frame_size = w * h * 3
                cmd = self._ffmpeg_decode_prefix() + [
                    "-f", "rawvideo", "-pix_fmt", "bgr24",
                    "-r", str(int(_PIPELINE_FPS)), "-an", "-",
                ]
                self._ffmpeg_proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL
                )
                assert self._ffmpeg_proc.stdout is not None
                while self.is_running and self._ffmpeg_proc.poll() is None:
                    raw = self._ffmpeg_proc.stdout.read(frame_size)
                    if len(raw) != frame_size:
                        break
                    current_time = time.time()
                    frame = np.frombuffer(raw, dtype=np.uint8).reshape((h, w, 3))
                    if self.callback:
                        try:
                            # Avoid per-frame copy; downstream can copy if needed.
                            self.callback(frame, current_time)
                        except Exception as e:
                            print(f"[Capture Agent {self.camera_id}] Callback error: {e}")
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

    def get_frame(self, timeout: float = 1.0) -> Optional[tuple]:
        try:
            return self.frame_queue.get(timeout=timeout)
        except Exception:
            return None

    def update_fps(self, new_fps: float):
        self.fps = _PIPELINE_FPS
        self.frame_interval = 1.0 / self.fps
        print(
            f"[Capture Agent {self.camera_id}] FPS update request ({new_fps}) ignored. "
            f"Fixed FPS: {self.fps}"
        )
