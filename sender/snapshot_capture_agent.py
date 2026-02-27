"""
Snapshot Capture Agent
Captures frames from camera with specified FPS rate.
RTSP: optional FFmpeg pipe (env RTSP_USE_FFMPEG_PIPE=1) for stable TCP decode.
"""

import os
import subprocess
import cv2
import time
import threading
import numpy as np
from queue import Queue
from typing import Optional, Callable, Union

# FFmpeg options for RTSP via OpenCV
_FFMPEG_RTSP_OPTS = "rtsp_transport=tcp|rtsp_flags=prefer_tcp|fflags=nobuffer|flags=low_delay|max_delay=5000000|stimeout=5000000"


class SnapshotCaptureAgent:
    """Agent for capturing snapshots from camera"""
    
    def __init__(self, source: Union[int, str] = 0, fps: float = 10.0, 
                 width: int = 640, height: int = 480, camera_id: str = "camera_0",
                 camera_type: str = "usb", use_ffmpeg_pipe_rtsp: Optional[bool] = None):
        """
        Initialize capture agent
        
        Args:
            source: Video source (index for USB or URL for IP cameras)
            fps: Frames per second
            width: Frame width
            height: Frame height
            camera_id: Camera identifier
            camera_type: Camera type ('usb', 'rtsp', 'http', 'file')
            use_ffmpeg_pipe_rtsp: For RTSP, use FFmpeg subprocess (True/False); None = follow env RTSP_USE_FFMPEG_PIPE
        """
        self.source = source
        self.fps = fps
        self.width = width
        self.height = height
        self.camera_id = camera_id
        self.camera_type = camera_type
        self.use_ffmpeg_pipe_rtsp = use_ffmpeg_pipe_rtsp  # None = use env
        self.frame_interval = 1.0 / fps if fps > 0 else 0.1
        
        self.cap: Optional[cv2.VideoCapture] = None
        self._capture_url: Optional[str] = None  # URL used for RTSP (for reconnect)
        self.is_running = False
        self.capture_thread: Optional[threading.Thread] = None
        self.frame_queue: Queue = Queue(maxsize=30)
        self.callback: Optional[Callable] = None
        self._reconnect_after_errors = 50  # reconnect RTSP after this many consecutive read errors
        self._ffmpeg_proc: Optional[subprocess.Popen] = None  # for RTSP pipe mode

    def _use_ffmpeg_pipe_rtsp(self) -> bool:
        """Use FFmpeg subprocess for RTSP (explicit -rtsp_transport tcp). Config or env."""
        if self.use_ffmpeg_pipe_rtsp is not None:
            return self.use_ffmpeg_pipe_rtsp
        return os.environ.get("RTSP_USE_FFMPEG_PIPE", "").strip().lower() in ("1", "true", "yes")

    def start(self, callback: Optional[Callable] = None):
        """
        Start capturing snapshots
        
        Args:
            callback: Callback function for processing each frame
        """
        if self.is_running:
            return
            
        self.callback = callback
        
        # For IP cameras (RTSP/HTTP) use special settings
        if self.camera_type in ['rtsp', 'http']:
            url = str(self.source)
            if self.camera_type == 'rtsp':
                sep = '&' if '?' in url else '?'
                if 'rtsp_transport=' not in url:
                    url += f"{sep}rtsp_transport=tcp"
                if 'stimeout=' not in url:
                    url += f"&stimeout=5000000"
            self._capture_url = url
            # Optional: RTSP via FFmpeg pipe (explicit TCP, avoids OpenCV "RTP bad cseq")
            if self.camera_type == 'rtsp' and self._use_ffmpeg_pipe_rtsp():
                self.cap = None
                self._capture_url = url.split("?")[0].split("&")[0]  # clean URL for ffmpeg
            else:
                if self.camera_type == 'rtsp':
                    print(f"[Capture Agent {self.camera_id}] RTSP via OpenCV (RTP/decoding errors? set RTSP_USE_FFMPEG_PIPE=true in .env)")
                old_opts = os.environ.get("OPENCV_FFMPEG_CAPTURE_OPTIONS", "")
                if self.camera_type == 'rtsp':
                    os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = _FFMPEG_RTSP_OPTS
                try:
                    self.cap = cv2.VideoCapture(url, cv2.CAP_FFMPEG)
                    self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                finally:
                    os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = old_opts
        else:
            self._capture_url = None
            self.cap = cv2.VideoCapture(self.source)

        if self.cap is not None and not self.cap.isOpened():
            raise RuntimeError(f"Failed to open video source: {self.source} (type: {self.camera_type})")
        
        # Set resolution (only for USB cameras)
        if self.cap is not None and self.camera_type == 'usb':
            self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.width)
            self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.height)

        self.is_running = True
        target = self._capture_loop_ffmpeg_pipe if (self.camera_type == 'rtsp' and self._use_ffmpeg_pipe_rtsp()) else self._capture_loop
        self.capture_thread = threading.Thread(target=target, daemon=True)
        self.capture_thread.start()
        print(f"[Capture Agent {self.camera_id}] Capture started with FPS: {self.fps}")
        
    def stop(self):
        """Stop capturing"""
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
        if self.cap is not None:
            self.cap.release()
            self.cap = None
        print(f"[Capture Agent {self.camera_id}] Capture stopped")

    def _capture_loop_ffmpeg_pipe(self):
        """Capture RTSP via FFmpeg subprocess with -rtsp_transport tcp (avoids RTP bad cseq)."""
        import sys
        url = self._capture_url or str(self.source)
        frame_size = self.width * self.height * 3
        last_capture_time = 0
        error_count = 0
        while self.is_running:
            try:
                cmd = [
                    "ffmpeg", "-y", "-loglevel", "error", "-rtsp_transport", "tcp",
                    "-i", url,
                    "-f", "rawvideo", "-pix_fmt", "bgr24",
                    "-s", f"{self.width}x{self.height}",
                    "-r", str(int(self.fps) if self.fps >= 1 else 1),
                    "-an", "-"
                ]
                self._ffmpeg_proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL
                )
                while self.is_running and self._ffmpeg_proc.poll() is None:
                    raw = self._ffmpeg_proc.stdout.read(frame_size)
                    if len(raw) != frame_size:
                        break
                    current_time = time.time()
                    if current_time - last_capture_time >= self.frame_interval:
                        frame = np.frombuffer(raw, dtype=np.uint8).reshape((self.height, self.width, 3))
                        last_capture_time = current_time
                        if self.callback:
                            try:
                                self.callback(frame.copy(), current_time)
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

    def _capture_loop(self):
        """Main capture loop"""
        last_capture_time = 0
        
        while self.is_running:
            current_time = time.time()
            
            # FPS control
            if current_time - last_capture_time < self.frame_interval:
                time.sleep(0.001)  # Small delay to reduce load
                continue
                
            ret, frame = self.cap.read()
            if not ret:
                if not hasattr(self, '_error_count'):
                    self._error_count = 0
                self._error_count += 1
                if self._error_count % 10 == 0:
                    print(f"[Capture Agent {self.camera_id}] Frame read error (count: {self._error_count})")
                # Reconnect RTSP after many consecutive errors (often recovers from decode errors)
                if self._capture_url and self._error_count >= self._reconnect_after_errors:
                    print(f"[Capture Agent {self.camera_id}] Reconnecting stream after {self._error_count} errors...")
                    old_opts = os.environ.get("OPENCV_FFMPEG_CAPTURE_OPTIONS", "")
                    try:
                        self.cap.release()
                        time.sleep(2.0)  # give camera time before reconnect
                        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = _FFMPEG_RTSP_OPTS
                        self.cap = cv2.VideoCapture(self._capture_url, cv2.CAP_FFMPEG)
                        self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                        self._error_count = 0
                    except Exception as e:
                        print(f"[Capture Agent {self.camera_id}] Reconnect failed: {e}")
                    finally:
                        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = old_opts
                time.sleep(0.1)
                continue
            
            # Reset error counter on successful read
            if hasattr(self, '_error_count'):
                self._error_count = 0
            
            # Resize if needed
            if frame.shape[1] != self.width or frame.shape[0] != self.height:
                frame = cv2.resize(frame, (self.width, self.height))
            
            last_capture_time = current_time
            
            # Add to queue or call callback
            if self.callback:
                try:
                    self.callback(frame, current_time)
                except Exception as e:
                    print(f"[Capture Agent {self.camera_id}] Callback error: {e}")
            else:
                # Add to queue (if no callback)
                if not self.frame_queue.full():
                    self.frame_queue.put((frame, current_time))
                else:
                    # Remove old frame if queue is full
                    try:
                        self.frame_queue.get_nowait()
                        self.frame_queue.put((frame, current_time))
                    except:
                        pass
    
    def get_frame(self, timeout: float = 1.0) -> Optional[tuple]:
        """
        Get frame from queue
        
        Returns:
            Tuple (frame, timestamp) or None
        """
        try:
            return self.frame_queue.get(timeout=timeout)
        except:
            return None
    
    def update_fps(self, new_fps: float):
        """Update frame rate"""
        self.fps = new_fps
        self.frame_interval = 1.0 / new_fps if new_fps > 0 else 0.1
        print(f"[Capture Agent {self.camera_id}] FPS updated: {self.fps}")
