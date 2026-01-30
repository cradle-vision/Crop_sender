"""
Snapshot Capture Agent
Captures frames from camera with specified FPS rate
"""

import cv2
import time
import threading
from queue import Queue
from typing import Optional, Callable, Union


class SnapshotCaptureAgent:
    """Agent for capturing snapshots from camera"""
    
    def __init__(self, source: Union[int, str] = 0, fps: float = 10.0, 
                 width: int = 640, height: int = 480, camera_id: str = "camera_0",
                 camera_type: str = "usb"):
        """
        Initialize capture agent
        
        Args:
            source: Video source (index for USB or URL for IP cameras)
            fps: Frames per second
            width: Frame width
            height: Frame height
            camera_id: Camera identifier
            camera_type: Camera type ('usb', 'rtsp', 'http', 'file')
        """
        self.source = source
        self.fps = fps
        self.width = width
        self.height = height
        self.camera_id = camera_id
        self.camera_type = camera_type
        self.frame_interval = 1.0 / fps if fps > 0 else 0.1
        
        self.cap: Optional[cv2.VideoCapture] = None
        self.is_running = False
        self.capture_thread: Optional[threading.Thread] = None
        self.frame_queue: Queue = Queue(maxsize=30)
        self.callback: Optional[Callable] = None
        
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
            # Settings for network streams
            self.cap = cv2.VideoCapture(self.source, cv2.CAP_FFMPEG)
            # Minimal buffer for network cameras
            self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        else:
            self.cap = cv2.VideoCapture(self.source)
        
        if not self.cap.isOpened():
            raise RuntimeError(f"Failed to open video source: {self.source} (type: {self.camera_type})")
        
        # Set resolution (only for USB cameras)
        if self.camera_type == 'usb':
            self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.width)
            self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.height)
        
        self.is_running = True
        self.capture_thread = threading.Thread(target=self._capture_loop, daemon=True)
        self.capture_thread.start()
        print(f"[Capture Agent {self.camera_id}] Capture started with FPS: {self.fps}")
        
    def stop(self):
        """Stop capturing"""
        self.is_running = False
        if self.capture_thread:
            self.capture_thread.join(timeout=2.0)
        if self.cap:
            self.cap.release()
        print(f"[Capture Agent {self.camera_id}] Capture stopped")
        
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
                # Reduce error spam - only print every 10th error
                if not hasattr(self, '_error_count'):
                    self._error_count = 0
                self._error_count += 1
                if self._error_count % 10 == 0:
                    print(f"[Capture Agent {self.camera_id}] Frame read error (count: {self._error_count})")
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
