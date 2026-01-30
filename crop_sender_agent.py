"""
Crop Sender Agent — sends cropped face images to user endpoint (HTTP POST).
Second sender: snapshot → face detect → crop → POST to backend/user.
"""

import cv2
import numpy as np
import urllib.request
import urllib.error
import time
from typing import Optional


class CropSenderAgent:
    """Sends cropped face images to HTTP endpoint (e.g. backend for user)."""
    
    def __init__(self, destination_url: str, timeout: float = 5.0):
        self.destination_url = destination_url.rstrip("/")
        self.timeout = timeout
    
    def send_crop(self, crop_image: np.ndarray, camera_id: str, timestamp: float, user_id: Optional[str] = None) -> bool:
        """
        POST cropped image to destination. Multipart or JSON+base64.
        
        Args:
            crop_image: BGR cropped face image
            camera_id: Camera identifier
            timestamp: Frame timestamp
            user_id: Optional user id (if backend needs it)
        
        Returns:
            True if sent successfully
        """
        try:
            _, buf = cv2.imencode(".jpg", crop_image)
            data = buf.tobytes()
            # Multipart form: camera_id, timestamp, image (file), optional user_id
            boundary = "----WebKitFormBoundary" + str(int(time.time() * 1000))
            body = []
            body.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"camera_id\"\r\n\r\n{camera_id}\r\n")
            body.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"timestamp\"\r\n\r\n{int(timestamp * 1000)}\r\n")
            if user_id:
                body.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"user_id\"\r\n\r\n{user_id}\r\n")
            body.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"image\"; filename=\"crop.jpg\"\r\nContent-Type: image/jpeg\r\n\r\n")
            body_bytes = "".join(body).encode("utf-8") + data + f"\r\n--{boundary}--\r\n".encode("utf-8")
            req = urllib.request.Request(self.destination_url, data=body_bytes, method="POST")
            req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
            req.add_header("Content-Length", str(len(body_bytes)))
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                if 200 <= resp.getcode() < 300:
                    return True
                return False
        except urllib.error.HTTPError as e:
            if not getattr(self, "_log_count", 0) or self._log_count % 20 == 0:
                print(f"[Crop Sender] HTTP error: {e.code} {e.reason}")
            self._log_count = getattr(self, "_log_count", 0) + 1
            return False
        except urllib.error.URLError as e:
            if not getattr(self, "_log_count", 0) or self._log_count % 20 == 0:
                print(f"[Crop Sender] URL error: {e.reason}")
            self._log_count = getattr(self, "_log_count", 0) + 1
            return False
        except Exception as e:
            print(f"[Crop Sender] Error: {e}")
            return False
