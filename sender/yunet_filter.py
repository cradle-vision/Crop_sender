"""Face filter on person crops. A 320px copy is scored; the original crop is what gets uploaded."""

from __future__ import annotations

import os
import threading
from queue import Empty, Full, Queue
from typing import Callable, Optional

import numpy as np

_SENTINEL = object()


def _env_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None or not str(raw).strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(key: str, default: int, lo: int, hi: int) -> int:
    raw = os.getenv(key)
    if raw is None or not str(raw).strip():
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(lo, min(hi, value))


class YunetFilter:
    """Queue between person crops and upload. Disabled means crops go straight to upload."""

    def __init__(self, on_face: Callable[..., None]):
        self._on_face = on_face
        self.enabled = _env_bool("FACE_PREFILTER_ENABLED", False)
        self.min_score = _env_float("FACE_PREFILTER_MIN_SCORE", 0.6)
        self.input_max_side = _env_int("FACE_PREFILTER_INPUT_MAX_SIDE", 320, 64, 1280)
        self.score_threshold = _env_float("FACE_PREFILTER_SCORE_THRESHOLD", 0.5)
        self.nms_threshold = _env_float("FACE_PREFILTER_NMS_THRESHOLD", 0.3)
        self.queue_max = _env_int("YUNET_QUEUE_MAX", 500, 1, 5000)
        default_model = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "models", "face_detection_yunet_2023mar.onnx")
        )
        self.model_path = os.getenv("YUNET_MODEL_PATH") or default_model
        self._queue: Queue = Queue(maxsize=self.queue_max)
        self._thread: Optional[threading.Thread] = None
        self._detector = None
        self._dropped_noface = 0
        self._dropped_full = 0
        self._kept = 0
        self._ready = False

    def start(self) -> None:
        if not self.enabled:
            print("[Yunet] FACE_PREFILTER_ENABLED=false — person crops upload as before")
            return
        try:
            import cv2

            cv2.setNumThreads(1)
            if not os.path.isfile(self.model_path):
                raise FileNotFoundError(self.model_path)
            self._detector = cv2.FaceDetectorYN.create(
                self.model_path,
                "",
                (320, 320),
                self.score_threshold,
                self.nms_threshold,
                5000,
            )
        except Exception as exc:
            self.enabled = False
            self._detector = None
            print(f"[Yunet] failed to start ({exc}); sending crops without a face filter")
            return
        self._ready = True
        self._thread = threading.Thread(target=self._loop, name="yunet-filter", daemon=True)
        self._thread.start()
        print(
            f"[Yunet] on min_score={self.min_score} input_max_side={self.input_max_side} "
            f"queue={self.queue_max} model={self.model_path}"
        )

    def submit(
        self,
        crop: np.ndarray,
        timestamp: float,
        camera_id: str,
        company_id,
        building_id,
        company_name,
        building_name,
        camera_name,
    ) -> None:
        if not self.enabled or not self._ready:
            self._on_face(
                crop, timestamp, camera_id, company_id, building_id, company_name, building_name, camera_name
            )
            return
        item = (crop, timestamp, camera_id, company_id, building_id, company_name, building_name, camera_name)
        while True:
            try:
                self._queue.put_nowait(item)
                return
            except Full:
                try:
                    self._queue.get_nowait()
                    self._dropped_full += 1
                    if self._dropped_full == 1 or self._dropped_full % 50 == 0:
                        print(f"[Yunet] queue full ({self.queue_max}); dropped oldest crop x{self._dropped_full}")
                except Empty:
                    pass

    def stop(self) -> None:
        if self._thread is None:
            return
        while True:
            try:
                self._queue.put_nowait(_SENTINEL)
                break
            except Full:
                try:
                    self._queue.get_nowait()
                except Empty:
                    pass
        self._thread.join(timeout=5.0)
        self._thread = None

    def _has_face(self, crop: np.ndarray) -> bool:
        import cv2

        h, w = crop.shape[:2]
        work = crop
        long_side = max(h, w)
        if self.input_max_side > 0 and long_side > self.input_max_side:
            scale = self.input_max_side / float(long_side)
            work = cv2.resize(
                crop,
                (max(1, int(w * scale)), max(1, int(h * scale))),
                interpolation=cv2.INTER_AREA,
            )
        wh, ww = work.shape[:2]
        self._detector.setInputSize((ww, wh))
        _, faces = self._detector.detect(work)
        if faces is None or len(faces) == 0:
            return False
        return float(np.max(faces[:, 14])) >= self.min_score

    def _loop(self) -> None:
        while True:
            try:
                item = self._queue.get(timeout=0.5)
            except Empty:
                continue
            if item is _SENTINEL:
                return
            crop, timestamp, camera_id, company_id, building_id, company_name, building_name, camera_name = item
            try:
                keep = self._has_face(crop)
            except Exception as exc:
                print(f"[Yunet] detect failed, keeping crop: {exc}")
                keep = True
            if not keep:
                self._dropped_noface += 1
                if self._dropped_noface == 1 or self._dropped_noface % 50 == 0:
                    print(f"[Yunet] no face, dropped crop x{self._dropped_noface} camera={camera_id}")
                continue
            self._kept += 1
            self._on_face(
                crop, timestamp, camera_id, company_id, building_id, company_name, building_name, camera_name
            )
