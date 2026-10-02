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
        self.enabled = _env_bool("FACE_PREFILTER_ENABLED", True)
        self.min_score = _env_float("FACE_PREFILTER_MIN_SCORE", 0.6)
        self.input_max_side = _env_int("FACE_PREFILTER_INPUT_MAX_SIDE", 320, 64, 1280)
        self.score_threshold = _env_float("FACE_PREFILTER_SCORE_THRESHOLD", 0.5)
        self.nms_threshold = _env_float("FACE_PREFILTER_NMS_THRESHOLD", 0.3)
        self.queue_max = _env_int("YUNET_QUEUE_MAX", 1200, 1, 5000)
        self.workers = _env_int("YUNET_WORKERS", 2, 1, 4)
        default_model = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "models", "face_detection_yunet_2023mar.onnx")
        )
        self.model_path = os.getenv("YUNET_MODEL_PATH") or default_model
        self._queue: Queue = Queue(maxsize=self.queue_max)
        self._encode_queue: Queue = Queue(maxsize=self.queue_max)
        self._threads: list[threading.Thread] = []
        self._encode_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
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
            detectors = [self._make_detector(cv2) for _ in range(self.workers)]
        except Exception as exc:
            self.enabled = False
            print(f"[Yunet] failed to start ({exc}); sending crops without a face filter")
            return
        self._ready = True
        for i, detector in enumerate(detectors):
            thread = threading.Thread(
                target=self._loop, args=(detector,), name=f"yunet-filter-{i}", daemon=True
            )
            thread.start()
            self._threads.append(thread)
        self._encode_thread = threading.Thread(target=self._encode_loop, name="yunet-encode", daemon=True)
        self._encode_thread.start()
        print(
            f"[Yunet] on workers={self.workers} min_score={self.min_score} "
            f"input_max_side={self.input_max_side} queue={self.queue_max} "
            f"disk-first model={self.model_path}"
        )

    def _make_detector(self, cv2):
        return cv2.FaceDetectorYN.create(
            self.model_path,
            "",
            (320, 320),
            self.score_threshold,
            self.nms_threshold,
            5000,
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
        try:
            self._queue.put_nowait(item)
        except Full:
            # Do not delete a crop. Send this one straight to disk so the queued half can still be filtered.
            self._enqueue_encode(item)
            print(f"[Yunet] score queue full ({self.queue_max}); saved one crop without waiting")

    def _enqueue_encode(self, item) -> None:
        try:
            self._encode_queue.put_nowait(item)
        except Full:
            self._deliver(item)

    def _deliver(self, item) -> None:
        crop, timestamp, camera_id, company_id, building_id, company_name, building_name, camera_name = item
        self._on_face(
            crop, timestamp, camera_id, company_id, building_id, company_name, building_name, camera_name
        )

    def stop(self) -> None:
        if self._threads:
            for _ in self._threads:
                self._put_sentinel(self._queue)
            for thread in self._threads:
                thread.join(timeout=5.0)
            self._threads = []
        if self._encode_thread is not None:
            self._put_sentinel(self._encode_queue)
            self._encode_thread.join(timeout=5.0)
            self._encode_thread = None

    def _put_sentinel(self, q: Queue) -> None:
        while True:
            try:
                q.put_nowait(_SENTINEL)
                return
            except Full:
                try:
                    item = q.get_nowait()
                except Empty:
                    continue
                if item is _SENTINEL:
                    return
                self._enqueue_encode(item) if q is self._queue else self._deliver(item)

    def _has_face(self, detector, crop: np.ndarray) -> bool:
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
        detector.setInputSize((ww, wh))
        _, faces = detector.detect(work)
        if faces is None or len(faces) == 0:
            return False
        return float(np.max(faces[:, 14])) >= self.min_score

    def _loop(self, detector) -> None:
        while True:
            try:
                item = self._queue.get(timeout=0.5)
            except Empty:
                continue
            if item is _SENTINEL:
                return
            crop, timestamp, camera_id, company_id, building_id, company_name, building_name, camera_name = item
            try:
                keep = self._has_face(detector, crop)
            except Exception as exc:
                print(f"[Yunet] detect failed, keeping crop: {exc}")
                keep = True
            if not keep:
                self._dropped_noface += 1
                if self._dropped_noface == 1 or self._dropped_noface % 50 == 0:
                    print(f"[Yunet] no face, dropped crop x{self._dropped_noface} camera={camera_id}")
                continue
            self._kept += 1
            self._enqueue_encode(item)

    def _encode_loop(self) -> None:
        while True:
            try:
                item = self._encode_queue.get(timeout=0.5)
            except Empty:
                continue
            if item is _SENTINEL:
                return
            self._deliver(item)
