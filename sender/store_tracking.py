"""Indoor store tracking: IoU tracks → positions Kafka + face crops → face pipeline."""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Tuple

import numpy as np

from iou_tracker import IoUTracker, Track, foot_point

if TYPE_CHECKING:
    from kafka_sender_agent import KafkaSenderAgent

BBox = Tuple[int, int, int, int]


def _env(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return v.strip() if v else default


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _env_float(key: str, default: float) -> float:
    try:
        v = os.getenv(key)
        return float(v) if v else default
    except (ValueError, TypeError):
        return default


def _env_int(key: str, default: int) -> int:
    try:
        v = os.getenv(key)
        return int(v) if v else default
    except (ValueError, TypeError):
        return default


def upper_third_face_crop(frame: np.ndarray, bbox: BBox) -> Optional[np.ndarray]:
    """Upper third of person bbox — good enough for backend face detector (640×640)."""
    h_img, w_img = frame.shape[:2]
    x1, y1, x2, y2 = bbox
    x1 = max(0, min(int(x1), w_img - 1))
    y1 = max(0, min(int(y1), h_img - 1))
    x2 = max(0, min(int(x2), w_img))
    y2 = max(0, min(int(y2), h_img))
    if x2 <= x1 or y2 <= y1:
        return None
    box_h = y2 - y1
    face_y2 = y1 + max(1, box_h // 3)
    crop = frame[y1:face_y2, x1:x2]
    if crop.size == 0 or crop.shape[0] < 8 or crop.shape[1] < 8:
        return None
    return crop.copy()


class StoreTrackingPublisher:
    """
    Per-camera IoU tracking + throttled position / face Kafka publish.

    Positions: TRACKING_POSITIONS_TOPIC (default store_tracking_positions), 1–2 Hz per track.
    Faces: upper-third person crop → MinIO → FACE_PIPELINE_TOPIC (triton_face_pipeline).
    """

    def __init__(self, sender: "KafkaSenderAgent") -> None:
        self.sender = sender
        self.enabled = _env_bool("TRACKING_ENABLED", True)
        self.positions_topic = _env("TRACKING_POSITIONS_TOPIC") or "store_tracking_positions"
        self.face_topic = _env("FACE_PIPELINE_TOPIC") or "triton_face_pipeline"
        interval_ms = max(500, _env_int("TRACKING_INTERVAL_MS", 700))
        self.interval_sec = interval_ms / 1000.0
        self.face_resend_sec = max(0.0, _env_float("FACE_RESEND_SEC", 0.0))
        self.iou_threshold = _env_float("TRACKING_IOU_THRESHOLD", 0.3)
        self.max_age = _env_int("TRACKING_MAX_AGE", 30)
        self._trackers: Dict[str, IoUTracker] = {}
        self._last_pos_mono: Dict[str, float] = {}
        self._face_sent_mono: Dict[str, float] = {}
        self._pos_sent = 0
        self._face_sent = 0
        self._skip_meta_logged: set = set()
        if self.enabled:
            print(
                f"[Store Tracking] enabled topics={self.positions_topic}/{self.face_topic} "
                f"interval_ms={interval_ms} face_resend_sec={self.face_resend_sec}"
            )
        else:
            print("[Store Tracking] disabled (TRACKING_ENABLED=false)")

    def _tracker_for(self, camera_id: str) -> IoUTracker:
        tr = self._trackers.get(camera_id)
        if tr is None:
            tr = IoUTracker(
                camera_id,
                iou_threshold=self.iou_threshold,
                max_age=self.max_age,
            )
            self._trackers[camera_id] = tr
        return tr

    def process_frame(
        self,
        frame: np.ndarray,
        timestamp: float,
        camera_id: str,
        company_id: Optional[str],
        building_id: Optional[str],
        detections: Sequence[BBox],
    ) -> List[Track]:
        if not self.enabled:
            return []
        cam = str(camera_id)
        company_id = str(company_id).strip() if company_id is not None else ""
        building_id = str(building_id).strip() if building_id is not None else ""
        if not company_id or not building_id:
            if cam not in self._skip_meta_logged:
                self._skip_meta_logged.add(cam)
                print(
                    f"[Store Tracking] camera={cam} skipped: need company_id and building_id"
                )
            return []

        tracker = self._tracker_for(cam)
        tracks = tracker.update(detections)
        now = time.monotonic()
        ts_ms = int(float(timestamp) * 1000)

        for track in tracks:
            tid = track.track_id
            last = self._last_pos_mono.get(tid, 0.0)
            if now - last >= self.interval_sec:
                lx, ly = foot_point(track.bbox)
                ok = self.sender.send_tracking_position(
                    topic=self.positions_topic,
                    camera_id=cam,
                    company_id=company_id,
                    building_id=building_id,
                    track_id=tid,
                    local_x=lx,
                    local_y=ly,
                    timestamp_ms=ts_ms,
                )
                if ok:
                    self._last_pos_mono[tid] = now
                    self._pos_sent += 1
                    if self._pos_sent == 1 or self._pos_sent % 100 == 0:
                        print(f"[Store Tracking] positions published={self._pos_sent}")

            should_face = tid not in self._face_sent_mono
            if (
                not should_face
                and self.face_resend_sec > 0
                and now - self._face_sent_mono[tid] >= self.face_resend_sec
            ):
                should_face = True
            if not should_face:
                continue

            face = upper_third_face_crop(frame, track.bbox)
            if face is None:
                continue
            ok = self.sender.send_face_pipeline(
                topic=self.face_topic,
                frame=face,
                timestamp=timestamp,
                camera_id=cam,
                company_id=company_id,
                building_id=building_id,
                track_id=tid,
            )
            if ok:
                self._face_sent_mono[tid] = now
                self._face_sent += 1
                if self._face_sent == 1 or self._face_sent % 20 == 0:
                    print(f"[Store Tracking] faces published={self._face_sent}")

        live_ids = tracker.live_track_ids()
        prefix = f"cam{cam}_"
        for store in (self._last_pos_mono, self._face_sent_mono):
            stale = [k for k in store if k.startswith(prefix) and k not in live_ids]
            for k in stale:
                del store[k]

        return tracks
