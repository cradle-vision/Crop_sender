"""Greedy IoU multi-object tracker (SORT-lite, no Kalman / ReID)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple

BBox = Tuple[int, int, int, int]


def bbox_iou(a: BBox, b: BBox) -> float:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    ix1 = max(ax1, bx1)
    iy1 = max(ay1, by1)
    ix2 = min(ax2, bx2)
    iy2 = min(ay2, by2)
    iw = max(0, ix2 - ix1)
    ih = max(0, iy2 - iy1)
    inter = iw * ih
    if inter <= 0:
        return 0.0
    area_a = max(0, ax2 - ax1) * max(0, ay2 - ay1)
    area_b = max(0, bx2 - bx1) * max(0, by2 - by1)
    union = area_a + area_b - inter
    return float(inter) / float(union) if union > 0 else 0.0


def foot_point(bbox: BBox) -> Tuple[float, float]:
    """Center of the bottom edge — preferred map projection point."""
    x1, _y1, x2, y2 = bbox
    return (x1 + x2) / 2.0, float(y2)


@dataclass
class Track:
    track_id: str
    bbox: BBox
    hits: int = 1
    age: int = 1
    time_since_update: int = 0


class IoUTracker:
    """Per-camera greedy IoU tracker with stable track_id strings."""

    def __init__(
        self,
        camera_id: str,
        *,
        iou_threshold: float = 0.3,
        max_age: int = 30,
    ) -> None:
        self.camera_id = str(camera_id)
        self.iou_threshold = max(0.05, float(iou_threshold))
        self.max_age = max(1, int(max_age))
        self._next_id = 1
        self._tracks: Dict[int, Track] = {}

    def _make_track_id(self, local_id: int) -> str:
        cam = self.camera_id.replace(" ", "")
        return f"cam{cam}_t{local_id}"

    def update(self, detections: Sequence[BBox]) -> List[Track]:
        dets = [tuple(int(v) for v in d) for d in detections if len(d) == 4]
        dets = [d for d in dets if d[2] > d[0] and d[3] > d[1]]

        for tr in self._tracks.values():
            tr.age += 1
            tr.time_since_update += 1

        if not self._tracks and not dets:
            return []

        track_ids = list(self._tracks.keys())
        matched_tracks: set = set()
        matched_dets: set = set()

        if track_ids and dets:
            pairs: List[Tuple[float, int, int]] = []
            for ti, tid in enumerate(track_ids):
                for di, det in enumerate(dets):
                    iou = bbox_iou(self._tracks[tid].bbox, det)
                    if iou >= self.iou_threshold:
                        pairs.append((iou, ti, di))
            pairs.sort(key=lambda x: x[0], reverse=True)
            for _iou, ti, di in pairs:
                tid = track_ids[ti]
                if tid in matched_tracks or di in matched_dets:
                    continue
                tr = self._tracks[tid]
                tr.bbox = dets[di]
                tr.hits += 1
                tr.time_since_update = 0
                matched_tracks.add(tid)
                matched_dets.add(di)

        for di, det in enumerate(dets):
            if di in matched_dets:
                continue
            local_id = self._next_id
            self._next_id += 1
            self._tracks[local_id] = Track(
                track_id=self._make_track_id(local_id),
                bbox=det,
                hits=1,
                age=1,
                time_since_update=0,
            )

        stale = [
            tid
            for tid, tr in self._tracks.items()
            if tr.time_since_update > self.max_age
        ]
        for tid in stale:
            del self._tracks[tid]

        return [tr for tr in self._tracks.values() if tr.time_since_update == 0]

    def live_track_ids(self) -> set:
        return {tr.track_id for tr in self._tracks.values()}
