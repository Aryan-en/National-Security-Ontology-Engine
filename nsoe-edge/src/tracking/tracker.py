"""
Multi-object tracker using ByteTrack algorithm.
Maintains persistent track IDs across frames for each camera session.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

import numpy as np
import structlog

logger = structlog.get_logger(__name__)


class TrackState(str, Enum):
    TENTATIVE = "tentative"  # not yet confirmed (< min_hits)
    CONFIRMED = "confirmed"
    LOST = "lost"            # disappeared but within max_age
    DELETED = "deleted"


@dataclass
class Track:
    track_id: str
    state: TrackState
    class_name: str
    last_seen_ms: int
    birth_ms: int
    hit_count: int
    miss_count: int
    x1: float
    y1: float
    x2: float
    y2: float
    confidence: float

    @property
    def duration_s(self) -> float:
        return (self.last_seen_ms - self.birth_ms) / 1000.0

    def to_dict(self) -> dict:
        return {
            "track_id": self.track_id,
            "state": self.state.value,
            "class_name": self.class_name,
            "last_seen_ms": self.last_seen_ms,
            "birth_ms": self.birth_ms,
            "duration_s": self.duration_s,
            "bbox": {"x1": self.x1, "y1": self.y1, "x2": self.x2, "y2": self.y2},
            "confidence": self.confidence,
        }


class ByteTracker:
    """
    Simplified ByteTrack-inspired multi-object tracker.
    For production deployment, replace with the official ByteTrack or BoT-SORT library.
    """

    def __init__(
        self,
        camera_id: str,
        min_hits: int = 3,
        max_age: int = 30,           # frames before a lost track is deleted
        iou_threshold: float = 0.3,
    ) -> None:
        self.camera_id = camera_id
        self.min_hits = min_hits
        self.max_age = max_age
        self.iou_threshold = iou_threshold
        self._tracks: Dict[str, Track] = {}
        self._next_id: int = 1
        self._frame_count: int = 0

    def _new_track_id(self) -> str:
        tid = f"{self.camera_id}_{self._next_id:06d}"
        self._next_id += 1
        return tid

    @staticmethod
    def _iou(a: Tuple[float, float, float, float], b: Tuple[float, float, float, float]) -> float:
        ax1, ay1, ax2, ay2 = a
        bx1, by1, bx2, by2 = b
        ix1 = max(ax1, bx1)
        iy1 = max(ay1, by1)
        ix2 = min(ax2, bx2)
        iy2 = min(ay2, by2)
        inter = max(0, ix2 - ix1) * max(0, iy2 - iy1)
        if inter == 0:
            return 0.0
        area_a = (ax2 - ax1) * (ay2 - ay1)
        area_b = (bx2 - bx1) * (by2 - by1)
        return inter / (area_a + area_b - inter)

    def update(
        self,
        detections: List,      # List[Detection] from detector.py
        timestamp_ms: int,
    ) -> List[Track]:
        """
        Update tracker with new detections.
        Returns list of active (CONFIRMED) tracks after this frame.
        """
        self._frame_count += 1
        active_track_ids = set()

        # Match detections to existing tracks (greedy IoU)
        unmatched_dets = list(range(len(detections)))
        for track_id, track in list(self._tracks.items()):
            if track.state == TrackState.DELETED:
                continue
            best_iou = self.iou_threshold
            best_det_idx: Optional[int] = None
            for det_idx in unmatched_dets:
                d = detections[det_idx]
                iou = self._iou(
                    (track.x1, track.y1, track.x2, track.y2),
                    (d.x1, d.y1, d.x2, d.y2),
                )
                if iou > best_iou:
                    best_iou = iou
                    best_det_idx = det_idx

            if best_det_idx is not None:
                d = detections[best_det_idx]
                track.x1, track.y1 = d.x1, d.y1
                track.x2, track.y2 = d.x2, d.y2
                track.confidence = d.confidence
                track.last_seen_ms = timestamp_ms
                track.hit_count += 1
                track.miss_count = 0
                if track.hit_count >= self.min_hits:
                    track.state = TrackState.CONFIRMED
                unmatched_dets.remove(best_det_idx)
                active_track_ids.add(track_id)
            else:
                track.miss_count += 1
                if track.miss_count > self.max_age:
                    track.state = TrackState.DELETED
                elif track.state == TrackState.CONFIRMED:
                    track.state = TrackState.LOST

        # Create new tracks for unmatched detections
        for det_idx in unmatched_dets:
            d = detections[det_idx]
            track_id = self._new_track_id()
            self._tracks[track_id] = Track(
                track_id=track_id,
                state=TrackState.TENTATIVE,
                class_name=d.class_name,
                last_seen_ms=timestamp_ms,
                birth_ms=timestamp_ms,
                hit_count=1,
                miss_count=0,
                x1=d.x1, y1=d.y1,
                x2=d.x2, y2=d.y2,
                confidence=d.confidence,
            )

        # Evict deleted tracks older than 5 minutes to cap memory
        cutoff = timestamp_ms - 300_000
        self._tracks = {
            k: v for k, v in self._tracks.items()
            if not (v.state == TrackState.DELETED and v.last_seen_ms < cutoff)
        }

        return [t for t in self._tracks.values() if t.state == TrackState.CONFIRMED]
