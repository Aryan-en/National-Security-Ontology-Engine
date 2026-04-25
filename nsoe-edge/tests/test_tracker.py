"""Unit tests for ByteTracker."""

import pytest
from nsoe_edge.tracking.tracker import ByteTracker, TrackState
from nsoe_edge.detection.detector import Detection


def _det(x1=0.1, y1=0.1, x2=0.3, y2=0.4, cls="PERSON", conf=0.85):
    return Detection(
        class_name=cls, raw_class=cls.lower(),
        confidence=conf, x1=x1, y1=y1, x2=x2, y2=y2,
        frame_width=1920, frame_height=1080,
    )


class TestByteTracker:
    def test_new_track_tentative(self):
        tracker = ByteTracker("cam01", min_hits=3)
        tracks = tracker.update([_det()], timestamp_ms=1000)
        # Not confirmed yet (only 1 hit, min_hits=3)
        assert len(tracks) == 0

    def test_track_confirmed_after_min_hits(self):
        tracker = ByteTracker("cam01", min_hits=3)
        for i in range(3):
            tracks = tracker.update([_det()], timestamp_ms=1000 + i * 100)
        assert len(tracks) == 1
        assert tracks[0].state == TrackState.CONFIRMED

    def test_track_id_stable_across_frames(self):
        tracker = ByteTracker("cam01", min_hits=2)
        for i in range(3):
            tracks = tracker.update([_det(x1=0.1 + i * 0.001)], timestamp_ms=1000 + i * 100)
        assert len(tracks) == 1
        tid = tracks[0].track_id
        tracks2 = tracker.update([_det(x1=0.103)], timestamp_ms=1400)
        assert tracks2[0].track_id == tid

    def test_separate_tracks_for_non_overlapping(self):
        tracker = ByteTracker("cam01", min_hits=1)
        # Two detections far apart
        tracks = tracker.update([
            _det(x1=0.0, y1=0.0, x2=0.1, y2=0.1),
            _det(x1=0.9, y1=0.9, x2=1.0, y2=1.0),
        ], timestamp_ms=1000)
        assert len(tracks) == 2
        assert tracks[0].track_id != tracks[1].track_id

    def test_track_lost_after_max_age(self):
        tracker = ByteTracker("cam01", min_hits=1, max_age=2)
        tracker.update([_det()], timestamp_ms=1000)
        tracker.update([], timestamp_ms=1100)
        tracker.update([], timestamp_ms=1200)
        # 3 misses > max_age=2, should be deleted
        tracks = tracker.update([], timestamp_ms=1300)
        assert len(tracks) == 0

    def test_duration_tracking(self):
        tracker = ByteTracker("cam01", min_hits=1)
        tracker.update([_det()], timestamp_ms=0)
        tracks = tracker.update([_det()], timestamp_ms=5000)
        assert len(tracks) == 1
        assert tracks[0].duration_s == pytest.approx(5.0, abs=0.1)
