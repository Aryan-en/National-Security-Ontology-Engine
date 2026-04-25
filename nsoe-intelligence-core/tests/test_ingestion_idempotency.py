"""
Tests for graph write idempotency.
Same event ingested twice → same graph state, no duplicates.
Uses a mock Neo4j session to verify MERGE semantics.
"""

import pytest
from unittest.mock import MagicMock, call
from nsoe_intelligence_core.ingestion.ontology_ingestion_service import (
    OntologyIngestionService,
    validate_sighting,
)


class TestShaclValidation:
    def test_valid_event_passes(self):
        event = {
            "event_id": "evt-001",
            "track_id": "cam01_000001",
            "camera_id": "cam01",
            "detection_class": "PERSON",
            "detection_confidence": 0.85,
        }
        assert validate_sighting(event) == []

    def test_missing_event_id_fails(self):
        event = {
            "track_id": "t01",
            "camera_id": "cam01",
            "detection_class": "PERSON",
            "detection_confidence": 0.8,
        }
        errors = validate_sighting(event)
        assert any("event_id" in e for e in errors)

    def test_invalid_detection_class_fails(self):
        event = {
            "event_id": "evt-002",
            "track_id": "t01",
            "camera_id": "cam01",
            "detection_class": "AIRPLANE",
            "detection_confidence": 0.8,
        }
        errors = validate_sighting(event)
        assert any("detection_class" in e for e in errors)

    def test_confidence_out_of_range_fails(self):
        event = {
            "event_id": "evt-003",
            "track_id": "t01",
            "camera_id": "cam01",
            "detection_class": "PERSON",
            "detection_confidence": 1.5,
        }
        errors = validate_sighting(event)
        assert any("confidence" in e for e in errors)

    def test_negative_confidence_fails(self):
        event = {
            "event_id": "evt-004",
            "track_id": "t01",
            "camera_id": "cam01",
            "detection_class": "PERSON",
            "detection_confidence": -0.1,
        }
        assert validate_sighting(event) != []


class TestGraphWriteIdempotency:
    """
    Verify that _ingest_cctv_event uses MERGE and not CREATE.
    The same event written twice should produce exactly 1 Sighting
    and 1 Camera node (MERGE semantics).
    """

    def _build_service(self):
        svc = OntologyIngestionService.__new__(OntologyIngestionService)
        svc._classification_level = "RESTRICTED"
        return svc

    def _make_event(self, event_id="evt-001"):
        return {
            "event_id": event_id,
            "edge_node_id": "edge-01",
            "camera_id": "cam01",
            "track_id": "cam01_000001",
            "detection_class": "PERSON",
            "detection_confidence": 0.85,
            "normalized_timestamp_ms": 1_714_000_000_000,
            "device_clock_synced": True,
            "geo_location": {
                "latitude": 28.6139, "longitude": 77.2090,
                "geo_zone_id": "ZONE_DELHI_01", "zone_name": "Delhi Central",
            },
            "bbox": {"x1": 0.1, "y1": 0.1, "x2": 0.3, "y2": 0.5},
            "dedup_hash": "abc123",
        }

    def test_merge_queries_used(self):
        """Ingesting a valid event should invoke MERGE_CAMERA and MERGE_SIGHTING."""
        svc = self._build_service()
        session = MagicMock()
        event = self._make_event()
        svc._ingest_cctv_event(session, event)
        # run should have been called 3 times: MERGE_CAMERA, MERGE_SIGHTING, LINK
        assert session.run.call_count == 3
        # First call should be camera merge
        first_call_args = session.run.call_args_list[0]
        assert "MERGE" in first_call_args[0][0]

    def test_duplicate_event_same_call_count(self):
        """Ingesting the same event twice should produce the same number of run() calls."""
        svc = self._build_service()
        session = MagicMock()
        event = self._make_event()
        svc._ingest_cctv_event(session, event)
        first_count = session.run.call_count
        session.reset_mock()
        svc._ingest_cctv_event(session, event)
        second_count = session.run.call_count
        assert first_count == second_count

    def test_invalid_event_not_written(self):
        """An event failing SHACL validation should not call session.run."""
        svc = self._build_service()
        session = MagicMock()
        bad_event = {"event_id": "", "camera_id": "cam01", "detection_class": "PERSON"}
        svc._ingest_cctv_event(session, bad_event)
        session.run.assert_not_called()
