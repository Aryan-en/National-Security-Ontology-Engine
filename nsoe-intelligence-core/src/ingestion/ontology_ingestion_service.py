"""
OntologyIngestionService
- Consumes from enriched.cctv.events (Kafka)
- Maps detection event → nsoe:Sighting node
- Maps camera → nsoe:Location node (upsert, not duplicate)
- SHACL-validates entities before graph write
- Writes to Neo4j with provenance metadata (MERGE semantics)
- Idempotent: same event ingested twice → same graph state
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException
from neo4j import GraphDatabase, Driver
from pydantic import BaseModel, Field, validator

logger = structlog.get_logger(__name__)

# ============================================================
# Neo4j Cypher Queries
# ============================================================

MERGE_CAMERA = """
MERGE (c:Camera:Location {camera_id: $camera_id})
ON CREATE SET
    c.entity_id         = $entity_id,
    c.latitude          = $latitude,
    c.longitude         = $longitude,
    c.geo_zone_id       = $geo_zone_id,
    c.zone_name         = $zone_name,
    c.ingested_at       = $ingested_at,
    c.classification_level = $classification_level
ON MATCH SET
    c.latitude          = $latitude,
    c.longitude         = $longitude,
    c.geo_zone_id       = $geo_zone_id,
    c.zone_name         = $zone_name
RETURN c.entity_id AS entity_id
"""

MERGE_SIGHTING = """
MERGE (s:Sighting:PhysicalEvent {event_id: $event_id})
ON CREATE SET
    s.entity_id             = $entity_id,
    s.track_id              = $track_id,
    s.detection_class       = $detection_class,
    s.detection_confidence  = $detection_confidence,
    s.bbox_x1               = $bbox_x1,
    s.bbox_y1               = $bbox_y1,
    s.bbox_x2               = $bbox_x2,
    s.bbox_y2               = $bbox_y2,
    s.valid_time_start      = $valid_time_start,
    s.ingested_at           = $ingested_at,
    s.source_id             = $source_id,
    s.classification_level  = $classification_level,
    s.source_domain         = 'CCTV',
    s.dedup_hash            = $dedup_hash
RETURN s.entity_id AS entity_id
"""

LINK_SIGHTING_TO_CAMERA = """
MATCH (s:Sighting {event_id: $event_id})
MATCH (c:Camera {camera_id: $camera_id})
MERGE (s)-[:SIGHTED_AT]->(c)
"""

CREATE_INDEXES = [
    "CREATE CONSTRAINT sighting_event_id IF NOT EXISTS FOR (s:Sighting) REQUIRE s.event_id IS UNIQUE",
    "CREATE CONSTRAINT camera_id IF NOT EXISTS FOR (c:Camera) REQUIRE c.camera_id IS UNIQUE",
    "CREATE INDEX sighting_track_id IF NOT EXISTS FOR (s:Sighting) ON (s.track_id)",
    "CREATE INDEX sighting_class IF NOT EXISTS FOR (s:Sighting) ON (s.detection_class)",
    "CREATE INDEX sighting_ts IF NOT EXISTS FOR (s:Sighting) ON (s.valid_time_start)",
    "CREATE INDEX entity_risk_score IF NOT EXISTS FOR (p:Person) ON (p.risk_score)",
]

# ============================================================
# SHACL Validation (simplified inline)
# ============================================================

VALID_DETECTION_CLASSES = {"PERSON", "BAG", "VEHICLE", "WEAPON", "UNKNOWN"}
VALID_CLASSIFICATION_LEVELS = {"UNCLASSIFIED", "RESTRICTED", "SECRET", "TOP_SECRET"}


def validate_sighting(event: Dict) -> List[str]:
    """Returns list of validation error messages. Empty = valid."""
    errors = []
    for field in ("event_id", "track_id", "camera_id", "detection_class"):
        if not event.get(field):
            errors.append(f"Missing required field: {field}")
    cls = event.get("detection_class", "")
    if cls and cls not in VALID_DETECTION_CLASSES:
        errors.append(f"Invalid detection_class: {cls}")
    conf = event.get("detection_confidence")
    if conf is not None and not (0.0 <= float(conf) <= 1.0):
        errors.append(f"detection_confidence out of range: {conf}")
    return errors


# ============================================================
# Ingestion Service
# ============================================================

class OntologyIngestionService:

    def __init__(
        self,
        kafka_bootstrap: str,
        kafka_group_id: str,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
        classification_level: str = "RESTRICTED",
    ) -> None:
        self._kafka_bootstrap = kafka_bootstrap
        self._kafka_group_id = kafka_group_id
        self._neo4j_uri = neo4j_uri
        self._neo4j_user = neo4j_user
        self._neo4j_password = neo4j_password
        self._classification_level = classification_level
        self._driver: Optional[Driver] = None
        self._consumer: Optional[Consumer] = None
        self._running = False

    def start(self) -> None:
        self._driver = GraphDatabase.driver(
            self._neo4j_uri,
            auth=(self._neo4j_user, self._neo4j_password),
        )
        self._setup_indexes()

        self._consumer = Consumer({
            "bootstrap.servers": self._kafka_bootstrap,
            "group.id": self._kafka_group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        self._consumer.subscribe(["enriched.cctv.events"])
        self._running = True
        logger.info("ingestion_service_started")
        self._consume_loop()

    def stop(self) -> None:
        self._running = False
        if self._consumer:
            self._consumer.close()
        if self._driver:
            self._driver.close()

    def _setup_indexes(self) -> None:
        with self._driver.session() as session:
            for cypher in CREATE_INDEXES:
                try:
                    session.run(cypher)
                except Exception as e:
                    logger.warning("index_creation_warning", error=str(e))

    def _consume_loop(self) -> None:
        batch: List[Dict] = []
        last_commit = time.monotonic()

        while self._running:
            msg = self._consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    pass
                else:
                    logger.error("kafka_error", error=str(msg.error()))
            else:
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    batch.append(event)
                except Exception as e:
                    logger.error("parse_error", error=str(e))

            # Process batch every 100 events or every 2 seconds
            if len(batch) >= 100 or (time.monotonic() - last_commit > 2.0 and batch):
                self._process_batch(batch)
                self._consumer.commit(asynchronous=False)
                batch.clear()
                last_commit = time.monotonic()

    def _process_batch(self, events: List[Dict]) -> None:
        with self._driver.session() as session:
            for event in events:
                self._ingest_cctv_event(session, event)

    def _ingest_cctv_event(self, session: Any, event: Dict) -> None:
        # SHACL validation
        errors = validate_sighting(event)
        if errors:
            logger.warning("shacl_validation_failed", errors=errors, event_id=event.get("event_id"))
            return

        now_iso = datetime.now(timezone.utc).isoformat()
        ts_ms = event.get("normalized_timestamp_ms", int(time.time() * 1000))
        ts_iso = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()

        geo = event.get("geo_location", {})
        camera_id = event.get("camera_id", "")

        # Upsert Camera node
        session.run(MERGE_CAMERA, {
            "camera_id":            camera_id,
            "entity_id":            f"camera_{camera_id}",
            "latitude":             geo.get("latitude", 0.0),
            "longitude":            geo.get("longitude", 0.0),
            "geo_zone_id":          geo.get("geo_zone_id", "UNKNOWN"),
            "zone_name":            geo.get("zone_name", "UNKNOWN"),
            "ingested_at":          now_iso,
            "classification_level": self._classification_level,
        })

        event_id = event.get("event_id", str(uuid.uuid4()))
        bbox = event.get("bbox", {})

        # Upsert Sighting node
        session.run(MERGE_SIGHTING, {
            "event_id":             event_id,
            "entity_id":            f"sighting_{event_id}",
            "track_id":             event.get("track_id", ""),
            "detection_class":      event.get("detection_class", "UNKNOWN"),
            "detection_confidence": float(event.get("detection_confidence", 0.0)),
            "bbox_x1":              float(bbox.get("x1", 0.0)),
            "bbox_y1":              float(bbox.get("y1", 0.0)),
            "bbox_x2":              float(bbox.get("x2", 0.0)),
            "bbox_y2":              float(bbox.get("y2", 0.0)),
            "valid_time_start":     ts_iso,
            "ingested_at":          now_iso,
            "source_id":            event.get("edge_node_id", ""),
            "classification_level": self._classification_level,
            "dedup_hash":           event.get("dedup_hash", ""),
        })

        # Link Sighting → Camera
        session.run(LINK_SIGHTING_TO_CAMERA, {
            "event_id":  event_id,
            "camera_id": camera_id,
        })

        logger.debug("sighting_ingested", event_id=event_id, camera_id=camera_id)


def main() -> None:
    svc = OntologyIngestionService(
        kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_group_id="nsoe-ontology-ingestion",
        neo4j_uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_user=os.environ.get("NEO4J_USER", "neo4j"),
        neo4j_password=os.environ.get("NEO4J_PASSWORD", ""),
        classification_level=os.environ.get("NSOE_CLASSIFICATION_LEVEL", "RESTRICTED"),
    )
    svc.start()


if __name__ == "__main__":
    main()
