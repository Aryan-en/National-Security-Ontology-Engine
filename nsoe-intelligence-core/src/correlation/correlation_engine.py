"""
CorrelationEngine — 2.3.1 / 2.3.2 / 2.3.5
Subscribes to enriched events from all three domains (Kafka multi-topic),
evaluates SWRL-derived rules, runs Bayesian fusion,
and writes CorrelationEvent nodes to Neo4j + alert to alerts.active.

Rule categories (from ontology/rules/nsoe-swrl.ttl):
  - co-location: same Camera within 300s window
  - temporal_proximity: events from different domains within 1h
  - shared_artifact: same IoC appears in cyber + socmint
  - network_overlap: same IP in cyber event + ThreatActor
  - communication_overlap: same Person appears in CCTV + SOCMINT
"""

from __future__ import annotations

import json
import os
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import structlog
from confluent_kafka import Consumer, Producer, KafkaError
from neo4j import Driver, GraphDatabase

from nsoe_intelligence_core.correlation.bayesian_fusion import (
    BayesianFusion, EvidenceSignal, FusionResult,
)

logger = structlog.get_logger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
NEO4J_URI       = os.environ.get("NEO4J_URI",       "bolt://localhost:7687")
NEO4J_USER      = os.environ.get("NEO4J_USER",      "neo4j")
NEO4J_PASSWORD  = os.environ.get("NEO4J_PASSWORD",  "")

WINDOW_S = 3600  # 1h temporal window for cross-domain correlation

# ── Rule definitions ─────────────────────────────────────────

@dataclass
class CorrelationMatch:
    rule_id: str
    entity_id: str             # Person or shared artifact
    signals: List[EvidenceSignal]
    timestamp_ms: int

WRITE_CORRELATION_EVENT = """
MERGE (ce:CorrelationEvent {correlation_id: $cid})
ON CREATE SET
    ce.entity_id            = $entity_id,
    ce.fused_confidence     = $fused_confidence,
    ce.alert_tier           = $alert_tier,
    ce.rule_id              = $rule_id,
    ce.domain_contributions = $domain_contributions,
    ce.explanation          = $explanation,
    ce.valid_time_start     = $valid_time_start,
    ce.ingested_at          = $ingested_at,
    ce.source_domain        = 'CORRELATION',
    ce.classification_level = $classification_level
RETURN ce.entity_id AS entity_id
"""

LINK_CORROBORATION = """
MATCH (ce:CorrelationEvent {correlation_id: $cid})
MATCH (e {entity_id: $evidence_entity_id})
MERGE (ce)-[:CORROBORATED_BY]->(e)
"""


class CorrelationEngine:
    """
    Multi-domain event stream correlator.
    Maintains a sliding window buffer of recent events per entity/camera/IP.
    Evaluates rules on every new event; emits CorrelationEvent when rules fire.
    """

    def __init__(
        self,
        neo4j_uri: str = NEO4J_URI,
        neo4j_user: str = NEO4J_USER,
        neo4j_password: str = NEO4J_PASSWORD,
        kafka_bootstrap: str = KAFKA_BOOTSTRAP,
        classification_level: str = "RESTRICTED",
        window_s: int = WINDOW_S,
    ) -> None:
        self._driver: Driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self._bootstrap = kafka_bootstrap
        self._classification = classification_level
        self._window_ms = window_s * 1000
        self._fusion = BayesianFusion()

        # In-memory sliding window buffers
        # Key: entity_id or camera_id or ip → [(timestamp_ms, event, domain)]
        self._entity_window: Dict[str, List[Tuple[int, Dict, str]]] = defaultdict(list)
        self._camera_window: Dict[str, List[Tuple[int, Dict, str]]] = defaultdict(list)
        self._ip_window:     Dict[str, List[Tuple[int, Dict, str]]] = defaultdict(list)
        self._emitted: set = set()  # dedup key set to avoid re-emitting same correlation

        self._consumer: Optional[Consumer] = None
        self._producer: Optional[Producer] = None
        self._running = False

    def start(self) -> None:
        self._consumer = Consumer({
            "bootstrap.servers": self._bootstrap,
            "group.id": "nsoe-correlation-engine",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        self._consumer.subscribe([
            "enriched.cctv.events",
            "cyber.normalized.events",
            "socmint.processed.events",
        ])
        self._producer = Producer({
            "bootstrap.servers": self._bootstrap,
            "enable.idempotence": True,
            "acks": "all",
        })
        self._running = True
        logger.info("correlation_engine_started")
        self._loop()

    def stop(self) -> None:
        self._running = False
        if self._consumer: self._consumer.close()
        if self._producer: self._producer.flush()
        self._driver.close()

    def _loop(self) -> None:
        batch = []
        last_commit = time.monotonic()
        while self._running:
            msg = self._consumer.poll(timeout=0.5)
            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("kafka_error", error=str(msg.error()))
            else:
                try:
                    event = json.loads(msg.value().decode())
                    domain = self._infer_domain(msg.topic())
                    batch.append((domain, event))
                except Exception:
                    pass

            if len(batch) >= 50 or (time.monotonic() - last_commit > 1.0 and batch):
                for domain, event in batch:
                    self._ingest(domain, event)
                self._consumer.commit(asynchronous=False)
                self._producer.flush(timeout=3.0)
                batch.clear()
                last_commit = time.monotonic()

    @staticmethod
    def _infer_domain(topic: str) -> str:
        if "cctv" in topic:    return "CCTV"
        if "cyber" in topic:   return "CYBER"
        if "socmint" in topic: return "SOCMINT"
        return "UNKNOWN"

    def _ingest(self, domain: str, event: dict) -> None:
        ts = self._event_ts(event, domain)
        now = int(time.time() * 1000)
        cutoff = now - self._window_ms

        # 1. Purge expired entries
        for buf in (self._entity_window, self._camera_window, self._ip_window):
            for key in list(buf):
                buf[key] = [(t, e, d) for t, e, d in buf[key] if t > cutoff]

        # 2. Index event by its keys
        if domain == "CCTV":
            cam_id = event.get("camera_id", "")
            if cam_id:
                self._camera_window[cam_id].append((ts, event, domain))

        if domain == "SOCMINT":
            for ent in event.get("entities", []):
                if ent.get("label") == "PER":
                    key = f"socmint_person_{ent['text'].lower()}"
                    self._entity_window[key].append((ts, event, domain))

        if domain == "CYBER":
            for ip in filter(None, [event.get("src_ip"), event.get("dst_ip")]):
                self._ip_window[ip].append((ts, event, domain))

        # 3. Evaluate rules
        self._eval_rules(domain, event, ts)

    def _eval_rules(self, domain: str, event: dict, ts: int) -> None:
        # RULE-001: watchlist_person in SOCMINT + CCTV Sighting in same 1h window
        if domain == "SOCMINT" and event.get("threat_intent_score", 0) >= 0.6:
            for ent in event.get("entities", []):
                if ent.get("label") == "PER":
                    name_key = f"socmint_person_{ent['text'].lower()}"
                    # Look for CCTV sightings of watchlisted persons in window
                    # (simplified: any PERSON sighting in any camera in this window)
                    for cam_key, entries in self._camera_window.items():
                        cctv_sightings = [(t, e, d) for t, e, d in entries
                                          if d == "CCTV" and e.get("detection_class") == "PERSON"
                                          and abs(t - ts) < self._window_ms]
                        if cctv_sightings:
                            self._emit_correlation("RULE-006-SOCMINT-CCTV", event, cctv_sightings[0][1], ts)

        # RULE-002: cyber intrusion + physical access at same facility in 1h window
        if domain == "CYBER" and event.get("is_anomaly"):
            facility = event.get("facility_id")
            if facility:
                for cam_key, entries in self._camera_window.items():
                    if cam_key.startswith(facility):
                        cctv_events = [(t, e, d) for t, e, d in entries
                                       if abs(t - ts) < self._window_ms]
                        if cctv_events:
                            self._emit_correlation("RULE-002-CYBER-PHYSICAL", event, cctv_events[0][1], ts)

    def _emit_correlation(self, rule_id: str, ev_a: dict, ev_b: dict, ts: int) -> None:
        dedup_key = f"{rule_id}_{ev_a.get('event_id','')}_{ev_b.get('event_id','')}"
        if dedup_key in self._emitted:
            return
        self._emitted.add(dedup_key)
        if len(self._emitted) > 100_000:
            self._emitted = set(list(self._emitted)[-50_000:])

        domain_a = "SOCMINT" if "threat_intent" in ev_a else "CYBER"
        domain_b = "CCTV"

        signals = [
            EvidenceSignal(
                domain=domain_a,
                signal_type="THREAT_POST" if domain_a == "SOCMINT" else "NETWORK_ANOMALY",
                confidence=ev_a.get("threat_intent_score") or ev_a.get("anomaly_score", 0.6) or 0.6,
                source_credibility=ev_a.get("source_credibility", 0.7),
                event_id=ev_a.get("event_id", ""),
            ),
            EvidenceSignal(
                domain=domain_b,
                signal_type="SIGHTING",
                confidence=ev_b.get("detection_confidence", 0.7),
                source_credibility=1.0,
                event_id=ev_b.get("event_id", ""),
            ),
        ]

        result: FusionResult = self._fusion.fuse(signals)
        if result.alert_tier == "BELOW_THRESHOLD":
            return

        cid = str(uuid.uuid4())
        now_iso = datetime.now(timezone.utc).isoformat()
        ts_iso  = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()

        # Write CorrelationEvent to Neo4j
        with self._driver.session() as session:
            session.run(WRITE_CORRELATION_EVENT, {
                "cid":                  cid,
                "entity_id":            f"corr_{cid}",
                "fused_confidence":     result.fused_confidence,
                "alert_tier":           result.alert_tier,
                "rule_id":              rule_id,
                "domain_contributions": json.dumps(result.domain_contributions),
                "explanation":          result.explanation,
                "valid_time_start":     ts_iso,
                "ingested_at":          now_iso,
                "classification_level": self._classification,
            })
            for sig in result.contributing_signals:
                if sig.event_id:
                    try:
                        session.run(LINK_CORROBORATION, {
                            "cid": cid,
                            "evidence_entity_id": f"sighting_{sig.event_id}",
                        })
                    except Exception:
                        pass

        # Publish alert
        alert = {
            "alert_id":            cid,
            "alert_type":          "CORROBORATED",
            "alert_tier":          result.alert_tier,
            "fused_confidence":    result.fused_confidence,
            "camera_id":           ev_b.get("camera_id"),
            "geo_zone_id":         ev_b.get("geo_location", {}).get("geo_zone_id"),
            "latitude":            ev_b.get("geo_location", {}).get("latitude"),
            "longitude":           ev_b.get("geo_location", {}).get("longitude"),
            "involved_track_ids":  [ev_b.get("track_id", "")],
            "involved_entity_ids": [],
            "contributing_events": [s.event_id for s in result.contributing_signals],
            "source_domains":      list({s.domain for s in result.contributing_signals}),
            "triggered_at_ms":     ts,
            "ttl_seconds":         86400,
            "rule_id":             rule_id,
            "explanation":         result.explanation,
            "schema_version":      "1.0.0",
        }
        self._producer.produce(
            topic="alerts.active",
            key=rule_id.encode(),
            value=json.dumps(alert).encode(),
        )
        logger.info("correlation_alert_emitted",
                    rule=rule_id, tier=result.alert_tier,
                    confidence=result.fused_confidence)

    @staticmethod
    def _event_ts(event: dict, domain: str) -> int:
        if domain == "CCTV":    return event.get("normalized_timestamp_ms", int(time.time() * 1000))
        if domain == "CYBER":   return event.get("received_at_ms", int(time.time() * 1000))
        if domain == "SOCMINT": return event.get("posted_at_ms",   int(time.time() * 1000))
        return int(time.time() * 1000)
