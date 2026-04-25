"""
SOCMINT Processor — 2.2.5 / 2.2.6 / 2.2.7
Kafka consumer → NLP pipeline → socmint.processed.events
Also writes SocialMediaEvent, ThreatPost, and author→Person nodes to Neo4j.
Implements the 48h hold buffer: SOCMINT-only flags are not escalated until
corroborated by another domain or until 48h expires.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import structlog
from confluent_kafka import Consumer, Producer, KafkaError
from neo4j import GraphDatabase, Driver

from nsoe_intelligence_core.socmint.nlp_pipeline import NLPPipeline, NLPResult
from nsoe_intelligence_core.socmint.credibility_bot import (
    SourceCredibilityScorer, BotDetector,
)

logger = structlog.get_logger(__name__)

HOLD_HOURS = int(os.environ.get("SOCMINT_HOLD_HOURS", 48))

MERGE_SOCIAL_EVENT = """
MERGE (e:SocialMediaEvent {post_id: $post_id, platform: $platform})
ON CREATE SET
    e.entity_id             = $entity_id,
    e.platform              = $platform,
    e.text_original         = $text_original,
    e.text_translated       = $text_translated,
    e.detected_language     = $detected_language,
    e.threat_intent_score   = $threat_intent_score,
    e.source_credibility    = $source_credibility,
    e.is_bot                = $is_bot,
    e.valid_time_start      = $valid_time_start,
    e.ingested_at           = $ingested_at,
    e.source_domain         = 'SOCMINT',
    e.hold_until            = $hold_until,
    e.classification_level  = $classification_level
RETURN e.entity_id AS entity_id
"""

MERGE_THREAT_POST = """
MATCH (e:SocialMediaEvent {entity_id: $social_event_id})
MERGE (tp:ThreatPost:SocialMediaEvent {entity_id: $entity_id})
ON CREATE SET
    tp.threat_intent_score  = $threat_intent_score,
    tp.source_credibility   = $source_credibility,
    tp.platform             = $platform,
    tp.valid_time_start     = $valid_time_start,
    tp.ingested_at          = $ingested_at,
    tp.classification_level = $classification_level
MERGE (tp)-[:DERIVED_FROM]->(e)
RETURN tp.entity_id AS entity_id
"""

MERGE_AUTHOR_PERSON = """
MERGE (p:Person {author_id: $author_id, platform: $platform})
ON CREATE SET
    p.entity_id             = $entity_id,
    p.author_handle         = $author_handle,
    p.source_domain         = 'SOCMINT',
    p.ingested_at           = $ingested_at,
    p.classification_level  = $classification_level
RETURN p.entity_id AS entity_id
"""

LINK_AUTHOR_TO_EVENT = """
MATCH (p:Person {entity_id: $person_entity_id})
MATCH (e:SocialMediaEvent {entity_id: $event_entity_id})
MERGE (p)-[:AUTHORED]->(e)
"""


class SocmintProcessor:

    def __init__(
        self,
        kafka_bootstrap: str,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
        classification_level: str = "RESTRICTED",
        threat_threshold: float = 0.5,
    ) -> None:
        self._kafka_bootstrap = kafka_bootstrap
        self._classification = classification_level
        self._threat_threshold = threat_threshold
        self._driver: Driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self._nlp = NLPPipeline()
        self._credibility = SourceCredibilityScorer()
        self._bot_detector = BotDetector()
        self._consumer: Optional[Consumer] = None
        self._producer: Optional[Producer] = None
        self._running = False

    def start(self) -> None:
        self._nlp.load()
        self._consumer = Consumer({
            "bootstrap.servers": self._kafka_bootstrap,
            "group.id": "nsoe-socmint-processor",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        self._consumer.subscribe(["socmint.raw.events"])
        self._producer = Producer({
            "bootstrap.servers": self._kafka_bootstrap,
            "enable.idempotence": True,
            "acks": "all",
        })
        self._running = True
        logger.info("socmint_processor_started")
        self._consume_loop()

    def stop(self) -> None:
        self._running = False
        if self._consumer: self._consumer.close()
        if self._producer: self._producer.flush()
        self._driver.close()

    def _consume_loop(self) -> None:
        batch: List[Dict] = []
        last_commit = time.monotonic()
        while self._running:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("kafka_error", error=str(msg.error()))
            else:
                try:
                    batch.append(json.loads(msg.value().decode()))
                except Exception:
                    pass

            if len(batch) >= 20 or (time.monotonic() - last_commit > 3.0 and batch):
                for raw in batch:
                    self._process(raw)
                self._consumer.commit(asynchronous=False)
                self._producer.flush(timeout=5.0)
                batch.clear()
                last_commit = time.monotonic()

    def _process(self, raw: dict) -> None:
        text = raw.get("text", "")
        platform = raw.get("platform", "UNKNOWN")
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()

        # NLP
        result: NLPResult = self._nlp.process(text)

        # Credibility & bot detection
        credibility = self._credibility.score_from_metadata(raw.get("author_metadata", {}), platform)
        is_bot, _bot_conf = self._bot_detector.detect_from_metadata(raw.get("author_metadata", {}))

        # 48h hold for SOCMINT-only threat signals
        hold_until_ms: Optional[int] = None
        if result.threat_intent_score >= self._threat_threshold:
            hold_until_ms = int((now + timedelta(hours=HOLD_HOURS)).timestamp() * 1000)

        processed: Dict[str, Any] = {
            "event_id":           raw.get("event_id", str(uuid.uuid4())),
            "platform":           platform,
            "post_id":            raw.get("post_id", ""),
            "author_id":          raw.get("author_id"),
            "author_handle":      raw.get("author_handle"),
            "text_original":      text,
            "text_translated":    result.text_translated,
            "detected_language":  result.detected_language,
            "posted_at_ms":       raw.get("posted_at_ms", int(time.time() * 1000)),
            "received_at_ms":     raw.get("received_at_ms", int(time.time() * 1000)),
            "threat_intent_score": result.threat_intent_score,
            "source_credibility": credibility,
            "is_bot":             is_bot,
            "entities": [
                {
                    "text": e.text, "label": e.label,
                    "start_char": e.start_char, "end_char": e.end_char,
                    "confidence": e.confidence, "normalized": e.normalized,
                }
                for e in result.entities
            ],
            "keywords":       result.keywords,
            "url":            raw.get("url"),
            "hold_until_ms":  hold_until_ms,
            "schema_version": "1.0.0",
        }

        # Publish to socmint.processed.events
        self._producer.produce(
            topic="socmint.processed.events",
            key=platform.encode(),
            value=json.dumps(processed).encode(),
        )

        # Graph write if above threat threshold and not a bot
        if result.threat_intent_score >= self._threat_threshold and not is_bot:
            with self._driver.session() as session:
                self._write_to_graph(session, processed, now_iso)

    def _write_to_graph(self, session: Any, ev: dict, now_iso: str) -> None:
        post_id = ev["post_id"]
        platform = ev["platform"]
        event_entity_id = f"socmint_{platform.lower()}_{post_id}"

        ts_ms = ev.get("posted_at_ms", int(time.time() * 1000))
        ts_iso = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
        hold_ms = ev.get("hold_until_ms")
        hold_iso = (datetime.fromtimestamp(hold_ms / 1000, tz=timezone.utc).isoformat()
                    if hold_ms else None)

        # Upsert SocialMediaEvent
        session.run(MERGE_SOCIAL_EVENT, {
            "post_id":             post_id,
            "platform":            platform,
            "entity_id":           event_entity_id,
            "text_original":       ev["text_original"][:1000],
            "text_translated":     ev.get("text_translated"),
            "detected_language":   ev["detected_language"],
            "threat_intent_score": ev["threat_intent_score"],
            "source_credibility":  ev["source_credibility"],
            "is_bot":              ev["is_bot"],
            "valid_time_start":    ts_iso,
            "ingested_at":         now_iso,
            "hold_until":          hold_iso,
            "classification_level": self._classification,
        })

        # If high-confidence threat, also create ThreatPost
        if ev["threat_intent_score"] >= 0.65:
            session.run(MERGE_THREAT_POST, {
                "social_event_id":     event_entity_id,
                "entity_id":           f"tp_{event_entity_id}",
                "threat_intent_score": ev["threat_intent_score"],
                "source_credibility":  ev["source_credibility"],
                "platform":            platform,
                "valid_time_start":    ts_iso,
                "ingested_at":         now_iso,
                "classification_level": self._classification,
            })

        # Upsert author Person node
        author_id = ev.get("author_id")
        if author_id:
            person_entity_id = f"person_socmint_{platform.lower()}_{author_id}"
            session.run(MERGE_AUTHOR_PERSON, {
                "author_id":           author_id,
                "platform":            platform,
                "entity_id":           person_entity_id,
                "author_handle":       ev.get("author_handle"),
                "ingested_at":         now_iso,
                "classification_level": self._classification,
            })
            session.run(LINK_AUTHOR_TO_EVENT, {
                "person_entity_id": person_entity_id,
                "event_entity_id":  event_entity_id,
            })
