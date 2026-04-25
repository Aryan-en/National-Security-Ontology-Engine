"""
Real-Time Risk Score Update Trigger — 2.5.7
When a new high-confidence event (confidence > 0.7) is written to the graph,
re-score the 1-hop neighbourhood of the affected entity using GraphSAGE.
Triggered via Kafka consumer on ontology.write.commands.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Dict, List, Optional, Set

import structlog
import torch
from confluent_kafka import Consumer, KafkaError
from neo4j import Driver, GraphDatabase

from nsoe_ml.models.graph_sage_risk import GraphSAGERiskModel
from nsoe_ml.features.graph_feature_extractor import GraphFeatureExtractor

logger = structlog.get_logger(__name__)

CONFIDENCE_THRESHOLD = float(os.environ.get("REALTIME_SCORE_THRESHOLD", "0.70"))

FETCH_1HOP_SUBGRAPH = """
MATCH (anchor:Person {entity_id: $entity_id})
OPTIONAL MATCH (anchor)-[r]-(neighbor:Person)
WITH anchor, collect(DISTINCT neighbor.entity_id) AS neighbor_ids
RETURN [anchor.entity_id] + neighbor_ids AS subgraph_ids
"""

UPDATE_RISK_SCORE = """
MATCH (p:Person {entity_id: $entity_id})
SET p.risk_score = $risk_score,
    p.risk_score_updated_at = $now
"""


class RealtimeRiskScorer:

    def __init__(
        self,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
        kafka_bootstrap: str,
        model_path: str,
    ) -> None:
        self._driver: Driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self._extractor = GraphFeatureExtractor(neo4j_uri, neo4j_user, neo4j_password)
        self._model = GraphSAGERiskModel()
        self._model.load_state_dict(torch.load(model_path, map_location="cpu"))
        self._model.eval()
        self._consumer: Optional[Consumer] = Consumer({
            "bootstrap.servers": kafka_bootstrap,
            "group.id": "nsoe-realtime-scorer",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        })
        self._consumer.subscribe(["ontology.write.commands"])
        self._running = False

    def start(self) -> None:
        self._running = True
        logger.info("realtime_scorer_started")
        self._loop()

    def stop(self) -> None:
        self._running = False
        self._consumer.close()
        self._extractor.close()
        self._driver.close()

    def _loop(self) -> None:
        while self._running:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                cmd = json.loads(msg.value().decode())
                self._handle_command(cmd)
            except Exception as e:
                logger.error("realtime_scorer_error", error=str(e))

    def _handle_command(self, cmd: dict) -> None:
        # Only trigger on high-confidence CCTV sightings
        if cmd.get("command_type") != "UPSERT_NODE":
            return
        node = cmd.get("node", {})
        if not node or node.get("label") != "Sighting":
            return
        props = node.get("properties", {})
        try:
            conf = float(props.get("detection_confidence", 0.0))
        except (TypeError, ValueError):
            return
        if conf < CONFIDENCE_THRESHOLD:
            return

        # Get anchor Person entity from the sighting
        track_id = props.get("track_id", "")
        if not track_id:
            return

        anchor_entity_id = self._get_person_for_track(track_id)
        if not anchor_entity_id:
            return

        self._rescore_neighborhood(anchor_entity_id)

    def _get_person_for_track(self, track_id: str) -> Optional[str]:
        with self._driver.session() as session:
            result = session.run(
                """
                MATCH (s:Sighting {track_id: $track_id})-[:PARTICIPATED_IN]-(p:Person)
                RETURN p.entity_id AS entity_id LIMIT 1
                """,
                track_id=track_id,
            )
            row = result.single()
            return row["entity_id"] if row else None

    def _rescore_neighborhood(self, entity_id: str) -> None:
        """Re-extract features for 1-hop subgraph and update scores."""
        with self._driver.session() as session:
            result = session.run(FETCH_1HOP_SUBGRAPH, entity_id=entity_id)
            row = result.single()
            subgraph_ids: List[str] = row["subgraph_ids"] if row else [entity_id]

        # Use full extractor for now; could be optimised to subgraph-only
        try:
            data = self._extractor.extract(include_labels=False)
        except Exception as e:
            logger.error("feature_extraction_failed", error=str(e))
            return

        scores = self._model.predict(data)
        id_to_score = dict(zip(data.entity_ids, scores.tolist()))

        now = __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ).isoformat()

        with self._driver.session() as session:
            for eid in subgraph_ids:
                if eid in id_to_score:
                    session.run(UPDATE_RISK_SCORE, {
                        "entity_id": eid,
                        "risk_score": float(id_to_score[eid]),
                        "now": now,
                    })

        logger.info("realtime_rescored", anchor=entity_id, neighborhood_size=len(subgraph_ids))
