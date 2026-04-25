"""
STIX 2.1 Ingestor — 2.1.1 / 2.1.6
Consumes STIX 2.1 bundles (from files, TAXII feeds, or Kafka),
normalises them to the NSOE ontology, and writes nodes to Neo4j.
Node types: CyberEvent, Indicator, ThreatActor, Malware, Campaign
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from neo4j import Driver, GraphDatabase

logger = structlog.get_logger(__name__)

# ── Neo4j Cypher ─────────────────────────────────────────────

MERGE_THREAT_ACTOR = """
MERGE (a:ThreatActor:Agent {stix_id: $stix_id})
ON CREATE SET
    a.entity_id          = $entity_id,
    a.name               = $name,
    a.aliases            = $aliases,
    a.threat_actor_types = $types,
    a.sophistication     = $sophistication,
    a.resource_level     = $resource_level,
    a.primary_motivation = $motivation,
    a.confidence_score   = $confidence,
    a.source_domain      = 'CYBER',
    a.ingested_at        = $now,
    a.classification_level = $classification
ON MATCH SET
    a.name               = $name,
    a.confidence_score   = $confidence
RETURN a.entity_id AS entity_id
"""

MERGE_INDICATOR = """
MERGE (i:Indicator:CyberArtifact {stix_id: $stix_id})
ON CREATE SET
    i.entity_id          = $entity_id,
    i.ioc_type           = $ioc_type,
    i.ioc_value          = $ioc_value,
    i.pattern            = $pattern,
    i.pattern_type       = $pattern_type,
    i.confidence_score   = $confidence,
    i.valid_time_start   = $valid_from,
    i.valid_time_end     = $valid_until,
    i.tlp_marking        = $tlp,
    i.source_domain      = 'CYBER',
    i.ingested_at        = $now,
    i.classification_level = $classification
RETURN i.entity_id AS entity_id
"""

MERGE_MALWARE = """
MERGE (m:Malware:CyberArtifact {stix_id: $stix_id})
ON CREATE SET
    m.entity_id       = $entity_id,
    m.name            = $name,
    m.malware_types   = $types,
    m.is_family       = $is_family,
    m.confidence_score= $confidence,
    m.source_domain   = 'CYBER',
    m.ingested_at     = $now,
    m.classification_level = $classification
RETURN m.entity_id AS entity_id
"""

MERGE_CAMPAIGN = """
MERGE (c:ThreatCampaign {stix_id: $stix_id})
ON CREATE SET
    c.entity_id       = $entity_id,
    c.name            = $name,
    c.description     = $description,
    c.first_seen      = $first_seen,
    c.last_seen       = $last_seen,
    c.confidence_score= $confidence,
    c.source_domain   = 'CYBER',
    c.ingested_at     = $now,
    c.classification_level = $classification
RETURN c.entity_id AS entity_id
"""

MERGE_RELATIONSHIP = """
MATCH (a {stix_id: $src_id})
MATCH (b {stix_id: $tgt_id})
MERGE (a)-[r:{rel_type} {{stix_rel_id: $rel_id}}]->(b)
ON CREATE SET r.created = $now, r.confidence = $confidence
"""

INDEXES = [
    "CREATE INDEX cyber_stix_id IF NOT EXISTS FOR (n:ThreatActor) ON (n.stix_id)",
    "CREATE INDEX indicator_stix_id IF NOT EXISTS FOR (n:Indicator) ON (n.stix_id)",
    "CREATE INDEX indicator_ioc_value IF NOT EXISTS FOR (n:Indicator) ON (n.ioc_value)",
    "CREATE INDEX malware_stix_id IF NOT EXISTS FOR (n:Malware) ON (n.stix_id)",
    "CREATE INDEX campaign_stix_id IF NOT EXISTS FOR (n:ThreatCampaign) ON (n.stix_id)",
]

# Maps STIX indicator pattern to ioc_type
PATTERN_TYPE_MAP = {
    "ipv4-addr": "ip", "ipv6-addr": "ip",
    "domain-name": "domain", "url": "url",
    "file:hashes": "hash", "email-addr": "email",
}


def _extract_ioc(pattern: str) -> tuple[str, str]:
    """Parse STIX pattern string to (ioc_type, ioc_value). Best-effort."""
    import re
    for stix_type, ioc_type in PATTERN_TYPE_MAP.items():
        m = re.search(rf"\[{re.escape(stix_type)}:value\s*=\s*'([^']+)'", pattern)
        if m:
            return ioc_type, m.group(1)
        m = re.search(rf'\[{re.escape(stix_type)}:value\s*=\s*"([^"]+)"', pattern)
        if m:
            return ioc_type, m.group(1)
    return "unknown", pattern[:200]


def _tlp(obj: Dict) -> str:
    for marking in obj.get("object_marking_refs", []):
        if "white" in marking.lower():  return "WHITE"
        if "green" in marking.lower():  return "GREEN"
        if "amber" in marking.lower():  return "AMBER"
        if "red" in marking.lower():    return "RED"
    return "WHITE"


class StixIngestor:
    def __init__(
        self,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
        classification_level: str = "RESTRICTED",
    ) -> None:
        self._driver: Driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self._classification = classification_level
        with self._driver.session() as s:
            for idx in INDEXES:
                try: s.run(idx)
                except Exception: pass

    def ingest_bundle(self, bundle: Dict) -> int:
        """Ingest a STIX 2.1 bundle dict. Returns count of objects processed."""
        objects = bundle.get("objects", [])
        now = datetime.now(timezone.utc).isoformat()
        id_map: Dict[str, str] = {}   # stix_id → entity_id

        # First pass: nodes
        with self._driver.session() as session:
            for obj in objects:
                eid = self._ingest_object(session, obj, now)
                if eid:
                    id_map[obj.get("id", "")] = eid

        # Second pass: relationships
        with self._driver.session() as session:
            for obj in objects:
                if obj.get("type") == "relationship":
                    self._ingest_relationship(session, obj, now)

        logger.info("stix_bundle_ingested", count=len(objects))
        return len(objects)

    def ingest_file(self, path: str) -> int:
        with open(path) as f:
            bundle = json.load(f)
        return self.ingest_bundle(bundle)

    def _ingest_object(self, session: Any, obj: Dict, now: str) -> Optional[str]:
        t = obj.get("type", "")
        conf = float(obj.get("confidence", 50)) / 100.0
        stix_id = obj.get("id", str(uuid.uuid4()))
        eid = f"cyber_{stix_id.replace(':', '_').replace('--', '_')}"

        if t == "threat-actor":
            session.run(MERGE_THREAT_ACTOR, {
                "stix_id": stix_id, "entity_id": eid,
                "name": obj.get("name", "Unknown"),
                "aliases": obj.get("aliases", []),
                "types": obj.get("threat_actor_types", []),
                "sophistication": obj.get("sophistication", ""),
                "resource_level": obj.get("resource_level", ""),
                "motivation": obj.get("primary_motivation", ""),
                "confidence": conf, "now": now,
                "classification": self._classification,
            })
            return eid

        if t == "indicator":
            pattern = obj.get("pattern", "")
            ioc_type, ioc_value = _extract_ioc(pattern)
            session.run(MERGE_INDICATOR, {
                "stix_id": stix_id, "entity_id": eid,
                "ioc_type": ioc_type, "ioc_value": ioc_value,
                "pattern": pattern,
                "pattern_type": obj.get("pattern_type", "stix"),
                "confidence": conf,
                "valid_from": obj.get("valid_from", now),
                "valid_until": obj.get("valid_until"),
                "tlp": _tlp(obj), "now": now,
                "classification": self._classification,
            })
            return eid

        if t == "malware":
            session.run(MERGE_MALWARE, {
                "stix_id": stix_id, "entity_id": eid,
                "name": obj.get("name", "Unknown"),
                "types": obj.get("malware_types", []),
                "is_family": obj.get("is_family", False),
                "confidence": conf, "now": now,
                "classification": self._classification,
            })
            return eid

        if t == "campaign":
            session.run(MERGE_CAMPAIGN, {
                "stix_id": stix_id, "entity_id": eid,
                "name": obj.get("name", "Unknown"),
                "description": obj.get("description", ""),
                "first_seen": obj.get("first_seen", now),
                "last_seen": obj.get("last_seen"),
                "confidence": conf, "now": now,
                "classification": self._classification,
            })
            return eid

        return None   # unsupported type

    def _ingest_relationship(self, session: Any, obj: Dict, now: str) -> None:
        rel_type = obj.get("relationship_type", "related_to").upper().replace("-", "_")
        cypher = MERGE_RELATIONSHIP.replace("{rel_type}", rel_type)
        try:
            session.run(cypher, {
                "src_id": obj.get("source_ref", ""),
                "tgt_id": obj.get("target_ref", ""),
                "rel_id": obj.get("id", str(uuid.uuid4())),
                "confidence": float(obj.get("confidence", 50)) / 100.0,
                "now": now,
            })
        except Exception as e:
            logger.debug("rel_skip", error=str(e))

    def close(self) -> None:
        self._driver.close()
