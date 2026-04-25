"""
Probabilistic Entity Resolution — 2.4.1 / 2.4.4 / 2.4.5
Fellegi-Sunter probabilistic record linkage model.
Links candidate Person pairs across CCTV, CYBER, SOCMINT domains.

Features used:
  - Name similarity (Jaro-Winkler on normalised names)
  - Location overlap (same geo_zone_id)
  - Temporal proximity (sighting times within window)
  - Biometric match score (face embedding cosine similarity)
"""

from __future__ import annotations

import math
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import structlog
from neo4j import Driver, GraphDatabase

from nsoe_intelligence_core.resolution.name_normalizer import name_similarity

logger = structlog.get_logger(__name__)

# Precision / recall targets (2.4.5):
# >0.92 precision, >0.85 recall on test dataset

MATCH_THRESHOLD   = float(os.environ.get("ER_MATCH_THRESHOLD",    "0.85"))
NOMATCH_THRESHOLD = float(os.environ.get("ER_NOMATCH_THRESHOLD",  "0.30"))


@dataclass
class EntityRecord:
    entity_id: str
    domain: str
    name: Optional[str] = None
    aliases: List[str] = field(default_factory=list)
    geo_zone_id: Optional[str] = None
    last_seen_ms: Optional[int] = None
    face_embedding: Optional[np.ndarray] = None
    source_credibility: float = 1.0


@dataclass
class LinkDecision:
    entity_id_a: str
    entity_id_b: str
    match_score: float
    decision: str       # MATCH | POSSIBLE | NOMATCH
    feature_scores: Dict[str, float]


MERGE_ENTITY_LINK = """
MATCH (a {entity_id: $id_a})
MATCH (b {entity_id: $id_b})
MERGE (a)-[r:SAME_AS {link_id: $link_id}]-(b)
ON CREATE SET r.match_score = $score, r.method = 'FELLEGI_SUNTER',
              r.created_at = $now
"""

FETCH_CANDIDATE_PERSONS = """
MATCH (p:Person)
WHERE p.entity_id <> $anchor_id
  AND p.source_domain <> $anchor_domain
RETURN p.entity_id AS entity_id,
       p.name AS name,
       coalesce(p.aliases, []) AS aliases,
       p.geo_zone_id AS geo_zone_id,
       p.source_domain AS domain,
       p.last_seen_ms AS last_seen_ms
LIMIT 500
"""


class FellegiSunterLinker:
    """
    Fellegi-Sunter probabilistic record linkage.
    m-probabilities (P(feature agrees | same entity)) and
    u-probabilities (P(feature agrees | different entities)) are estimated
    from a small labelled set; defaults below are priors.
    """

    # m-probs: probability feature agrees given TRUE match
    M = {
        "name":    0.92,
        "location": 0.80,
        "temporal": 0.75,
        "biometric": 0.98,
    }
    # u-probs: probability feature agrees given NON-match (base rates)
    U = {
        "name":    0.02,
        "location": 0.15,
        "temporal": 0.30,
        "biometric": 0.001,
    }

    # Feature weights (log likelihood ratios)
    @property
    def weights(self) -> Dict[str, float]:
        return {
            k: math.log(self.M[k] / self.U[k])
            for k in self.M
        }

    def score_pair(self, a: EntityRecord, b: EntityRecord,
                   temporal_window_ms: int = 86_400_000) -> LinkDecision:
        feature_scores: Dict[str, float] = {}
        log_score = 0.0

        # 1. Name similarity
        if a.name and b.name:
            name_sim = name_similarity(a.name, b.name)
            feature_scores["name"] = name_sim
            if name_sim > 0.85:
                log_score += self.weights["name"]
            elif name_sim < 0.3:
                log_score += math.log(
                    (1 - self.M["name"]) / (1 - self.U["name"])
                )
        else:
            feature_scores["name"] = 0.0

        # 2. Location overlap
        loc_match = (a.geo_zone_id and b.geo_zone_id and
                     a.geo_zone_id == b.geo_zone_id)
        feature_scores["location"] = 1.0 if loc_match else 0.0
        if loc_match:
            log_score += self.weights["location"]

        # 3. Temporal proximity
        if a.last_seen_ms and b.last_seen_ms:
            delta_ms = abs(a.last_seen_ms - b.last_seen_ms)
            temporal_match = delta_ms <= temporal_window_ms
            feature_scores["temporal"] = 1.0 if temporal_match else 0.0
            if temporal_match:
                log_score += self.weights["temporal"]
        else:
            feature_scores["temporal"] = 0.0

        # 4. Biometric / face embedding cosine similarity
        if a.face_embedding is not None and b.face_embedding is not None:
            cos_sim = _cosine_similarity(a.face_embedding, b.face_embedding)
            feature_scores["biometric"] = float(cos_sim)
            if cos_sim > 0.85:
                log_score += self.weights["biometric"]
            elif cos_sim < 0.40:
                log_score += math.log(
                    (1 - self.M["biometric"]) / (1 - self.U["biometric"])
                )
        else:
            feature_scores["biometric"] = 0.0

        # Convert log-odds to probability
        match_prob = _sigmoid(log_score)

        if match_prob >= MATCH_THRESHOLD:
            decision = "MATCH"
        elif match_prob <= NOMATCH_THRESHOLD:
            decision = "NOMATCH"
        else:
            decision = "POSSIBLE"

        return LinkDecision(
            entity_id_a=a.entity_id,
            entity_id_b=b.entity_id,
            match_score=round(match_prob, 4),
            decision=decision,
            feature_scores=feature_scores,
        )


class EntityResolutionService:
    """
    Runs the Fellegi-Sunter linker against candidate pairs from Neo4j.
    Writes SAME_AS relationships for confirmed matches.
    """

    def __init__(
        self,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
    ) -> None:
        self._driver: Driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self._linker = FellegiSunterLinker()

    def resolve_person(self, anchor: EntityRecord) -> List[LinkDecision]:
        """Find and link entities matching the anchor."""
        decisions: List[LinkDecision] = []
        now_iso = __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ).isoformat()

        with self._driver.session() as session:
            result = session.run(
                FETCH_CANDIDATE_PERSONS,
                anchor_id=anchor.entity_id,
                anchor_domain=anchor.domain,
            )
            candidates = [
                EntityRecord(
                    entity_id=r["entity_id"],
                    domain=r["domain"] or "UNKNOWN",
                    name=r["name"],
                    aliases=r["aliases"] or [],
                    geo_zone_id=r["geo_zone_id"],
                    last_seen_ms=r["last_seen_ms"],
                )
                for r in result
            ]

        for candidate in candidates:
            decision = self._linker.score_pair(anchor, candidate)
            decisions.append(decision)
            if decision.decision == "MATCH":
                with self._driver.session() as session:
                    session.run(MERGE_ENTITY_LINK, {
                        "id_a":    anchor.entity_id,
                        "id_b":    candidate.entity_id,
                        "link_id": str(uuid.uuid4()),
                        "score":   decision.match_score,
                        "now":     now_iso,
                    })
                logger.info(
                    "entity_linked",
                    a=anchor.entity_id, b=candidate.entity_id,
                    score=decision.match_score,
                )

        return decisions

    def close(self) -> None:
        self._driver.close()


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    a_n = a / (np.linalg.norm(a) + 1e-9)
    b_n = b / (np.linalg.norm(b) + 1e-9)
    return float(np.dot(a_n, b_n))


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))
