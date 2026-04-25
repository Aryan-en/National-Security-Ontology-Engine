"""
Graph Feature Extraction Pipeline — 2.5.2
Pulls node features from Neo4j and constructs a PyTorch Geometric Data object.

Node features (8-dim per spec):
  [0] entity_type_encoded  (Person=0.0, ThreatActor=0.5, Org=0.75, Other=1.0)
  [1] source_credibility   (float, default 0.5)
  [2] connection_count     (log1p normalized)
  [3] historical_incident_count (log1p normalized)
  [4] geographic_centrality (betweenness centrality from graph, [0,1])
  [5] days_since_last_seen  (log1p normalized, capped at 365)
  [6] watchlist_flag        (0.0 or 1.0)
  [7] cross_domain_count   (CCTV + CYBER + SOCMINT appearances, log1p normalized)
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import structlog
import torch
from torch_geometric.data import Data
from neo4j import Driver, GraphDatabase

logger = structlog.get_logger(__name__)

ENTITY_TYPE_ENCODING = {
    "Person":       0.0,
    "ThreatActor":  0.5,
    "Organization": 0.75,
    "ThreatGroup":  1.0,
}

FETCH_PERSONS = """
MATCH (p:Person)
OPTIONAL MATCH (p)-[r]-()
WITH p,
     count(r)                                          AS connection_count,
     coalesce(p.risk_score, 0.0)                      AS risk_score,
     coalesce(p.source_credibility, 0.5)              AS source_credibility,
     coalesce(p.watchlist_flag, false)                 AS watchlist_flag,
     coalesce(p.historical_incident_count, 0)          AS incident_count,
     coalesce(p.cross_domain_count, 0)                 AS cross_domain_count,
     p.last_seen_ms                                     AS last_seen_ms,
     labels(p)                                          AS entity_labels
RETURN p.entity_id AS entity_id, risk_score, source_credibility,
       watchlist_flag, incident_count, cross_domain_count,
       connection_count, last_seen_ms, entity_labels
ORDER BY p.entity_id
"""

FETCH_RELATIONSHIPS = """
MATCH (a:Person)-[r]-(b:Person)
WHERE type(r) IN ['COMMUNICATED_WITH','CO_LOCATED_WITH','SAME_AS','MEMBER_OF']
RETURN a.entity_id AS src, b.entity_id AS tgt, type(r) AS rel_type,
       coalesce(r.confidence, 0.5) AS confidence
"""


class GraphFeatureExtractor:
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str) -> None:
        self._driver: Driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    def extract(self, include_labels: bool = False) -> Data:
        """
        Returns a PyTorch Geometric Data object with node features + edge_index.
        If include_labels=True, also populates data.y with risk labels.
        """
        with self._driver.session() as session:
            node_rows = list(session.run(FETCH_PERSONS))
            edge_rows = list(session.run(FETCH_RELATIONSHIPS))

        if not node_rows:
            logger.warning("no_persons_found_in_graph")
            return Data(
                x=torch.zeros((1, 8)),
                edge_index=torch.zeros((2, 0), dtype=torch.long),
            )

        entity_ids = [r["entity_id"] for r in node_rows]
        id_to_idx  = {eid: i for i, eid in enumerate(entity_ids)}
        now_ms = int(time.time() * 1000)

        features = []
        labels   = []
        for row in node_rows:
            labels_list = row["entity_labels"] or []
            etype = 0.0
            for lbl in labels_list:
                if lbl in ENTITY_TYPE_ENCODING:
                    etype = ENTITY_TYPE_ENCODING[lbl]
                    break

            last_ms = row["last_seen_ms"] or now_ms
            days_since = (now_ms - last_ms) / (1000 * 86400)

            feat = [
                etype,
                float(row["source_credibility"] or 0.5),
                _log_norm(row["connection_count"] or 0, cap=500),
                _log_norm(row["incident_count"] or 0, cap=50),
                0.0,   # geographic_centrality — placeholder (computed by networkx batch)
                _log_norm(min(days_since, 365), cap=365),
                1.0 if row["watchlist_flag"] else 0.0,
                _log_norm(row["cross_domain_count"] or 0, cap=10),
            ]
            features.append(feat)
            labels.append(float(row["risk_score"] or 0.0))

        x = torch.tensor(features, dtype=torch.float)
        y = torch.tensor(labels,   dtype=torch.float)

        # Build edge_index
        src_list, tgt_list = [], []
        for row in edge_rows:
            s = id_to_idx.get(row["src"])
            t = id_to_idx.get(row["tgt"])
            if s is not None and t is not None:
                src_list.append(s)
                tgt_list.append(t)
                src_list.append(t)  # undirected
                tgt_list.append(s)

        if src_list:
            edge_index = torch.tensor([src_list, tgt_list], dtype=torch.long)
        else:
            edge_index = torch.zeros((2, 0), dtype=torch.long)

        data = Data(x=x, edge_index=edge_index, y=y)
        data.entity_ids = entity_ids
        logger.info("features_extracted", nodes=x.size(0), edges=edge_index.size(1))
        return data

    def close(self) -> None:
        self._driver.close()


def _log_norm(value: float, cap: float = 100.0) -> float:
    """Log1p normalize a positive value, capped at `cap`."""
    return float(np.log1p(min(value, cap)) / np.log1p(cap))
