"""
CaseManagementService — 2.7.1 / 2.7.2
Provides:
  - Alert CRUD with SLA tracking
  - Case creation from alert
  - Evidence linking (graph entities, documents, video clips)
  - Multi-analyst collaboration (case assignments, comments)
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import structlog
from neo4j import Driver, GraphDatabase

logger = structlog.get_logger(__name__)

NEO4J_URI      = os.environ.get("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.environ.get("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "")

# SLA targets (minutes) per alert tier
SLA_MINUTES = {
    "ESCALATION":     15,
    "SOFT_ALERT":     60,
    "ANALYST_REVIEW": 480,
}

CREATE_CASE = """
CREATE (c:Case {
    case_id:          $case_id,
    title:            $title,
    description:      $description,
    status:           'OPEN',
    priority:         $priority,
    created_by:       $created_by,
    created_at:       $created_at,
    updated_at:       $created_at,
    assigned_to:      $assigned_to,
    sla_deadline:     $sla_deadline,
    classification_level: $classification_level
})
RETURN c.case_id AS case_id
"""

LINK_ALERT_TO_CASE = """
MATCH (c:Case   {case_id:  $case_id})
MATCH (a:Alert  {alert_id: $alert_id})
MERGE (c)-[:INCLUDES_ALERT]->(a)
"""

LINK_EVIDENCE = """
MATCH (c:Case {case_id: $case_id})
MERGE (e:Evidence {evidence_id: $evidence_id})
ON CREATE SET e.evidence_type = $evidence_type,
              e.uri           = $uri,
              e.description   = $description,
              e.added_by      = $added_by,
              e.added_at      = $added_at
MERGE (c)-[:HAS_EVIDENCE]->(e)
"""

ADD_CASE_COMMENT = """
MATCH (c:Case {case_id: $case_id})
CREATE (cm:CaseComment {
    comment_id: $comment_id,
    body:       $body,
    author_sub: $author_sub,
    created_at: $created_at
})
CREATE (c)-[:HAS_COMMENT]->(cm)
"""

UPDATE_CASE_STATUS = """
MATCH (c:Case {case_id: $case_id})
SET c.status     = $status,
    c.updated_at = $now,
    c.closed_by  = $user_sub
"""

FETCH_CASE_TIMELINE = """
MATCH (c:Case {case_id: $case_id})
OPTIONAL MATCH (c)-[:INCLUDES_ALERT]->(a:Alert)
OPTIONAL MATCH (c)-[:HAS_EVIDENCE]->(e:Evidence)
OPTIONAL MATCH (c)-[:HAS_COMMENT]->(cm:CaseComment)
RETURN c,
       collect(DISTINCT a{.alert_id, .alert_type, .triggered_at, .alert_tier}) AS alerts,
       collect(DISTINCT e{.evidence_id, .evidence_type, .uri, .added_at})      AS evidence,
       collect(DISTINCT cm{.comment_id, .body, .author_sub, .created_at})       AS comments
"""


class CaseManagementService:

    def __init__(
        self,
        neo4j_uri: str = NEO4J_URI,
        neo4j_user: str = NEO4J_USER,
        neo4j_password: str = NEO4J_PASSWORD,
    ) -> None:
        self._driver: Driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    def create_case_from_alert(
        self,
        alert_id: str,
        alert_tier: str,
        created_by: str,
        title: Optional[str] = None,
        description: str = "",
        assigned_to: Optional[str] = None,
        classification_level: str = "RESTRICTED",
    ) -> str:
        case_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        sla_mins = SLA_MINUTES.get(alert_tier, 480)
        sla_deadline = (now + timedelta(minutes=sla_mins)).isoformat()

        with self._driver.session() as session:
            session.run(CREATE_CASE, {
                "case_id":              case_id,
                "title":                title or f"Case from alert {alert_id[:8]}",
                "description":          description,
                "priority":             alert_tier,
                "created_by":           created_by,
                "created_at":           now_iso,
                "assigned_to":          assigned_to or created_by,
                "sla_deadline":         sla_deadline,
                "classification_level": classification_level,
            })
            session.run(LINK_ALERT_TO_CASE, {
                "case_id": case_id, "alert_id": alert_id,
            })

        logger.info("case_created", case_id=case_id, alert_id=alert_id, priority=alert_tier)
        return case_id

    def add_evidence(
        self,
        case_id: str,
        evidence_type: str,   # GRAPH_ENTITY | VIDEO_CLIP | DOCUMENT | INDICATOR
        uri: str,
        description: str,
        added_by: str,
    ) -> str:
        evidence_id = str(uuid.uuid4())
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._driver.session() as session:
            session.run(LINK_EVIDENCE, {
                "case_id": case_id, "evidence_id": evidence_id,
                "evidence_type": evidence_type, "uri": uri,
                "description": description, "added_by": added_by,
                "added_at": now_iso,
            })
        return evidence_id

    def add_comment(self, case_id: str, body: str, author_sub: str) -> str:
        comment_id = str(uuid.uuid4())
        with self._driver.session() as session:
            session.run(ADD_CASE_COMMENT, {
                "case_id": case_id, "comment_id": comment_id,
                "body": body, "author_sub": author_sub,
                "created_at": datetime.now(timezone.utc).isoformat(),
            })
        return comment_id

    def close_case(self, case_id: str, user_sub: str) -> None:
        with self._driver.session() as session:
            session.run(UPDATE_CASE_STATUS, {
                "case_id": case_id, "status": "CLOSED",
                "now": datetime.now(timezone.utc).isoformat(),
                "user_sub": user_sub,
            })

    def get_timeline(self, case_id: str) -> Dict[str, Any]:
        with self._driver.session() as session:
            result = session.run(FETCH_CASE_TIMELINE, case_id=case_id)
            row = result.single()
            if not row:
                return {}
            c = dict(row["c"])
            return {
                "case":     c,
                "alerts":   row["alerts"],
                "evidence": row["evidence"],
                "comments": row["comments"],
            }

    def close(self) -> None:
        self._driver.close()
