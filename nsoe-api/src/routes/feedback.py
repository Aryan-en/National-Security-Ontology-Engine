"""
Analyst feedback endpoint.
POST /api/v1/feedback  — submit TP/FP verdict on an alert
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Literal

import structlog
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from nsoe_api.auth.dependencies import require_analyst
from nsoe_api.db.neo4j_client import get_neo4j_session

logger = structlog.get_logger(__name__)
router = APIRouter()


class FeedbackRequest(BaseModel):
    alert_id: str
    verdict: Literal["TRUE_POSITIVE", "FALSE_POSITIVE"]
    notes: str = ""


STORE_FEEDBACK_CYPHER = """
MATCH (a:Alert {alert_id: $alert_id})
SET a.analyst_verdict    = $verdict,
    a.feedback_notes     = $notes,
    a.feedback_at        = $feedback_at,
    a.feedback_by        = $feedback_by
WITH a
MERGE (f:AnalystFeedback {alert_id: $alert_id})
SET f.verdict    = $verdict,
    f.notes      = $notes,
    f.feedback_at = $feedback_at,
    f.analyst_id  = $feedback_by
RETURN a.alert_id AS alert_id
"""

CREATE_SUPPRESSION_RULE_CYPHER = """
MERGE (sr:SuppressionRule {pattern_key: $pattern_key})
ON CREATE SET sr.created_at = $now, sr.created_by = $analyst_id,
              sr.alert_type = $alert_type, sr.geo_zone_id = $geo_zone_id,
              sr.active = true
ON MATCH SET sr.updated_at = $now
"""


@router.post("", status_code=204)
async def submit_feedback(
    req: FeedbackRequest,
    user=Depends(require_analyst),
    session=Depends(get_neo4j_session),
) -> None:
    now = datetime.now(timezone.utc).isoformat()

    result = session.run(
        STORE_FEEDBACK_CYPHER,
        alert_id=req.alert_id,
        verdict=req.verdict,
        notes=req.notes,
        feedback_at=now,
        feedback_by=user.sub,
    )
    if not result.single():
        raise HTTPException(status_code=404, detail="Alert not found")

    logger.info(
        "feedback_received",
        alert_id=req.alert_id,
        verdict=req.verdict,
        analyst=user.sub,
    )

    # If FP, create a suppression rule to reduce future noise
    if req.verdict == "FALSE_POSITIVE":
        alert_result = session.run(
            "MATCH (a:Alert {alert_id: $id}) RETURN a.alert_type AS t, a.geo_zone_id AS z",
            id=req.alert_id,
        )
        row = alert_result.single()
        if row:
            pattern_key = f"{row['t']}_{row['z']}"
            session.run(
                CREATE_SUPPRESSION_RULE_CYPHER,
                pattern_key=pattern_key,
                now=now,
                analyst_id=user.sub,
                alert_type=row["t"],
                geo_zone_id=row["z"],
            )
            logger.info("suppression_rule_created", pattern_key=pattern_key)
