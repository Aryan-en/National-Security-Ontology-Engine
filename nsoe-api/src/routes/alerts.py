"""
Alert management endpoints.
GET  /api/v1/alerts          — paginated active alert feed
GET  /api/v1/alerts/{id}     — single alert detail
POST /api/v1/alerts/{id}/acknowledge
POST /api/v1/alerts/{id}/close
POST /api/v1/alerts/{id}/escalate
WS   /api/v1/alerts/stream   — WebSocket live alert feed
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import AsyncGenerator, List, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect, status
from pydantic import BaseModel, Field

from nsoe_api.auth.dependencies import require_analyst
from nsoe_api.db.neo4j_client import get_neo4j_session
from nsoe_api.db.kafka_client import get_alert_consumer

logger = structlog.get_logger(__name__)
router = APIRouter()

# ============================================================
# Models
# ============================================================

class AlertResponse(BaseModel):
    alert_id: str
    alert_type: str
    alert_tier: str
    fused_confidence: float
    camera_id: Optional[str]
    geo_zone_id: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    involved_track_ids: List[str] = []
    involved_entity_ids: List[str] = []
    contributing_events: List[str] = []
    source_domains: List[str] = []
    triggered_at: str
    status: str  # ACTIVE | ACKNOWLEDGED | CLOSED | ESCALATED


class AlertPage(BaseModel):
    alerts: List[AlertResponse]
    total: int
    page: int
    page_size: int


# ============================================================
# Routes
# ============================================================

GET_ALERTS_CYPHER = """
MATCH (a:Alert)
WHERE a.status IN $statuses
RETURN a
ORDER BY a.triggered_at DESC
SKIP $skip LIMIT $limit
"""

COUNT_ALERTS_CYPHER = """
MATCH (a:Alert) WHERE a.status IN $statuses RETURN count(a) AS total
"""

GET_ALERT_CYPHER = """
MATCH (a:Alert {alert_id: $alert_id}) RETURN a
"""

UPDATE_ALERT_STATUS_CYPHER = """
MATCH (a:Alert {alert_id: $alert_id})
SET a.status = $status, a.updated_at = $updated_at, a.updated_by = $updated_by
RETURN a.alert_id AS alert_id
"""


@router.get("", response_model=AlertPage)
async def list_alerts(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    status: List[str] = Query(default=["ACTIVE"]),
    user=Depends(require_analyst),
    session=Depends(get_neo4j_session),
) -> AlertPage:
    skip = (page - 1) * page_size
    result = session.run(GET_ALERTS_CYPHER, statuses=status, skip=skip, limit=page_size)
    count_result = session.run(COUNT_ALERTS_CYPHER, statuses=status)
    total = count_result.single()["total"]
    alerts = [_row_to_alert(r["a"]) for r in result]
    return AlertPage(alerts=alerts, total=total, page=page, page_size=page_size)


@router.get("/{alert_id}", response_model=AlertResponse)
async def get_alert(
    alert_id: str,
    user=Depends(require_analyst),
    session=Depends(get_neo4j_session),
) -> AlertResponse:
    result = session.run(GET_ALERT_CYPHER, alert_id=alert_id)
    row = result.single()
    if not row:
        raise HTTPException(status_code=404, detail="Alert not found")
    return _row_to_alert(row["a"])


@router.post("/{alert_id}/acknowledge", status_code=204)
async def acknowledge_alert(
    alert_id: str,
    user=Depends(require_analyst),
    session=Depends(get_neo4j_session),
) -> None:
    _update_status(session, alert_id, "ACKNOWLEDGED", user.sub)


@router.post("/{alert_id}/close", status_code=204)
async def close_alert(
    alert_id: str,
    user=Depends(require_analyst),
    session=Depends(get_neo4j_session),
) -> None:
    _update_status(session, alert_id, "CLOSED", user.sub)


@router.post("/{alert_id}/escalate", status_code=204)
async def escalate_alert(
    alert_id: str,
    user=Depends(require_analyst),
    session=Depends(get_neo4j_session),
) -> None:
    _update_status(session, alert_id, "ESCALATED", user.sub)


@router.websocket("/stream")
async def alert_stream(websocket: WebSocket) -> None:
    """Stream live alerts from the alerts.active Kafka topic via WebSocket."""
    await websocket.accept()
    consumer = get_alert_consumer()
    try:
        while True:
            msg = consumer.poll(timeout=0.1)
            if msg and not msg.error():
                await websocket.send_text(msg.value().decode("utf-8"))
            await asyncio.sleep(0.05)
    except WebSocketDisconnect:
        pass
    finally:
        consumer.close()


def _update_status(session: object, alert_id: str, new_status: str, user_sub: str) -> None:
    result = session.run(
        UPDATE_ALERT_STATUS_CYPHER,
        alert_id=alert_id,
        status=new_status,
        updated_at=datetime.now(timezone.utc).isoformat(),
        updated_by=user_sub,
    )
    if not result.single():
        raise HTTPException(status_code=404, detail="Alert not found")


def _row_to_alert(node: dict) -> AlertResponse:
    return AlertResponse(
        alert_id=node.get("alert_id", ""),
        alert_type=node.get("alert_type", ""),
        alert_tier=node.get("alert_tier", ""),
        fused_confidence=float(node.get("fused_confidence", 0.0)),
        camera_id=node.get("camera_id"),
        geo_zone_id=node.get("geo_zone_id"),
        latitude=node.get("latitude"),
        longitude=node.get("longitude"),
        involved_track_ids=node.get("involved_track_ids", []),
        involved_entity_ids=node.get("involved_entity_ids", []),
        contributing_events=node.get("contributing_events", []),
        source_domains=node.get("source_domains", ["CCTV"]),
        triggered_at=node.get("triggered_at", ""),
        status=node.get("status", "ACTIVE"),
    )
