"""
Case Management API endpoints — 2.7.2
POST /api/v1/cases                       — create case from alert
GET  /api/v1/cases/{id}                  — get case with timeline
POST /api/v1/cases/{id}/evidence         — add evidence
POST /api/v1/cases/{id}/comments         — add comment
PUT  /api/v1/cases/{id}/status           — update status
GET  /api/v1/cases                       — list open cases (analyst workqueue)
"""

from __future__ import annotations

from typing import List, Literal, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from nsoe_api.auth.dependencies import require_analyst
from nsoe_api.db.neo4j_client import get_neo4j_session
from nsoe_intelligence_core.cases.case_management import CaseManagementService
from nsoe_intelligence_core.security.audit_log import AuditLogService
import os

logger = structlog.get_logger(__name__)
router = APIRouter()

_case_svc: Optional[CaseManagementService] = None
_audit_svc: Optional[AuditLogService] = None


def _get_case_svc() -> CaseManagementService:
    global _case_svc
    if _case_svc is None:
        _case_svc = CaseManagementService()
    return _case_svc


def _get_audit_svc() -> AuditLogService:
    global _audit_svc
    if _audit_svc is None:
        _audit_svc = AuditLogService(
            os.environ.get("NEO4J_URI",      "bolt://localhost:7687"),
            os.environ.get("NEO4J_USER",     "neo4j"),
            os.environ.get("NEO4J_PASSWORD", ""),
        )
    return _audit_svc


# ── Models ──────────────────────────────────────────────────

class CreateCaseRequest(BaseModel):
    alert_id: str
    alert_tier: str
    title: Optional[str] = None
    description: str = ""
    assigned_to: Optional[str] = None
    classification_level: str = "RESTRICTED"


class AddEvidenceRequest(BaseModel):
    evidence_type: Literal["GRAPH_ENTITY", "VIDEO_CLIP", "DOCUMENT", "INDICATOR"]
    uri: str
    description: str = ""


class AddCommentRequest(BaseModel):
    body: str


class UpdateStatusRequest(BaseModel):
    status: Literal["OPEN", "IN_PROGRESS", "CLOSED", "ESCALATED"]


# ── Routes ───────────────────────────────────────────────────

@router.post("", status_code=201)
async def create_case(
    req: CreateCaseRequest,
    user=Depends(require_analyst),
) -> dict:
    svc = _get_case_svc()
    case_id = svc.create_case_from_alert(
        alert_id=req.alert_id,
        alert_tier=req.alert_tier,
        created_by=user.sub,
        title=req.title,
        description=req.description,
        assigned_to=req.assigned_to,
        classification_level=req.classification_level,
    )
    _get_audit_svc().log(user.sub, "CREATE_CASE", "CASE", case_id)
    return {"case_id": case_id}


@router.get("")
async def list_cases(
    status: List[str] = Query(default=["OPEN", "IN_PROGRESS"]),
    assigned_to_me: bool = Query(default=False),
    user=Depends(require_analyst),
    session=Depends(get_neo4j_session),
) -> dict:
    cypher = """
    MATCH (c:Case) WHERE c.status IN $statuses
    """
    params: dict = {"statuses": status}
    if assigned_to_me:
        cypher += " AND c.assigned_to = $sub"
        params["sub"] = user.sub
    cypher += " RETURN c ORDER BY c.sla_deadline ASC LIMIT 100"
    result = session.run(cypher, **params)
    cases = [dict(r["c"]) for r in result]
    return {"cases": cases, "total": len(cases)}


@router.get("/{case_id}")
async def get_case(
    case_id: str,
    user=Depends(require_analyst),
) -> dict:
    svc = _get_case_svc()
    timeline = svc.get_timeline(case_id)
    if not timeline:
        raise HTTPException(status_code=404, detail="Case not found")
    _get_audit_svc().log(user.sub, "VIEW_CASE", "CASE", case_id)
    return timeline


@router.post("/{case_id}/evidence", status_code=201)
async def add_evidence(
    case_id: str,
    req: AddEvidenceRequest,
    user=Depends(require_analyst),
) -> dict:
    svc = _get_case_svc()
    evidence_id = svc.add_evidence(
        case_id=case_id,
        evidence_type=req.evidence_type,
        uri=req.uri,
        description=req.description,
        added_by=user.sub,
    )
    _get_audit_svc().log(user.sub, "ADD_EVIDENCE", "CASE", case_id)
    return {"evidence_id": evidence_id}


@router.post("/{case_id}/comments", status_code=201)
async def add_comment(
    case_id: str,
    req: AddCommentRequest,
    user=Depends(require_analyst),
) -> dict:
    svc = _get_case_svc()
    comment_id = svc.add_comment(case_id=case_id, body=req.body, author_sub=user.sub)
    return {"comment_id": comment_id}


@router.put("/{case_id}/status", status_code=204)
async def update_status(
    case_id: str,
    req: UpdateStatusRequest,
    user=Depends(require_analyst),
) -> None:
    svc = _get_case_svc()
    if req.status == "CLOSED":
        svc.close_case(case_id, user.sub)
    else:
        session_gen = get_neo4j_session()
        session = next(session_gen)
        session.run(
            "MATCH (c:Case {case_id:$id}) SET c.status=$s, c.updated_at=$now",
            id=case_id, s=req.status,
            now=__import__("datetime").datetime.now(
                __import__("datetime").timezone.utc).isoformat(),
        )
    _get_audit_svc().log(user.sub, f"CASE_STATUS_{req.status}", "CASE", case_id)
