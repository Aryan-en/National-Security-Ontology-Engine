"""
Entity search endpoint.
GET /api/v1/entities/search?q=...&type=...&min_risk=...
GET /api/v1/entities/{entity_id}
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from nsoe_api.auth.dependencies import require_analyst
from nsoe_api.db.elasticsearch_client import get_es_client

logger = structlog.get_logger(__name__)
router = APIRouter()


class EntityResult(BaseModel):
    entity_id: str
    entity_type: str
    name: Optional[str] = None
    risk_score: Optional[float] = None
    confidence_score: Optional[float] = None
    watchlist_flag: bool = False
    classification_level: str = "RESTRICTED"
    source_domain: Optional[str] = None
    last_seen: Optional[str] = None
    highlight: Dict[str, List[str]] = {}


class SearchResponse(BaseModel):
    results: List[EntityResult]
    total: int
    query: str


@router.get("/search", response_model=SearchResponse)
async def search_entities(
    q: str = Query(min_length=2, max_length=256),
    entity_type: Optional[str] = Query(default=None),
    min_risk: float = Query(default=0.0, ge=0.0, le=1.0),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    user=Depends(require_analyst),
    es=Depends(get_es_client),
) -> SearchResponse:
    must = [{"multi_match": {"query": q, "fields": ["name^3", "alias", "ioc_value", "track_id"]}}]
    if entity_type:
        must.append({"term": {"entity_type": entity_type}})
    if min_risk > 0.0:
        must.append({"range": {"risk_score": {"gte": min_risk}}})

    body = {
        "query": {"bool": {"must": must}},
        "from": (page - 1) * page_size,
        "size": page_size,
        "highlight": {
            "fields": {"name": {}, "alias": {}, "ioc_value": {}},
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"],
        },
    }

    resp = es.search(index="nsoe_entities", body=body)
    hits = resp["hits"]["hits"]
    total = resp["hits"]["total"]["value"]

    results = []
    for hit in hits:
        src = hit["_source"]
        results.append(EntityResult(
            entity_id=src.get("entity_id", hit["_id"]),
            entity_type=src.get("entity_type", ""),
            name=src.get("name"),
            risk_score=src.get("risk_score"),
            confidence_score=src.get("confidence_score"),
            watchlist_flag=src.get("watchlist_flag", False),
            classification_level=src.get("classification_level", "RESTRICTED"),
            source_domain=src.get("source_domain"),
            last_seen=src.get("last_seen"),
            highlight=hit.get("highlight", {}),
        ))

    return SearchResponse(results=results, total=total, query=q)


@router.get("/{entity_id}", response_model=EntityResult)
async def get_entity(
    entity_id: str,
    user=Depends(require_analyst),
    es=Depends(get_es_client),
) -> EntityResult:
    try:
        resp = es.get(index="nsoe_entities", id=entity_id)
        src = resp["_source"]
        return EntityResult(
            entity_id=src.get("entity_id", entity_id),
            entity_type=src.get("entity_type", ""),
            name=src.get("name"),
            risk_score=src.get("risk_score"),
            confidence_score=src.get("confidence_score"),
            watchlist_flag=src.get("watchlist_flag", False),
            classification_level=src.get("classification_level", "RESTRICTED"),
            source_domain=src.get("source_domain"),
            last_seen=src.get("last_seen"),
        )
    except Exception:
        raise HTTPException(status_code=404, detail="Entity not found")
