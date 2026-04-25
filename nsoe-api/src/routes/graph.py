"""
Graph query endpoint.
POST /api/v1/graph/neighborhood  — get N-hop neighborhood of an entity
POST /api/v1/graph/path          — shortest path between two entities
GET  /api/v1/graph/cypher        — restricted Cypher query (read-only)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from nsoe_api.auth.dependencies import require_analyst
from nsoe_api.db.neo4j_client import get_neo4j_session

logger = structlog.get_logger(__name__)
router = APIRouter()

# Allowed relationship types for graph traversal (allowlist for security)
ALLOWED_REL_TYPES = {
    "SIGHTED_AT", "PARTICIPATED_IN", "COMMUNICATED_WITH", "MEMBER_OF",
    "ASSOCIATED_CAMPAIGN", "CORROBORATED_BY", "LOCATED_AT", "USES",
    "TARGETS", "OPERATES_INFRASTRUCTURE", "HAS_INDICATOR",
}


class NeighborhoodRequest(BaseModel):
    entity_id: str
    hops: int = Field(default=1, ge=1, le=3)
    rel_types: Optional[List[str]] = None  # None = all allowed


class GraphNode(BaseModel):
    id: str
    labels: List[str]
    properties: Dict[str, Any]


class GraphEdge(BaseModel):
    source: str
    target: str
    type: str
    properties: Dict[str, Any]


class GraphResponse(BaseModel):
    nodes: List[GraphNode]
    edges: List[GraphEdge]


class PathRequest(BaseModel):
    from_entity_id: str
    to_entity_id: str
    max_hops: int = Field(default=5, ge=1, le=10)


@router.post("/neighborhood", response_model=GraphResponse)
async def get_neighborhood(
    req: NeighborhoodRequest,
    user=Depends(require_analyst),
    session=Depends(get_neo4j_session),
) -> GraphResponse:
    rel_filter = _build_rel_filter(req.rel_types)
    cypher = f"""
    MATCH (start {{entity_id: $entity_id}})
    CALL apoc.path.subgraphAll(start, {{
        maxLevel: $hops,
        relationshipFilter: '{rel_filter}'
    }})
    YIELD nodes, relationships
    RETURN nodes, relationships
    """
    result = session.run(cypher, entity_id=req.entity_id, hops=req.hops)
    row = result.single()
    if not row:
        return GraphResponse(nodes=[], edges=[])
    return _build_graph_response(row["nodes"], row["relationships"])


@router.post("/path", response_model=GraphResponse)
async def get_path(
    req: PathRequest,
    user=Depends(require_analyst),
    session=Depends(get_neo4j_session),
) -> GraphResponse:
    cypher = """
    MATCH (a {entity_id: $from_id}), (b {entity_id: $to_id}),
          p = shortestPath((a)-[*..{max_hops}]-(b))
    RETURN nodes(p) AS nodes, relationships(p) AS relationships
    """.replace("{max_hops}", str(req.max_hops))
    result = session.run(cypher, from_id=req.from_entity_id, to_id=req.to_entity_id)
    row = result.single()
    if not row:
        return GraphResponse(nodes=[], edges=[])
    return _build_graph_response(row["nodes"], row["relationships"])


def _build_rel_filter(rel_types: Optional[List[str]]) -> str:
    if not rel_types:
        types = ALLOWED_REL_TYPES
    else:
        types = {r for r in rel_types if r in ALLOWED_REL_TYPES}
    return "|".join(f">{t}" for t in types)


def _build_graph_response(nodes: list, rels: list) -> GraphResponse:
    graph_nodes = []
    for n in nodes:
        graph_nodes.append(GraphNode(
            id=n.get("entity_id", str(n.id)),
            labels=list(n.labels),
            properties={k: v for k, v in dict(n).items() if k not in ("entity_id",)},
        ))
    graph_edges = []
    for r in rels:
        graph_edges.append(GraphEdge(
            source=r.start_node.get("entity_id", ""),
            target=r.end_node.get("entity_id", ""),
            type=r.type,
            properties=dict(r),
        ))
    return GraphResponse(nodes=graph_nodes, edges=graph_edges)
