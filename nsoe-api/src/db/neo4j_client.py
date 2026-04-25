"""Neo4j session dependency for FastAPI."""

from __future__ import annotations

import os
from typing import Generator

from neo4j import GraphDatabase, Session

_driver = None


def get_driver():
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(
            os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
            auth=(
                os.environ.get("NEO4J_USER", "neo4j"),
                os.environ.get("NEO4J_PASSWORD", ""),
            ),
        )
    return _driver


def get_neo4j_session() -> Generator[Session, None, None]:
    with get_driver().session() as session:
        yield session
