"""Elasticsearch client dependency for FastAPI."""

from __future__ import annotations

import os

from elasticsearch import Elasticsearch

_client = None


def get_es_client() -> Elasticsearch:
    global _client
    if _client is None:
        _client = Elasticsearch(
            os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200"),
            http_auth=(
                os.environ.get("ES_USER", "elastic"),
                os.environ.get("ES_PASSWORD", ""),
            ),
        )
    return _client
