"""
CERT-In TAXII 2.1 feed client — 2.1.5
Polls a TAXII server collection and feeds bundles into StixIngestor.
Designed to run as an Airflow task or standalone daemon.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional

import structlog
import requests
from requests.auth import HTTPBasicAuth

from nsoe_intelligence_core.cyber.stix_ingestor import StixIngestor

logger = structlog.get_logger(__name__)

# Default: CERT-In TAXII test endpoint (replace with production URL)
TAXII_BASE_URL   = os.environ.get("TAXII_BASE_URL",   "https://taxii.cert-in.org.in/taxii2/")
TAXII_COLLECTION = os.environ.get("TAXII_COLLECTION", "nsoe-indicators")
TAXII_USER       = os.environ.get("TAXII_USER",       "")
TAXII_PASSWORD   = os.environ.get("TAXII_PASSWORD",   "")
POLL_INTERVAL_S  = int(os.environ.get("TAXII_POLL_INTERVAL_S", 3600))


class TaxiiClient:
    """
    TAXII 2.1 client.
    Fetches STIX 2.1 bundles from a collection since the last poll timestamp.
    State is persisted via a simple flat file for resumability.
    """

    def __init__(
        self,
        ingestor: StixIngestor,
        state_file: str = "/var/lib/nsoe/taxii_state.json",
    ) -> None:
        self._ingestor = ingestor
        self._state_file = state_file
        self._session = requests.Session()
        if TAXII_USER:
            self._session.auth = HTTPBasicAuth(TAXII_USER, TAXII_PASSWORD)
        self._session.headers.update({
            "Accept": "application/taxii+json;version=2.1",
            "Content-Type": "application/taxii+json;version=2.1",
        })

    def _load_state(self) -> Optional[str]:
        """Return ISO timestamp of last successful poll, or None."""
        import json, pathlib
        p = pathlib.Path(self._state_file)
        if p.exists():
            try:
                return json.loads(p.read_text()).get("last_added_after")
            except Exception:
                pass
        return None

    def _save_state(self, ts: str) -> None:
        import json, pathlib
        pathlib.Path(self._state_file).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(self._state_file).write_text(json.dumps({"last_added_after": ts}))

    def _collection_url(self) -> str:
        return f"{TAXII_BASE_URL.rstrip('/')}/collections/{TAXII_COLLECTION}/objects/"

    def _fetch_page(self, added_after: Optional[str], next_cursor: Optional[str]) -> dict:
        params: dict = {"limit": 100}
        if added_after and not next_cursor:
            params["added_after"] = added_after
        if next_cursor:
            params["next"] = next_cursor
        resp = self._session.get(
            self._collection_url(),
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def poll_once(self) -> int:
        """Fetch all new objects since last poll. Returns total objects ingested."""
        added_after = self._load_state()
        now = datetime.now(timezone.utc).isoformat()
        total = 0
        next_cursor = None

        while True:
            try:
                data = self._fetch_page(added_after, next_cursor)
            except requests.HTTPError as e:
                logger.error("taxii_fetch_error", status=e.response.status_code if e.response else None, error=str(e))
                break
            except Exception as e:
                logger.error("taxii_fetch_error", error=str(e))
                break

            objects = data.get("objects", [])
            if objects:
                bundle = {"type": "bundle", "spec_version": "2.1", "objects": objects}
                total += self._ingestor.ingest_bundle(bundle)

            next_cursor = data.get("next")
            if not next_cursor:
                break

        self._save_state(now)
        logger.info("taxii_poll_complete", objects_ingested=total, added_after=added_after)
        return total

    def run_daemon(self) -> None:
        """Poll indefinitely at POLL_INTERVAL_S."""
        logger.info("taxii_daemon_started", interval_s=POLL_INTERVAL_S)
        while True:
            try:
                self.poll_once()
            except Exception as e:
                logger.error("taxii_poll_failed", error=str(e))
            time.sleep(POLL_INTERVAL_S)


def main() -> None:
    ingestor = StixIngestor(
        neo4j_uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_user=os.environ.get("NEO4J_USER", "neo4j"),
        neo4j_password=os.environ.get("NEO4J_PASSWORD", ""),
    )
    client = TaxiiClient(ingestor)
    client.run_daemon()


if __name__ == "__main__":
    main()
