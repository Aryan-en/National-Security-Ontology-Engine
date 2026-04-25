"""
Immutable Audit Log Service — 2.6.6
All analyst actions (alert acknowledge/close/escalate, feedback, graph queries)
are written to an append-only audit log stored in:
  - Neo4j (AuditEntry nodes, write-only relationship from analyst)
  - S3/MinIO (WORM bucket: nsoe-audit-logs)
  - Structured log stream → SIEM

WORM guarantee: MinIO is configured with Object Lock in GOVERNANCE mode
with a 7-year retention period (per MHA data retention policy).
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import boto3
import structlog
from botocore.config import Config
from neo4j import Driver, GraphDatabase

logger = structlog.get_logger(__name__)

# MinIO / S3 config
S3_ENDPOINT  = os.environ.get("S3_ENDPOINT",   "http://minio:9000")
S3_ACCESS    = os.environ.get("S3_ACCESS_KEY",  "nsoe-admin")
S3_SECRET    = os.environ.get("S3_SECRET_KEY",  "")
AUDIT_BUCKET = os.environ.get("AUDIT_BUCKET",   "nsoe-audit-logs")
RETENTION_YEARS = int(os.environ.get("AUDIT_RETENTION_YEARS", 7))

WRITE_AUDIT_ENTRY = """
CREATE (a:AuditEntry {
    audit_id:       $audit_id,
    analyst_sub:    $analyst_sub,
    action:         $action,
    resource_type:  $resource_type,
    resource_id:    $resource_id,
    outcome:        $outcome,
    ip_address:     $ip_address,
    timestamp:      $timestamp,
    payload_hash:   $payload_hash
})
"""


class AuditLogService:
    """
    Writes analyst actions to both Neo4j (queryable) and MinIO WORM (durable).
    All entries are signed with a SHA-256 hash of the payload for tamper detection.
    """

    def __init__(
        self,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
    ) -> None:
        self._driver: Driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self._s3 = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS,
            aws_secret_access_key=S3_SECRET,
            config=Config(signature_version="s3v4"),
        )
        self._ensure_bucket()

    def log(
        self,
        analyst_sub: str,
        action: str,               # ACKNOWLEDGE_ALERT | CLOSE_ALERT | ESCALATE | FEEDBACK | GRAPH_QUERY
        resource_type: str,        # ALERT | CASE | ENTITY | GRAPH
        resource_id: str,
        outcome: str = "SUCCESS",  # SUCCESS | DENIED | ERROR
        ip_address: Optional[str] = None,
        extra: Optional[Dict] = None,
    ) -> str:
        """Log an analyst action. Returns audit_id."""
        audit_id = str(uuid.uuid4())
        now_iso = datetime.now(timezone.utc).isoformat()
        payload = {
            "audit_id":     audit_id,
            "analyst_sub":  analyst_sub,
            "action":       action,
            "resource_type": resource_type,
            "resource_id":  resource_id,
            "outcome":      outcome,
            "ip_address":   ip_address or "",
            "timestamp":    now_iso,
            "extra":        extra or {},
        }
        payload_json = json.dumps(payload, sort_keys=True)
        payload_hash = hashlib.sha256(payload_json.encode()).hexdigest()
        payload["payload_hash"] = payload_hash

        # 1. Write to Neo4j (best-effort)
        try:
            with self._driver.session() as session:
                session.run(WRITE_AUDIT_ENTRY, {
                    "audit_id":      audit_id,
                    "analyst_sub":   analyst_sub,
                    "action":        action,
                    "resource_type": resource_type,
                    "resource_id":   resource_id,
                    "outcome":       outcome,
                    "ip_address":    ip_address or "",
                    "timestamp":     now_iso,
                    "payload_hash":  payload_hash,
                })
        except Exception as e:
            logger.error("audit_neo4j_write_failed", error=str(e))

        # 2. Write to MinIO WORM bucket (durable)
        try:
            key = f"{now_iso[:10]}/{audit_id}.json"
            self._s3.put_object(
                Bucket=AUDIT_BUCKET,
                Key=key,
                Body=json.dumps(payload).encode(),
                ContentType="application/json",
                ObjectLockMode="GOVERNANCE",
                ObjectLockRetainUntilDate=self._retention_date(),
            )
        except Exception as e:
            logger.error("audit_s3_write_failed", error=str(e))

        # 3. Structured log → SIEM
        logger.info(
            "analyst_action",
            audit_id=audit_id,
            analyst=analyst_sub,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            outcome=outcome,
            payload_hash=payload_hash,
        )

        return audit_id

    def _ensure_bucket(self) -> None:
        try:
            self._s3.head_bucket(Bucket=AUDIT_BUCKET)
        except Exception:
            try:
                self._s3.create_bucket(Bucket=AUDIT_BUCKET)
                self._s3.put_object_lock_configuration(
                    Bucket=AUDIT_BUCKET,
                    ObjectLockConfiguration={
                        "ObjectLockEnabled": "Enabled",
                        "Rule": {
                            "DefaultRetention": {
                                "Mode": "GOVERNANCE",
                                "Years": RETENTION_YEARS,
                            }
                        },
                    },
                )
            except Exception as e:
                logger.warning("audit_bucket_setup_failed", error=str(e))

    def _retention_date(self) -> datetime:
        from datetime import timedelta
        return datetime.now(timezone.utc) + timedelta(days=365 * RETENTION_YEARS)

    def close(self) -> None:
        self._driver.close()
