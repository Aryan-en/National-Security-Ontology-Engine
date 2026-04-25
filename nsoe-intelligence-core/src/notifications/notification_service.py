"""
Notification Service — 2.7.4
  - WebSocket push handled by nsoe-api (alerts stream)
  - SMS/email for high-severity alerts (fused_confidence > 0.8)
  - PagerDuty/Opsgenie integration for on-call escalation
Triggered by consuming alerts.active Kafka topic.
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, Optional

import requests
import structlog
from confluent_kafka import Consumer, KafkaError

logger = structlog.get_logger(__name__)

# ── Config from environment ───────────────────────────────────
KAFKA_BOOTSTRAP       = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SMS_API_URL           = os.environ.get("SMS_API_URL",     "")          # e.g. AWS SNS endpoint
SMS_API_KEY           = os.environ.get("SMS_API_KEY",     "")
EMAIL_API_URL         = os.environ.get("EMAIL_API_URL",   "")          # e.g. AWS SES
EMAIL_FROM            = os.environ.get("EMAIL_FROM",      "alerts@nsoe.gov.in")
ON_CALL_EMAILS        = os.environ.get("ON_CALL_EMAILS",  "").split(",")
ON_CALL_PHONES        = os.environ.get("ON_CALL_PHONES",  "").split(",")
PAGERDUTY_ROUTING_KEY = os.environ.get("PAGERDUTY_ROUTING_KEY", "")
OPSGENIE_API_KEY      = os.environ.get("OPSGENIE_API_KEY", "")
HIGH_SEVERITY_THRESHOLD = float(os.environ.get("HIGH_SEVERITY_THRESHOLD", "0.80"))


class NotificationService:

    def __init__(self) -> None:
        self._consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "nsoe-notification-service",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        })
        self._consumer.subscribe(["alerts.active"])
        self._running = False

    def start(self) -> None:
        self._running = True
        logger.info("notification_service_started")
        self._loop()

    def stop(self) -> None:
        self._running = False
        self._consumer.close()

    def _loop(self) -> None:
        while self._running:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("kafka_error", error=str(msg.error()))
                continue
            try:
                alert = json.loads(msg.value().decode())
                self._dispatch(alert)
            except Exception as e:
                logger.error("notification_error", error=str(e))

    def _dispatch(self, alert: Dict) -> None:
        confidence = float(alert.get("fused_confidence", 0.0))
        tier = alert.get("alert_tier", "")
        if confidence >= HIGH_SEVERITY_THRESHOLD or tier == "ESCALATION":
            self._send_sms(alert)
            self._send_email(alert)
            self._pagerduty_trigger(alert)
            self._opsgenie_trigger(alert)
        elif tier == "SOFT_ALERT":
            self._send_email(alert)

    def _send_sms(self, alert: Dict) -> None:
        if not SMS_API_URL or not ON_CALL_PHONES[0]:
            return
        body = (
            f"[NSOE ALERT] {alert.get('alert_type')} "
            f"Tier:{alert.get('alert_tier')} "
            f"Confidence:{int(alert.get('fused_confidence',0)*100)}% "
            f"Zone:{alert.get('geo_zone_id','?')} "
            f"ID:{alert.get('alert_id','?')[:8]}"
        )
        for phone in ON_CALL_PHONES:
            if not phone.strip():
                continue
            try:
                requests.post(SMS_API_URL, json={
                    "to": phone.strip(), "message": body,
                    "api_key": SMS_API_KEY,
                }, timeout=5)
                logger.info("sms_sent", phone=phone[:4] + "****")
            except Exception as e:
                logger.error("sms_failed", error=str(e))

    def _send_email(self, alert: Dict) -> None:
        if not EMAIL_API_URL or not ON_CALL_EMAILS[0]:
            return
        subject = f"[NSOE] {alert.get('alert_tier')} Alert: {alert.get('alert_type')}"
        body = json.dumps(alert, indent=2)
        for email in ON_CALL_EMAILS:
            if not email.strip():
                continue
            try:
                requests.post(EMAIL_API_URL, json={
                    "from": EMAIL_FROM, "to": email.strip(),
                    "subject": subject, "text": body,
                }, timeout=5)
                logger.info("email_sent", to=email.strip())
            except Exception as e:
                logger.error("email_failed", error=str(e))

    def _pagerduty_trigger(self, alert: Dict) -> None:
        if not PAGERDUTY_ROUTING_KEY:
            return
        try:
            requests.post(
                "https://events.pagerduty.com/v2/enqueue",
                json={
                    "routing_key": PAGERDUTY_ROUTING_KEY,
                    "event_action": "trigger",
                    "dedup_key": alert.get("alert_id", ""),
                    "payload": {
                        "summary": f"NSOE {alert.get('alert_tier')}: {alert.get('alert_type')}",
                        "severity": "critical" if alert.get("alert_tier") == "ESCALATION" else "warning",
                        "source": "nsoe-correlation-engine",
                        "custom_details": alert,
                    },
                },
                timeout=5,
            )
            logger.info("pagerduty_triggered", alert_id=alert.get("alert_id"))
        except Exception as e:
            logger.error("pagerduty_failed", error=str(e))

    def _opsgenie_trigger(self, alert: Dict) -> None:
        if not OPSGENIE_API_KEY:
            return
        try:
            requests.post(
                "https://api.opsgenie.com/v2/alerts",
                headers={"Authorization": f"GenieKey {OPSGENIE_API_KEY}"},
                json={
                    "message":   f"NSOE {alert.get('alert_tier')}: {alert.get('alert_type')}",
                    "alias":     alert.get("alert_id", ""),
                    "priority":  "P1" if alert.get("alert_tier") == "ESCALATION" else "P2",
                    "details":   {k: str(v) for k, v in alert.items()},
                    "source":    "nsoe",
                    "tags":      ["nsoe", alert.get("alert_tier", "")],
                },
                timeout=5,
            )
            logger.info("opsgenie_triggered", alert_id=alert.get("alert_id"))
        except Exception as e:
            logger.error("opsgenie_failed", error=str(e))
