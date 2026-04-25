"""Kafka consumer factory for the alerts WebSocket stream."""

from __future__ import annotations

import os

from confluent_kafka import Consumer


def get_alert_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": f"nsoe-api-ws-{os.getpid()}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "topics": ["alerts.active"],
    })
