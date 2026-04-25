"""
Kafka producer for edge node detection events.
- Protobuf/Avro serialization via Confluent Schema Registry
- TLS client certificate authentication
- Idempotent producer (no duplicate events)
- Integrates with RocksDB buffer for at-least-once delivery
"""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any, Callable, Dict, Optional

import structlog
from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from nsoe_edge.buffer.rocksdb_buffer import RocksDBEventBuffer

logger = structlog.get_logger(__name__)

TOPIC_CCTV_EVENTS = "edge.cctv.events"


class EdgeKafkaProducer:
    """
    Wraps confluent-kafka Producer with:
    - TLS mutual authentication using client certs
    - Avro serialization via Schema Registry
    - Idempotent delivery (enable.idempotence=true)
    - Delivery callback feeding into RocksDB buffer acknowledgement
    """

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        ssl_ca_location: str,
        ssl_certificate_location: str,
        ssl_key_location: str,
        edge_node_id: str,
        buffer: RocksDBEventBuffer,
    ) -> None:
        self.edge_node_id = edge_node_id
        self.buffer = buffer
        self._producer: Optional[Producer] = None
        self._serializer: Optional[AvroSerializer] = None
        self._schema_registry_url = schema_registry_url
        self._bootstrap_servers = bootstrap_servers
        self._ssl = {
            "ssl.ca.location": ssl_ca_location,
            "ssl.certificate.location": ssl_certificate_location,
            "ssl.key.location": ssl_key_location,
        }

    def connect(self, avro_schema_str: str) -> None:
        sr_client = SchemaRegistryClient({"url": self._schema_registry_url})
        self._serializer = AvroSerializer(
            sr_client,
            avro_schema_str,
            conf={"auto.register.schemas": False},
        )
        producer_conf: Dict[str, Any] = {
            "bootstrap.servers": self._bootstrap_servers,
            "security.protocol": "SSL",
            "enable.idempotence": True,
            "acks": "all",
            "retries": 2147483647,
            "max.in.flight.requests.per.connection": 5,
            "compression.type": "lz4",
            "linger.ms": 20,
            "batch.size": 65536,
            "client.id": f"nsoe-edge-{self.edge_node_id}",
            **self._ssl,
        }
        self._producer = Producer(producer_conf)
        logger.info("kafka_producer_connected", edge_node_id=self.edge_node_id)

    def publish(self, event: Dict[str, Any], seq: int) -> None:
        """
        Serialize and produce an event.
        seq is the RocksDB sequence number for acknowledgement on delivery.
        """
        if self._producer is None or self._serializer is None:
            raise RuntimeError("Producer not connected. Call connect() first.")

        value = self._serializer(
            event,
            SerializationContext(TOPIC_CCTV_EVENTS, MessageField.VALUE),
        )
        key = event.get("camera_id", self.edge_node_id).encode()

        def delivery_callback(err: Optional[KafkaError], msg: Any) -> None:
            if err:
                logger.error(
                    "kafka_delivery_failed",
                    error=str(err),
                    event_id=event.get("event_id"),
                )
            else:
                self.buffer.acknowledge(seq)
                logger.debug(
                    "kafka_delivered",
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    event_id=event.get("event_id"),
                )

        self._producer.produce(
            topic=TOPIC_CCTV_EVENTS,
            key=key,
            value=value,
            on_delivery=delivery_callback,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        if self._producer is not None:
            return self._producer.flush(timeout=timeout)
        return 0

    def replay_buffered(self) -> None:
        """Replay all unacknowledged events from the RocksDB buffer."""
        count = 0
        for seq, event in self.buffer.get_unacknowledged():
            self.publish(event, seq)
            count += 1
        if count > 0:
            logger.info("buffer_replayed", count=count)
            self.flush()
