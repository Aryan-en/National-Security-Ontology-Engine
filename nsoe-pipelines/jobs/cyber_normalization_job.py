"""
Flink job: cyber-normalization-job — 2.1.2 / 2.1.4
Consumes from cyber.raw.events:
  - Normalises syslog/CEF/LEEF → STIX 2.1 intermediate format
  - Runs Isolation Forest anomaly detection on NetFlow/IPFIX records
  - Maps detected techniques to MITRE ATT&CK
  - Writes normalised events to cyber.normalized.events and anomalies to alerts.active
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from typing import Dict, List, Optional, Any

from pyflink.common import Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema,
    KafkaOffsetInitializer, DeliveryGuarantee,
)
from pyflink.datastream.functions import MapFunction, FilterFunction, RichMapFunction

BOOTSTRAP_SERVERS   = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC         = "cyber.raw.events"
OUTPUT_NORMALIZED   = "cyber.normalized.events"
OUTPUT_ALERTS       = "alerts.active"
PARALLELISM         = int(os.environ.get("FLINK_PARALLELISM", "4"))
ANOMALY_THRESHOLD   = float(os.environ.get("ANOMALY_THRESHOLD", "-0.05"))   # IF decision threshold


# ── CEF/LEEF/Syslog parsers ──────────────────────────────────

class ParseRawEvent(MapFunction):
    def map(self, raw: str) -> Optional[Dict]:
        try:
            return json.loads(raw)
        except Exception:
            return None


class FilterValid(FilterFunction):
    def filter(self, v: Optional[Dict]) -> bool:
        return v is not None


class NormalizeToCyberEvent(RichMapFunction):
    """
    Normalise CEF/LEEF/syslog → common CyberEvent dict.
    Also applies MITRE ATT&CK keyword mapping.
    """

    def open(self, ctx: Any) -> None:
        # Import mapper inside task to avoid serialisation issues
        import sys
        sys.path.insert(0, "/opt/nsoe")
        from nsoe_intelligence_core.cyber.mitre_attack_mapper import get_mapper
        self._mapper = get_mapper()

    def map(self, event: Dict) -> Dict:
        fmt = event.get("source_format", "UNKNOWN")
        now = int(time.time() * 1000)
        cyber = {
            "event_id":       event.get("event_id", str(uuid.uuid4())),
            "source_format":  fmt,
            "source_system_id": event.get("source_system_id", ""),
            "received_at_ms": event.get("received_at_ms", now),
            "facility_id":    event.get("facility_id"),
            "src_ip":         event.get("src_ip"),
            "dst_ip":         event.get("dst_ip"),
            "src_port":       event.get("src_port"),
            "dst_port":       event.get("dst_port"),
            "protocol":       event.get("protocol"),
            "bytes_in":       event.get("bytes_in", 0),
            "bytes_out":      event.get("bytes_out", 0),
            "action":         event.get("action"),
            "severity":       event.get("severity", 0),
            "ttp_ids":        [],
            "anomaly_score":  None,
            "is_anomaly":     False,
            "stix_type":      "network-traffic",
            "schema_version": "1.0.0",
        }

        # TTP mapping from raw payload
        raw_payload = event.get("raw_payload", "")
        if raw_payload and self._mapper:
            techniques = self._mapper.map_event(raw_payload, top_k=3)
            cyber["ttp_ids"] = [t.technique_id for t in techniques]

        # CEF-specific field extraction
        if fmt == "CEF":
            cyber.update(_parse_cef_extensions(raw_payload))

        return cyber


class IsolationForestAnomalyDetector(RichMapFunction):
    """
    Runs scikit-learn IsolationForest on numeric NetFlow features.
    Model is loaded from a pre-trained pickle file on the classpath.
    """

    _FEATURE_KEYS = ["bytes_in", "bytes_out", "src_port", "dst_port", "severity"]

    def open(self, ctx: Any) -> None:
        import pickle
        model_path = os.environ.get("ANOMALY_MODEL_PATH", "/opt/nsoe/models/isolation_forest.pkl")
        try:
            with open(model_path, "rb") as f:
                self._model = pickle.load(f)
        except Exception:
            self._model = None

    def map(self, event: Dict) -> Dict:
        if self._model is None or event.get("source_format") not in ("NETFLOW", "IPFIX"):
            return event
        import numpy as np
        features = np.array([[
            event.get(k) or 0 for k in self._FEATURE_KEYS
        ]], dtype=float)
        score = float(self._model.decision_function(features)[0])
        pred  = int(self._model.predict(features)[0])   # -1 = anomaly
        event = dict(event)
        event["anomaly_score"] = score
        event["is_anomaly"] = (pred == -1 and score < ANOMALY_THRESHOLD)
        return event


class AnomalyToAlert(MapFunction):
    def map(self, event: Dict) -> Optional[str]:
        if not event.get("is_anomaly"):
            return None
        alert = {
            "alert_id":            str(uuid.uuid4()),
            "alert_type":          "NETWORK_ANOMALY",
            "alert_tier":          _tier(0.7),
            "fused_confidence":    0.7,
            "camera_id":           None,
            "geo_zone_id":         event.get("facility_id"),
            "latitude":            None,
            "longitude":           None,
            "involved_track_ids":  [],
            "involved_entity_ids": [],
            "contributing_events": [event.get("event_id", "")],
            "source_domains":      ["CYBER"],
            "triggered_at_ms":     event.get("received_at_ms", int(time.time() * 1000)),
            "ttl_seconds":         86400,
            "anomaly_score":       event.get("anomaly_score"),
            "src_ip":              event.get("src_ip"),
            "dst_ip":              event.get("dst_ip"),
            "ttp_ids":             event.get("ttp_ids", []),
            "schema_version":      "1.0.0",
        }
        return json.dumps(alert)


def _tier(score: float) -> str:
    if score >= 0.8: return "ESCALATION"
    if score >= 0.6: return "SOFT_ALERT"
    return "ANALYST_REVIEW"


def _parse_cef_extensions(raw: str) -> Dict:
    """Extract key=value pairs from CEF extension string."""
    result = {}
    for m in re.finditer(r"(\w+)=((?:[^\\=]|\\.)+?)(?=\s\w+=|$)", raw):
        result[m.group(1)] = m.group(2).strip()
    return result


def build_job(env: StreamExecutionEnvironment) -> None:
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id("nsoe-cyber-normalization")
        .set_starting_offsets(KafkaOffsetInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    watermark = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))

    def _make_sink(topic: str):
        return (KafkaSink.builder()
                .set_bootstrap_servers(BOOTSTRAP_SERVERS)
                .set_record_serializer(
                    KafkaRecordSerializationSchema.builder()
                    .set_topic(topic)
                    .set_value_serialization_schema(SimpleStringSchema())
                    .build())
                .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build())

    normalized = (
        env.from_source(kafka_source, watermark, "KafkaSource-CyberRaw")
        .map(ParseRawEvent())
        .filter(FilterValid())
        .map(NormalizeToCyberEvent())
        .map(IsolationForestAnomalyDetector())
    )

    # Normalized events → cyber.normalized.events
    normalized.map(lambda e: json.dumps(e)).sink_to(_make_sink(OUTPUT_NORMALIZED))

    # Anomalies → alerts.active
    (normalized
     .map(AnomalyToAlert())
     .filter(lambda s: s is not None)
     .sink_to(_make_sink(OUTPUT_ALERTS)))


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(30_000)
    build_job(env)
    env.execute("nsoe-cyber-normalization-job")


if __name__ == "__main__":
    main()
