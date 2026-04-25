"""
Flink job: basic-cep-alert-job
Consumes from enriched.cctv.events and detects:
  - LOITERING: person track present > 300s in same zone
  - ABANDONED_OBJECT: object detected, no person nearby for > 60s
Emits matched patterns to alerts.active Kafka topic.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any, Dict, Iterable, Iterator, List, Optional

from pyflink.cep import CEP, Pattern
from pyflink.cep.pattern_select_function import PatternSelectFunction
from pyflink.cep.pattern_timeout_function import PatternTimeoutFunction
from pyflink.common import Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema,
    KafkaOffsetInitializer, DeliveryGuarantee,
)
from pyflink.datastream.functions import (
    KeyedProcessFunction, MapFunction, FilterFunction,
)
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common import Types

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = "enriched.cctv.events"
OUTPUT_TOPIC = "alerts.active"
PARALLELISM = int(os.environ.get("FLINK_PARALLELISM", "4"))

LOITERING_THRESHOLD_MS = 300_000   # 5 minutes
ABANDONED_THRESHOLD_MS = 60_000    # 1 minute
PROXIMITY_PX = 0.1                  # normalised bbox distance threshold


class ParseEnrichedEvent(MapFunction):
    def map(self, raw: str) -> Optional[Dict]:
        try:
            return json.loads(raw)
        except Exception:
            return None


class FilterValid(FilterFunction):
    def filter(self, v: Optional[Dict]) -> bool:
        return v is not None and v.get("detection_class") is not None


class LoiteringDetector(KeyedProcessFunction):
    """
    Keyed by (track_id, camera_id).
    Tracks entry time; emits LOITERING alert when track persists > threshold.
    """

    def open(self, ctx: Any) -> None:
        self._entry_time = ctx.get_state(ValueStateDescriptor("entry_time", Types.LONG()))
        self._alerted = ctx.get_state(ValueStateDescriptor("alerted", Types.BOOLEAN()))
        self._last_event = ctx.get_state(ValueStateDescriptor("last_event", Types.STRING()))

    def process_element(self, event: Dict, ctx: Any, out: Any) -> None:
        if event.get("detection_class") != "PERSON":
            return

        ts = event.get("normalized_timestamp_ms", ctx.timestamp())

        if self._entry_time.value() is None:
            self._entry_time.update(ts)
            self._alerted.update(False)

        self._last_event.update(json.dumps(event))

        duration = ts - (self._entry_time.value() or ts)
        if duration >= LOITERING_THRESHOLD_MS and not self._alerted.value():
            self._alerted.update(True)
            alert = _make_alert(
                alert_type="LOITERING",
                alert_tier="SOFT_ALERT",
                confidence=0.75,
                event=event,
            )
            out.collect(json.dumps(alert))

        # Register cleanup timer: if no new event in 60s, reset state
        ctx.timer_service().register_event_time_timer(ts + 60_000)

    def on_timer(self, timestamp: int, ctx: Any, out: Any) -> None:
        last_raw = self._last_event.value()
        if last_raw:
            last = json.loads(last_raw)
            last_ts = last.get("normalized_timestamp_ms", 0)
            if timestamp - last_ts >= 60_000:
                # Track gone; reset
                self._entry_time.clear()
                self._alerted.clear()
                self._last_event.clear()


class AbandonedObjectDetector(KeyedProcessFunction):
    """
    Keyed by geo_zone_id.
    Tracks objects detected without a nearby person for > threshold.
    State: map of object_track_id → (first_seen_ms, last_bbox)
    """

    def open(self, ctx: Any) -> None:
        self._objects = ctx.get_map_state(
            MapStateDescriptor("objects", Types.STRING(), Types.STRING())
        )
        self._persons = ctx.get_map_state(
            MapStateDescriptor("persons", Types.STRING(), Types.STRING())
        )

    def process_element(self, event: Dict, ctx: Any, out: Any) -> None:
        ts = event.get("normalized_timestamp_ms", ctx.timestamp())
        track_id = event.get("track_id", "")
        cls = event.get("detection_class", "")
        bbox = event.get("bbox", {})

        if cls == "PERSON":
            self._persons.put(track_id, json.dumps({"ts": ts, "bbox": bbox}))
        elif cls in ("BAG", "VEHICLE"):
            if not self._objects.contains(track_id):
                self._objects.put(track_id, json.dumps({"first_seen": ts, "bbox": bbox, "alerted": False}))

        # Check each tracked object
        for obj_track_id in list(self._objects.keys()):
            obj = json.loads(self._objects.get(obj_track_id))
            if obj.get("alerted"):
                continue
            age = ts - obj["first_seen"]
            if age < ABANDONED_THRESHOLD_MS:
                continue
            # Check if any person is nearby
            obj_bbox = obj["bbox"]
            person_nearby = False
            for p_track_id in list(self._persons.keys()):
                p = json.loads(self._persons.get(p_track_id))
                if ts - p["ts"] > 10_000:
                    continue  # stale person record
                if _bboxes_close(obj_bbox, p["bbox"]):
                    person_nearby = True
                    break
            if not person_nearby:
                obj["alerted"] = True
                self._objects.put(obj_track_id, json.dumps(obj))
                alert = _make_alert(
                    alert_type="ABANDONED_OBJECT",
                    alert_tier="ANALYST_REVIEW",
                    confidence=0.65,
                    event=event,
                )
                alert["involved_track_ids"] = [obj_track_id]
                out.collect(json.dumps(alert))

        # Evict stale person records
        for p_track_id in list(self._persons.keys()):
            p = json.loads(self._persons.get(p_track_id))
            if ts - p["ts"] > 30_000:
                self._persons.remove(p_track_id)

        ctx.timer_service().register_event_time_timer(ts + 10_000)

    def on_timer(self, timestamp: int, ctx: Any, out: Any) -> None:
        pass


def _bboxes_close(a: Dict, b: Dict) -> bool:
    cx_a = (a.get("x1", 0) + a.get("x2", 0)) / 2
    cy_a = (a.get("y1", 0) + a.get("y2", 0)) / 2
    cx_b = (b.get("x1", 0) + b.get("x2", 0)) / 2
    cy_b = (b.get("y1", 0) + b.get("y2", 0)) / 2
    return ((cx_a - cx_b) ** 2 + (cy_a - cy_b) ** 2) ** 0.5 < PROXIMITY_PX


def _make_alert(alert_type: str, alert_tier: str, confidence: float, event: Dict) -> Dict:
    return {
        "alert_id": str(uuid.uuid4()),
        "alert_type": alert_type,
        "alert_tier": alert_tier,
        "fused_confidence": confidence,
        "camera_id": event.get("camera_id"),
        "geo_zone_id": event.get("geo_location", {}).get("geo_zone_id"),
        "latitude": event.get("geo_location", {}).get("latitude"),
        "longitude": event.get("geo_location", {}).get("longitude"),
        "involved_track_ids": [event.get("track_id", "")],
        "involved_entity_ids": [],
        "contributing_events": [event.get("event_id", "")],
        "source_domains": ["CCTV"],
        "triggered_at_ms": event.get("normalized_timestamp_ms", int(time.time() * 1000)),
        "ttl_seconds": 86400,
        "schema_version": "1.0.0",
    }


def build_job(env: StreamExecutionEnvironment) -> None:
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id("nsoe-cep-alert")
        .set_starting_offsets(KafkaOffsetInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(
            lambda s, _: json.loads(s).get("normalized_timestamp_ms", 0) if s else 0
        )
    )

    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    parsed = (
        env.from_source(kafka_source, watermark_strategy, "KafkaSource-Enriched")
        .map(ParseEnrichedEvent())
        .filter(FilterValid())
    )

    loitering_alerts = (
        parsed
        .key_by(lambda e: f"{e['track_id']}_{e['camera_id']}")
        .process(LoiteringDetector())
    )

    abandoned_alerts = (
        parsed
        .key_by(lambda e: e.get("geo_location", {}).get("geo_zone_id", "UNKNOWN"))
        .process(AbandonedObjectDetector())
    )

    loitering_alerts.union(abandoned_alerts).sink_to(kafka_sink)


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(30_000)
    build_job(env)
    env.execute("nsoe-basic-cep-alert-job")


if __name__ == "__main__":
    main()
