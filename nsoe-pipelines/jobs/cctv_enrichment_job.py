"""
Flink job: cctv-enrichment-job
- Consumes from edge.cctv.events
- Deduplication (30s sliding window by track_id + camera_id)
- Timestamp normalization (server receipt time override for unsynced devices)
- Geolocation enrichment (join camera metadata lookup table)
- Emits to enriched.cctv.events
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any, Dict, Iterable, Optional

from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema,
    KafkaOffsetInitializer, DeliveryGuarantee,
)
from pyflink.datastream.functions import (
    MapFunction, FilterFunction, RichFlatMapFunction,
    KeyedProcessFunction, ProcessWindowFunction,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import SlidingEventTimeWindows, Time

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
INPUT_TOPIC = "edge.cctv.events"
OUTPUT_TOPIC = "enriched.cctv.events"
PARALLELISM = int(os.environ.get("FLINK_PARALLELISM", "4"))

# Camera metadata lookup: camera_id → {lat, lon, geo_zone_id, zone_name}
# In production this comes from a broadcast state updated from a DB table.
CAMERA_METADATA: Dict[str, Dict[str, Any]] = {}


def load_camera_metadata(path: str) -> None:
    """Load camera metadata from a JSON file into the lookup table."""
    import json
    with open(path) as f:
        data = json.load(f)
    for cam in data:
        CAMERA_METADATA[cam["camera_id"]] = cam


class ParseEventFunction(MapFunction):
    """Deserialize JSON string → dict."""

    def map(self, raw: str) -> Optional[Dict]:
        try:
            return json.loads(raw)
        except Exception:
            return None


class FilterNullFunction(FilterFunction):
    def filter(self, value: Optional[Dict]) -> bool:
        return value is not None


class NormalizeTimestampFunction(MapFunction):
    """
    Override event timestamp with server_receipt_timestamp_ms when
    the device clock is not synced (device_clock_synced=False).
    """

    def map(self, event: Dict) -> Dict:
        if not event.get("device_clock_synced", True):
            server_ts = event.get("server_receipt_timestamp_ms")
            if server_ts:
                event = dict(event)
                event["frame_timestamp_ms"] = server_ts
        return event


class GeoEnrichmentFunction(MapFunction):
    """
    Join camera_id → geo metadata.
    If camera not found in lookup, emit with placeholder coordinates.
    """

    def map(self, event: Dict) -> Dict:
        camera_id = event.get("camera_id", "")
        meta = CAMERA_METADATA.get(camera_id, {
            "latitude": 0.0,
            "longitude": 0.0,
            "geo_zone_id": "UNKNOWN",
            "zone_name": "UNKNOWN",
        })
        event = dict(event)
        event["geo_location"] = {
            "latitude": meta["latitude"],
            "longitude": meta["longitude"],
            "geo_zone_id": meta["geo_zone_id"],
            "zone_name": meta["zone_name"],
        }
        return event


class DedupKeyFunction(MapFunction):
    """Compute dedup key: SHA-256(track_id + camera_id + 30s_bucket)."""

    _BUCKET_SIZE_MS = 30_000

    def map(self, event: Dict) -> Dict:
        ts = event.get("frame_timestamp_ms", 0)
        bucket = ts // self._BUCKET_SIZE_MS
        raw = f"{event.get('track_id', '')}_{event.get('camera_id', '')}_{bucket}"
        event = dict(event)
        event["dedup_hash"] = hashlib.sha256(raw.encode()).hexdigest()
        return event


class DedupFilterFunction(KeyedProcessFunction):
    """
    Per-key stateful dedup.
    Key = dedup_hash. Passes through first occurrence; drops subsequent.
    State TTL: 60 seconds.
    """

    def open(self, runtime_context: Any) -> None:
        descriptor = ValueStateDescriptor("seen", Types.BOOLEAN())
        self._seen = runtime_context.get_state(descriptor)

    def process_element(self, event: Dict, ctx: Any, out: Any) -> None:
        if not self._seen.value():
            self._seen.update(True)
            # Register cleanup timer 60s later
            ctx.timer_service().register_event_time_timer(
                ctx.timestamp() + 60_000
            )
            out.collect(event)

    def on_timer(self, timestamp: int, ctx: Any, out: Any) -> None:
        self._seen.clear()


class EnrichmentToOutputFunction(MapFunction):
    """Map enriched event dict to the enriched schema."""

    def map(self, event: Dict) -> Dict:
        return {
            "event_id":              event.get("event_id"),
            "edge_node_id":          event.get("edge_node_id"),
            "camera_id":             event.get("camera_id"),
            "track_id":              event.get("track_id"),
            "detection_class":       event.get("detection_class"),
            "detection_confidence":  event.get("detection_confidence"),
            "bbox":                  event.get("bbox"),
            "normalized_timestamp_ms": event.get("frame_timestamp_ms"),
            "device_clock_synced":   event.get("device_clock_synced", True),
            "geo_location":          event.get("geo_location"),
            "face_embedding":        event.get("face_embedding"),
            "dedup_hash":            event.get("dedup_hash", ""),
            "schema_version":        "1.0.0",
        }


class SerializeFunction(MapFunction):
    def map(self, event: Dict) -> str:
        return json.dumps(event)


def build_job(env: StreamExecutionEnvironment) -> None:
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id("nsoe-cctv-enrichment")
        .set_starting_offsets(KafkaOffsetInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(
            # Lambda: extract timestamp from event JSON
            lambda event_str, _: json.loads(event_str).get("frame_timestamp_ms", 0)
            if event_str else 0
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

    stream = (
        env.from_source(kafka_source, watermark_strategy, "KafkaSource-CCTV")
        .map(ParseEventFunction())
        .filter(FilterNullFunction())
        .map(NormalizeTimestampFunction())
        .map(GeoEnrichmentFunction())
        .map(DedupKeyFunction())
        .key_by(lambda e: e["dedup_hash"])
        .process(DedupFilterFunction())
        .map(EnrichmentToOutputFunction())
        .map(SerializeFunction())
    )

    stream.sink_to(kafka_sink)


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(30_000)  # checkpoint every 30s

    meta_path = os.environ.get("NSOE_CAMERA_METADATA_PATH", "/etc/nsoe/camera_metadata.json")
    if os.path.exists(meta_path):
        load_camera_metadata(meta_path)

    build_job(env)
    env.execute("nsoe-cctv-enrichment-job")


if __name__ == "__main__":
    main()
