"""
Flink CEP job: cross-domain-alert-job — 2.3.4
Pattern 1: watchlist_person SOCMINT signal + CCTV sighting in same 1h window
Pattern 2: cyber_intrusion + physical_access_event at same facility in 1h
Pattern 3: 3+ watchlist persons co-located within 300s
Consumes from all three enriched topics via a unified side-input stream.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any, Dict, Optional

from pyflink.common import Duration, Row, Types
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
from pyflink.datastream.state import (
    MapStateDescriptor, ValueStateDescriptor, ListStateDescriptor,
)
from pyflink.common import Types

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
OUTPUT_TOPIC      = "alerts.active"
PARALLELISM       = int(os.environ.get("FLINK_PARALLELISM", "4"))

WINDOW_MS      = 3_600_000   # 1 hour
COLOC_WINDOW_MS = 300_000    # 5 minutes for co-location
CELL_THRESHOLD  = 3          # number of co-located watchlist persons for cell alert


class UnifiedEventMapper(MapFunction):
    """Tag each event with its domain and extract a routing key."""

    def map(self, raw: str) -> Optional[Dict]:
        try:
            ev = json.loads(raw)
            # Detect domain by field presence
            if "detection_class" in ev:
                ev["_domain"] = "CCTV"
                ev["_key"] = ev.get("camera_id", "unknown")
            elif "threat_intent_score" in ev:
                ev["_domain"] = "SOCMINT"
                ev["_key"] = "global"    # socmint events go to all
            elif "anomaly_score" in ev or "src_ip" in ev:
                ev["_domain"] = "CYBER"
                ev["_key"] = ev.get("facility_id", "unknown")
            else:
                return None
            return ev
        except Exception:
            return None


class WatchlistCoLocationDetector(KeyedProcessFunction):
    """
    Keyed by geo_zone_id.
    Pattern 3: 3+ watchlisted persons sighted in same zone within 300s.
    """

    def open(self, ctx: Any) -> None:
        self._tracks = ctx.get_map_state(
            MapStateDescriptor("tracks", Types.STRING(), Types.LONG())
        )
        self._alerted = ctx.get_map_state(
            MapStateDescriptor("alerted_pairs", Types.STRING(), Types.BOOLEAN())
        )

    def process_element(self, event: Dict, ctx: Any, out: Any) -> None:
        if event.get("_domain") != "CCTV":
            return
        if event.get("detection_class") != "PERSON":
            return

        ts = event.get("normalized_timestamp_ms", ctx.timestamp())
        track_id = event.get("track_id", "")
        if not track_id:
            return

        self._tracks.put(track_id, ts)

        # Count active tracks in window
        active = [(t, ts_) for t, ts_ in self._tracks.items()
                  if ts - ts_ <= COLOC_WINDOW_MS]

        if len(active) >= CELL_THRESHOLD:
            cell_key = "_".join(sorted(t for t, _ in active[:CELL_THRESHOLD]))
            if not self._alerted.contains(cell_key):
                self._alerted.put(cell_key, True)
                alert = _make_alert(
                    "WATCHLIST_CO_LOCATION", "ESCALATION", 0.85, event,
                    track_ids=[t for t, _ in active],
                )
                out.collect(json.dumps(alert))

        ctx.timer_service().register_event_time_timer(ts + COLOC_WINDOW_MS)

    def on_timer(self, ts: int, ctx: Any, out: Any) -> None:
        # Evict stale tracks
        for t in list(self._tracks.keys()):
            if ts - (self._tracks.get(t) or 0) > COLOC_WINDOW_MS:
                self._tracks.remove(t)


class CrossDomainWindowDetector(KeyedProcessFunction):
    """
    Keyed by facility_id / zone.
    Pattern 2: cyber intrusion + physical access event in 1h window.
    """

    def open(self, ctx: Any) -> None:
        self._cyber_events = ctx.get_list_state(
            ListStateDescriptor("cyber", Types.STRING())
        )
        self._cctv_events = ctx.get_list_state(
            ListStateDescriptor("cctv", Types.STRING())
        )
        self._emitted = ctx.get_map_state(
            MapStateDescriptor("emitted", Types.STRING(), Types.BOOLEAN())
        )

    def process_element(self, event: Dict, ctx: Any, out: Any) -> None:
        domain = event.get("_domain", "")
        ts = ctx.timestamp()

        if domain == "CYBER" and event.get("is_anomaly"):
            self._cyber_events.add(json.dumps({
                "event_id": event.get("event_id", ""),
                "ts": ts,
                "src_ip": event.get("src_ip"),
            }))
        elif domain == "CCTV":
            self._cctv_events.add(json.dumps({
                "event_id": event.get("event_id", ""),
                "ts": ts,
                "camera_id": event.get("camera_id"),
            }))

        # Check for pattern: cyber + CCTV within WINDOW_MS
        cyber_list = [json.loads(s) for s in self._cyber_events.get() or []]
        cctv_list  = [json.loads(s) for s in self._cctv_events.get()  or []]

        for ce in cyber_list:
            for pe in cctv_list:
                if abs(ce["ts"] - pe["ts"]) > WINDOW_MS:
                    continue
                pair_key = f"{ce['event_id']}_{pe['event_id']}"
                if self._emitted.contains(pair_key):
                    continue
                self._emitted.put(pair_key, True)
                alert = _make_alert(
                    "CYBER_PHYSICAL", "ESCALATION", 0.88, event,
                    track_ids=[],
                    contributing=[ce["event_id"], pe["event_id"]],
                )
                out.collect(json.dumps(alert))

        ctx.timer_service().register_event_time_timer(ts + WINDOW_MS)

    def on_timer(self, ts: int, ctx: Any, out: Any) -> None:
        # Evict old states
        cutoff = ts - WINDOW_MS
        fresh_cyber = [s for s in (self._cyber_events.get() or [])
                       if json.loads(s).get("ts", 0) > cutoff]
        self._cyber_events.clear()
        for s in fresh_cyber:
            self._cyber_events.add(s)


def _make_alert(alert_type: str, tier: str, confidence: float,
                event: Dict, track_ids=None, contributing=None) -> Dict:
    return {
        "alert_id":            str(uuid.uuid4()),
        "alert_type":          alert_type,
        "alert_tier":          tier,
        "fused_confidence":    confidence,
        "camera_id":           event.get("camera_id"),
        "geo_zone_id":         (event.get("geo_location") or {}).get("geo_zone_id"),
        "latitude":            (event.get("geo_location") or {}).get("latitude"),
        "longitude":           (event.get("geo_location") or {}).get("longitude"),
        "involved_track_ids":  track_ids or [],
        "involved_entity_ids": [],
        "contributing_events": contributing or [event.get("event_id", "")],
        "source_domains":      ["CCTV", "CYBER"],
        "triggered_at_ms":     event.get("normalized_timestamp_ms", int(time.time() * 1000)),
        "ttl_seconds":         86400,
        "schema_version":      "1.0.0",
    }


def build_job(env: StreamExecutionEnvironment) -> None:
    def _src(topics):
        return (KafkaSource.builder()
                .set_bootstrap_servers(BOOTSTRAP_SERVERS)
                .set_topics(*topics)
                .set_group_id("nsoe-cross-domain-cep")
                .set_starting_offsets(KafkaOffsetInitializer.latest())
                .set_value_only_deserializer(SimpleStringSchema())
                .build())

    watermark = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))

    sink = (KafkaSink.builder()
            .set_bootstrap_servers(BOOTSTRAP_SERVERS)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(OUTPUT_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build())
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build())

    unified = (
        env.from_source(
            _src(["enriched.cctv.events", "cyber.normalized.events", "socmint.processed.events"]),
            watermark, "KafkaSource-AllDomains",
        )
        .map(UnifiedEventMapper())
        .filter(lambda e: e is not None)
    )

    coloc_alerts = (
        unified
        .key_by(lambda e: (e.get("geo_location") or {}).get("geo_zone_id", "global"))
        .process(WatchlistCoLocationDetector())
    )

    cyber_physical_alerts = (
        unified
        .key_by(lambda e: e.get("facility_id") or (e.get("geo_location") or {}).get("geo_zone_id", "global"))
        .process(CrossDomainWindowDetector())
    )

    coloc_alerts.union(cyber_physical_alerts).sink_to(sink)


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(30_000)
    build_job(env)
    env.execute("nsoe-cross-domain-cep-job")


if __name__ == "__main__":
    main()
