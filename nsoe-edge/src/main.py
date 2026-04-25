"""
Edge node main entry point.
Wires together: camera adapter → detector → tracker → buffer → Kafka producer.
"""

from __future__ import annotations

import json
import os
import signal
import time
import uuid
from pathlib import Path
from typing import Optional

import structlog
import yaml

from nsoe_edge.buffer.rocksdb_buffer import RocksDBEventBuffer
from nsoe_edge.camera.adapter import build_adapter, CameraConfig, CameraType
from nsoe_edge.config.remote_config import EdgeNodeConfig, RemoteConfigService
from nsoe_edge.detection.detector import YOLODetector
from nsoe_edge.health.health_service import HealthService, _status, FRAMES_PROCESSED, DETECTIONS_TOTAL, KAFKA_PUBLISH_TOTAL
from nsoe_edge.producer.kafka_producer import EdgeKafkaProducer
from nsoe_edge.tracking.tracker import ByteTracker

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ]
)
logger = structlog.get_logger(__name__)

_shutdown = False


def _handle_sigterm(sig: int, frame: object) -> None:
    global _shutdown
    logger.info("shutdown_signal_received")
    _shutdown = True


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def main() -> None:
    config_path = os.environ.get("NSOE_CONFIG", "/etc/nsoe-edge/config.yaml")
    raw = load_config(config_path)
    node_cfg = EdgeNodeConfig(**raw["node"])

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    # Health service
    health = HealthService(port=int(os.environ.get("NSOE_HEALTH_PORT", 8090)))
    health.start()

    # RocksDB buffer
    buffer_path = os.environ.get("NSOE_BUFFER_PATH", "/data/nsoe-edge/buffer")
    Path(buffer_path).mkdir(parents=True, exist_ok=True)
    buf = RocksDBEventBuffer(buffer_path)
    buf.open()

    # Read Avro schema
    schema_path = Path(__file__).parent.parent.parent / "nsoe-pipelines/schemas/edge_cctv_event.avsc"
    avro_schema = schema_path.read_text() if schema_path.exists() else "{}"

    # Kafka producer
    producer = EdgeKafkaProducer(
        bootstrap_servers=node_cfg.kafka_bootstrap_servers,
        schema_registry_url=node_cfg.schema_registry_url,
        ssl_ca_location=raw.get("ssl", {}).get("ca", "/etc/nsoe-edge/certs/ca.crt"),
        ssl_certificate_location=raw.get("ssl", {}).get("cert", "/etc/nsoe-edge/certs/client.crt"),
        ssl_key_location=raw.get("ssl", {}).get("key", "/etc/nsoe-edge/certs/client.key"),
        edge_node_id=node_cfg.edge_node_id,
        buffer=buf,
    )
    producer.connect(avro_schema)
    producer.replay_buffered()  # Replay any events from prior disconnection

    # Detector
    detector = YOLODetector(
        model_path=os.environ.get("NSOE_MODEL_PATH", "yolov8n.pt"),
        conf_threshold=node_cfg.detection_conf_threshold,
        device=os.environ.get("NSOE_DEVICE", "cuda"),
    )
    detector.load()
    _status.detector_loaded = True

    # Remote config service
    if raw.get("config_server", {}).get("url"):
        remote_cfg = RemoteConfigService(
            config_url=raw["config_server"]["url"],
            node_id=node_cfg.edge_node_id,
            api_token=os.environ.get("NSOE_API_TOKEN", ""),
        )
        remote_cfg.start(initial_config=node_cfg)

    # Per-camera setup
    cameras = raw.get("cameras", [])
    for cam_raw in cameras:
        cam_cfg = CameraConfig(
            camera_id=cam_raw["camera_id"],
            camera_type=CameraType(cam_raw.get("type", "rtsp")),
            rtsp_url=cam_raw["rtsp_url"],
            username=cam_raw.get("username", ""),
            password=cam_raw.get("password", ""),
            geo_zone_id=cam_raw["geo_zone_id"],
            latitude=cam_raw["latitude"],
            longitude=cam_raw["longitude"],
            target_fps=node_cfg.target_fps,
            motion_trigger=node_cfg.motion_trigger,
        )
        _run_camera_loop(cam_cfg, detector, buf, producer, node_cfg)


def _run_camera_loop(
    cam_cfg: CameraConfig,
    detector: YOLODetector,
    buf: RocksDBEventBuffer,
    producer: EdgeKafkaProducer,
    node_cfg: EdgeNodeConfig,
) -> None:
    import threading

    def _loop() -> None:
        adapter = build_adapter(cam_cfg)
        tracker = ByteTracker(camera_id=cam_cfg.camera_id)
        _status.cameras_active[cam_cfg.camera_id] = False
        _status.kafka_connected = True

        for frame in adapter.start_streaming():
            if _shutdown:
                adapter.stop()
                break

            _status.cameras_active[cam_cfg.camera_id] = True
            _status.last_frame_ts[cam_cfg.camera_id] = frame.timestamp_ms
            FRAMES_PROCESSED.labels(camera_id=cam_cfg.camera_id).inc()

            t0 = time.monotonic()
            detections = detector.detect(frame.frame_data, frame.width, frame.height)
            latency_ms = (time.monotonic() - t0) * 1000
            from nsoe_edge.health.health_service import INFERENCE_LATENCY
            INFERENCE_LATENCY.labels(camera_id=cam_cfg.camera_id).set(latency_ms)

            if not detections:
                continue

            tracks = tracker.update(detections, frame.timestamp_ms)

            for det, track in zip(detections, tracks[:len(detections)]):
                DETECTIONS_TOTAL.labels(camera_id=cam_cfg.camera_id, **{"class": det.class_name}).inc()
                event = {
                    "event_id": str(uuid.uuid4()),
                    "edge_node_id": node_cfg.edge_node_id,
                    "camera_id": cam_cfg.camera_id,
                    "track_id": track.track_id,
                    "detection_class": det.class_name,
                    "detection_confidence": det.confidence,
                    "bbox": {"x1": det.x1, "y1": det.y1, "x2": det.x2, "y2": det.y2},
                    "frame_timestamp_ms": frame.timestamp_ms,
                    "device_clock_synced": frame.device_clock_synced,
                    "server_receipt_timestamp_ms": None,
                    "face_embedding": None,
                    "schema_version": "1.0.0",
                }
                seq = buf.put(event)
                try:
                    producer.publish(event, seq)
                    KAFKA_PUBLISH_TOTAL.labels(camera_id=cam_cfg.camera_id).inc()
                except Exception as exc:
                    logger.error("publish_error", error=str(exc), camera_id=cam_cfg.camera_id)

        _status.cameras_active[cam_cfg.camera_id] = False

    t = threading.Thread(target=_loop, daemon=True, name=f"cam-{cam_cfg.camera_id}")
    t.start()


if __name__ == "__main__":
    main()
