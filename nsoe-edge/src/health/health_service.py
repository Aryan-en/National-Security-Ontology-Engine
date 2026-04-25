"""
Health check service for K3s liveness and readiness probes.
Exposes /healthz (liveness) and /readyz (readiness) on a lightweight HTTP server.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable, Dict, List

import structlog
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST

logger = structlog.get_logger(__name__)

# Prometheus metrics
FRAMES_PROCESSED = Counter("nsoe_edge_frames_processed_total", "Total frames processed", ["camera_id"])
DETECTIONS_TOTAL = Counter("nsoe_edge_detections_total", "Total detections", ["camera_id", "class"])
KAFKA_PUBLISH_TOTAL = Counter("nsoe_edge_kafka_published_total", "Events published to Kafka", ["camera_id"])
KAFKA_PUBLISH_ERRORS = Counter("nsoe_edge_kafka_errors_total", "Kafka publish errors", ["camera_id"])
BUFFER_SIZE_BYTES = Gauge("nsoe_edge_buffer_bytes", "RocksDB buffer size in bytes")
INFERENCE_LATENCY = Gauge("nsoe_edge_inference_latency_ms", "Last inference latency in ms", ["camera_id"])


@dataclass
class HealthStatus:
    kafka_connected: bool = False
    cameras_active: Dict[str, bool] = field(default_factory=dict)
    detector_loaded: bool = False
    last_frame_ts: Dict[str, int] = field(default_factory=dict)

    @property
    def is_live(self) -> bool:
        """Liveness: the process is not stuck. True unless detector failed to load."""
        return self.detector_loaded

    @property
    def is_ready(self) -> bool:
        """Readiness: at least one camera active and Kafka connected."""
        return self.kafka_connected and any(self.cameras_active.values())


_status = HealthStatus()


def get_health_status() -> HealthStatus:
    return _status


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: object) -> None:
        pass  # suppress default HTTP logging

    def do_GET(self) -> None:
        if self.path == "/healthz":
            code = 200 if _status.is_live else 503
            self._respond(code, b"ok" if code == 200 else b"not live")
        elif self.path == "/readyz":
            code = 200 if _status.is_ready else 503
            self._respond(code, b"ok" if code == 200 else b"not ready")
        elif self.path == "/metrics":
            body = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(body)
        else:
            self._respond(404, b"not found")

    def _respond(self, code: int, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(body)


class HealthService:
    def __init__(self, port: int = 8090) -> None:
        self._port = port
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._server = HTTPServer(("0.0.0.0", self._port), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info("health_service_started", port=self._port)

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
