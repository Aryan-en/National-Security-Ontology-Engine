"""
Camera adapter service.
Handles RTSP ingestion, ONVIF discovery, Hikvision SDK wrapper,
and reconnection with exponential backoff.
"""

from __future__ import annotations

import threading
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Generator, Optional

import cv2
import structlog
from tenacity import retry, stop_after_delay, wait_exponential

logger = structlog.get_logger(__name__)


class CameraType(str, Enum):
    RTSP = "rtsp"
    ONVIF = "onvif"
    HIKVISION = "hikvision"
    DAHUA = "dahua"


@dataclass(frozen=True)
class CameraConfig:
    camera_id: str
    camera_type: CameraType
    rtsp_url: str
    username: str
    password: str
    geo_zone_id: str
    latitude: float
    longitude: float
    target_fps: int = 5
    motion_trigger: bool = True
    reconnect_max_wait: int = 60  # seconds


@dataclass
class Frame:
    camera_id: str
    frame_id: str
    frame_data: bytes          # JPEG-encoded
    timestamp_ms: int          # device clock epoch ms
    device_clock_synced: bool
    width: int
    height: int


class CameraAdapter(ABC):
    """Base class for all camera adapters."""

    def __init__(self, config: CameraConfig) -> None:
        self.config = config
        self._running = False
        self._frame_count = 0

    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def disconnect(self) -> None: ...

    @abstractmethod
    def read_frame(self) -> Optional[Frame]: ...

    def is_clock_synced(self) -> bool:
        """Check if system NTP is synced within 2s of GPS reference."""
        try:
            import subprocess
            result = subprocess.run(
                ["chronyc", "tracking"],
                capture_output=True, text=True, timeout=2
            )
            for line in result.stdout.splitlines():
                if "System time" in line:
                    offset_str = line.split(":")[1].strip().split()[0]
                    return abs(float(offset_str)) < 2.0
        except Exception:
            pass
        return False

    def start_streaming(self) -> Generator[Frame, None, None]:
        """Yield frames continuously, reconnecting on failure."""
        self._running = True
        while self._running:
            try:
                self.connect()
                frame_interval = 1.0 / self.config.target_fps
                last_frame_time = 0.0
                while self._running:
                    now = time.monotonic()
                    if now - last_frame_time < frame_interval:
                        time.sleep(0.001)
                        continue
                    frame = self.read_frame()
                    if frame is not None:
                        self._frame_count += 1
                        last_frame_time = now
                        yield frame
            except Exception as exc:
                logger.warning(
                    "camera_stream_error",
                    camera_id=self.config.camera_id,
                    error=str(exc),
                )
                self.disconnect()
                time.sleep(min(2 ** min(self._frame_count, 6), self.config.reconnect_max_wait))

    def stop(self) -> None:
        self._running = False
        self.disconnect()


class RTSPAdapter(CameraAdapter):
    """OpenCV-based RTSP adapter for any ONVIF-compatible camera."""

    def __init__(self, config: CameraConfig) -> None:
        super().__init__(config)
        self._cap: Optional[cv2.VideoCapture] = None
        self._clock_synced: bool = False

    def connect(self) -> None:
        logger.info("rtsp_connecting", camera_id=self.config.camera_id, url=self.config.rtsp_url)
        self._cap = cv2.VideoCapture(self.config.rtsp_url, cv2.CAP_FFMPEG)
        if not self._cap.isOpened():
            raise ConnectionError(f"Cannot open RTSP stream: {self.config.rtsp_url}")
        self._clock_synced = self.is_clock_synced()
        logger.info("rtsp_connected", camera_id=self.config.camera_id, clock_synced=self._clock_synced)

    def disconnect(self) -> None:
        if self._cap is not None:
            self._cap.release()
            self._cap = None

    def read_frame(self) -> Optional[Frame]:
        if self._cap is None:
            return None
        ret, bgr = self._cap.read()
        if not ret or bgr is None:
            raise IOError("Failed to read frame from RTSP stream")
        timestamp_ms = int(time.time() * 1000)
        _, buf = cv2.imencode(".jpg", bgr, [cv2.IMWRITE_JPEG_QUALITY, 85])
        h, w = bgr.shape[:2]
        return Frame(
            camera_id=self.config.camera_id,
            frame_id=str(uuid.uuid4()),
            frame_data=buf.tobytes(),
            timestamp_ms=timestamp_ms,
            device_clock_synced=self._clock_synced,
            width=w,
            height=h,
        )


class MotionTriggeredAdapter(RTSPAdapter):
    """Wraps RTSPAdapter and only yields frames when motion is detected."""

    _MOTION_THRESHOLD = 500  # contour area in pixels

    def __init__(self, config: CameraConfig) -> None:
        super().__init__(config)
        self._bg_subtractor = cv2.createBackgroundSubtractorMOG2(
            history=200, varThreshold=25, detectShadows=False
        )
        self._motion_active = False

    def read_frame(self) -> Optional[Frame]:
        frame = super().read_frame()
        if frame is None:
            return None
        # Decode to check motion
        import numpy as np
        arr = np.frombuffer(frame.frame_data, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        fg_mask = self._bg_subtractor.apply(img)
        contours, _ = cv2.findContours(fg_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        motion_area = sum(cv2.contourArea(c) for c in contours)
        self._motion_active = motion_area > self._MOTION_THRESHOLD
        if not self._motion_active:
            return None
        return frame


def build_adapter(config: CameraConfig) -> CameraAdapter:
    """Factory that selects the appropriate adapter for a camera config."""
    if config.camera_type == CameraType.HIKVISION:
        from nsoe_edge.camera.hikvision_adapter import HikvisionAdapter
        return HikvisionAdapter(config)
    if config.motion_trigger:
        return MotionTriggeredAdapter(config)
    return RTSPAdapter(config)
