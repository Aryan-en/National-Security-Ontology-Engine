"""
YOLOv8n detection module.
Runs TensorRT INT8 quantized inference on Jetson Orin NX.
Target latency: < 30ms at 1080p.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import numpy as np
import structlog

logger = structlog.get_logger(__name__)

# Detection classes of interest
INTEREST_CLASSES = {"person", "bag", "suitcase", "backpack", "car", "truck", "motorcycle", "knife", "gun"}
CLASS_MAP = {
    "person": "PERSON",
    "bag": "BAG",
    "suitcase": "BAG",
    "backpack": "BAG",
    "car": "VEHICLE",
    "truck": "VEHICLE",
    "motorcycle": "VEHICLE",
    "knife": "WEAPON",
    "gun": "WEAPON",
}


@dataclass
class Detection:
    class_name: str          # normalized: PERSON | BAG | VEHICLE | WEAPON | UNKNOWN
    raw_class: str
    confidence: float
    x1: float
    y1: float
    x2: float
    y2: float
    frame_width: int
    frame_height: int

    @property
    def area(self) -> float:
        return (self.x2 - self.x1) * (self.y2 - self.y1)


class YOLODetector:
    """
    Wraps Ultralytics YOLOv8n.
    On Jetson Orin, use the TensorRT engine exported via:
        yolo export model=yolov8n.pt format=engine int8=True imgsz=1080
    """

    def __init__(
        self,
        model_path: str = "yolov8n.pt",
        conf_threshold: float = 0.45,
        iou_threshold: float = 0.45,
        device: str = "cuda",
    ) -> None:
        self.model_path = model_path
        self.conf_threshold = conf_threshold
        self.iou_threshold = iou_threshold
        self.device = device
        self._model = None

    def load(self) -> None:
        from ultralytics import YOLO
        logger.info("yolo_loading", model=self.model_path, device=self.device)
        self._model = YOLO(self.model_path)
        self._model.to(self.device)
        # Warm-up
        dummy = np.zeros((1, 640, 640, 3), dtype=np.uint8)
        self._model.predict(source=dummy, verbose=False)
        logger.info("yolo_ready", model=self.model_path)

    def detect(self, frame_data: bytes, width: int, height: int) -> List[Detection]:
        """Run inference on a JPEG-encoded frame. Returns filtered detections."""
        import cv2

        if self._model is None:
            raise RuntimeError("YOLODetector not loaded. Call load() first.")

        arr = np.frombuffer(frame_data, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)

        results = self._model.predict(
            source=img,
            conf=self.conf_threshold,
            iou=self.iou_threshold,
            verbose=False,
            device=self.device,
        )

        detections: List[Detection] = []
        for result in results:
            for box in result.boxes:
                cls_id = int(box.cls[0])
                raw_class = result.names[cls_id]
                if raw_class not in INTEREST_CLASSES:
                    continue
                x1, y1, x2, y2 = box.xyxy[0].tolist()
                detections.append(Detection(
                    class_name=CLASS_MAP.get(raw_class, "UNKNOWN"),
                    raw_class=raw_class,
                    confidence=float(box.conf[0]),
                    x1=x1 / width,
                    y1=y1 / height,
                    x2=x2 / width,
                    y2=y2 / height,
                    frame_width=width,
                    frame_height=height,
                ))
        return detections
