"""
Local RocksDB event buffer.
Events are written here before Kafka publish.
Supports replay on reconnection and auto-eviction at 80% capacity.
"""

from __future__ import annotations

import json
import os
import struct
import time
from pathlib import Path
from typing import Iterator, List, Optional

import structlog

logger = structlog.get_logger(__name__)

MAX_BUFFER_BYTES = int(os.environ.get("NSOE_BUFFER_MAX_BYTES", 2 * 1024 * 1024 * 1024))  # 2GB default
EVICT_THRESHOLD = 0.80   # evict oldest when >80% capacity
EVICT_TARGET    = 0.60   # evict down to 60%


class RocksDBEventBuffer:
    """
    Durable event buffer backed by RocksDB.
    Key: 8-byte big-endian sequence number
    Value: JSON-serialized event bytes
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._db = None
        self._seq: int = 0
        self._acknowledged_seq: int = 0

    def open(self) -> None:
        try:
            from rocksdict import Rdict, Options
            opts = Options()
            opts.create_if_missing(True)
            self._db = Rdict(self._db_path, opts)
            # Recover sequence from last key
            keys = list(self._db.keys())
            if keys:
                self._seq = max(struct.unpack(">Q", k)[0] for k in keys)
                self._acknowledged_seq = self._seq
            logger.info("rocksdb_opened", path=self._db_path, last_seq=self._seq)
        except ImportError:
            logger.warning("rocksdict_not_available_using_in_memory_fallback")
            self._db = {}

    def close(self) -> None:
        if hasattr(self._db, "close"):
            self._db.close()

    def put(self, event: dict) -> int:
        """Write event to buffer. Returns the sequence number assigned."""
        self._seq += 1
        key = struct.pack(">Q", self._seq)
        value = json.dumps(event).encode()
        self._db[key] = value
        self._maybe_evict()
        return self._seq

    def acknowledge(self, seq: int) -> None:
        """Mark events up to seq as successfully published. They can be evicted."""
        self._acknowledged_seq = max(self._acknowledged_seq, seq)

    def get_unacknowledged(self) -> Iterator[tuple[int, dict]]:
        """Yield (seq, event) for all events not yet acknowledged (for replay)."""
        if isinstance(self._db, dict):
            for key, value in self._db.items():
                seq = struct.unpack(">Q", key)[0]
                if seq > self._acknowledged_seq:
                    yield seq, json.loads(value)
            return
        for key, value in self._db.items():
            seq = struct.unpack(">Q", key)[0]
            if seq > self._acknowledged_seq:
                yield seq, json.loads(value)

    def _maybe_evict(self) -> None:
        if isinstance(self._db, dict):
            return
        try:
            # Approximate size check
            db_path = Path(self._db_path)
            if db_path.exists():
                total_bytes = sum(
                    f.stat().st_size
                    for f in db_path.rglob("*")
                    if f.is_file()
                )
                if total_bytes > MAX_BUFFER_BYTES * EVICT_THRESHOLD:
                    self._evict_to_target(total_bytes)
        except Exception as e:
            logger.warning("eviction_check_failed", error=str(e))

    def _evict_to_target(self, current_bytes: int) -> None:
        target_bytes = MAX_BUFFER_BYTES * EVICT_TARGET
        evicted = 0
        if isinstance(self._db, dict):
            return
        for key, _ in self._db.items():
            seq = struct.unpack(">Q", key)[0]
            if seq <= self._acknowledged_seq:
                del self._db[key]
                evicted += 1
                # Rough estimate: stop after removing enough entries
                current_bytes -= 512
                if current_bytes <= target_bytes:
                    break
        logger.info("buffer_evicted", evicted_count=evicted)
