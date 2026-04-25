"""
Remote configuration service.
Polls central config endpoint and applies changes without restart where possible.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

import requests
import structlog
import yaml
from pydantic import BaseModel, Field

logger = structlog.get_logger(__name__)


class EdgeNodeConfig(BaseModel):
    edge_node_id: str
    target_fps: int = Field(default=5, ge=1, le=10)
    motion_trigger: bool = True
    detection_conf_threshold: float = Field(default=0.45, ge=0.1, le=1.0)
    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    buffer_max_bytes: int = 2_147_483_648
    classification_level: str = "RESTRICTED"
    watchlist_refresh_interval_s: int = 300
    log_level: str = "INFO"
    config_version: str = "0.0.0"


class RemoteConfigService:
    """
    Periodically fetches EdgeNodeConfig from the central management API.
    Notifies registered handlers when specific fields change.
    """

    def __init__(
        self,
        config_url: str,
        node_id: str,
        api_token: str,
        poll_interval_s: int = 60,
    ) -> None:
        self._config_url = config_url
        self._node_id = node_id
        self._api_token = api_token
        self._poll_interval_s = poll_interval_s
        self._current: Optional[EdgeNodeConfig] = None
        self._handlers: Dict[str, list[Callable]] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self, initial_config: Optional[EdgeNodeConfig] = None) -> None:
        self._current = initial_config
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        logger.info("remote_config_service_started", node_id=self._node_id)

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def register_handler(self, field_name: str, handler: Callable[[Any, Any], None]) -> None:
        """Register a callback invoked with (old_value, new_value) when field changes."""
        self._handlers.setdefault(field_name, []).append(handler)

    @property
    def current(self) -> Optional[EdgeNodeConfig]:
        return self._current

    def _poll_loop(self) -> None:
        while self._running:
            try:
                self._fetch_and_apply()
            except Exception as e:
                logger.warning("config_fetch_failed", error=str(e))
            time.sleep(self._poll_interval_s)

    def _fetch_and_apply(self) -> None:
        resp = requests.get(
            f"{self._config_url}/nodes/{self._node_id}/config",
            headers={"Authorization": f"Bearer {self._api_token}"},
            timeout=10,
        )
        resp.raise_for_status()
        new_config = EdgeNodeConfig(**resp.json())

        if self._current is None or new_config.config_version != self._current.config_version:
            self._apply(new_config)

    def _apply(self, new_config: EdgeNodeConfig) -> None:
        old = self._current
        logger.info(
            "config_updated",
            old_version=old.config_version if old else "none",
            new_version=new_config.config_version,
        )
        for field_name, handlers in self._handlers.items():
            old_val = getattr(old, field_name, None) if old else None
            new_val = getattr(new_config, field_name, None)
            if old_val != new_val:
                for h in handlers:
                    try:
                        h(old_val, new_val)
                    except Exception as e:
                        logger.error("config_handler_error", field=field_name, error=str(e))
        self._current = new_config
