"""NSOE API package shim for the flat src layout."""

from __future__ import annotations

from pathlib import Path

# Allow imports like nsoe_api.routes.* to resolve against the flat src tree.
__path__.append(str(Path(__file__).resolve().parent.parent))
