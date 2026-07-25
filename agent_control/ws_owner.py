"""Marker file so only one process owns WS /ws/agents (shared config volume)."""

from __future__ import annotations

import os
import time
from typing import Optional

_DEFAULT_MARKER = "/app/config/.agent_ws_owner"
_DEFAULT_MAX_AGE_SEC = 90.0


def marker_path() -> str:
    explicit = (os.getenv("AGENT_WS_OWNER_MARKER") or "").strip()
    if explicit:
        return explicit
    cameras = (os.getenv("CAMERAS_CONFIG_PATH") or "").strip()
    if cameras:
        base = os.path.dirname(os.path.abspath(cameras))
        if base:
            return os.path.join(base, ".agent_ws_owner")
    return _DEFAULT_MARKER


def _max_age_sec() -> float:
    raw = (os.getenv("AGENT_WS_OWNER_MAX_AGE_SEC") or "").strip()
    if not raw:
        return _DEFAULT_MAX_AGE_SEC
    try:
        return max(15.0, float(raw))
    except ValueError:
        return _DEFAULT_MAX_AGE_SEC


def claim_ws_owner(owner: str) -> None:
    path = marker_path()
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(owner.strip() + "\n")


def refresh_ws_owner(owner: str) -> None:
    """Rewrite/touch marker so stale detection does not expire an active owner."""
    path = marker_path()
    if not os.path.isfile(path):
        claim_ws_owner(owner)
        return
    try:
        with open(path, "r", encoding="utf-8") as fh:
            current = fh.read().strip()
        if current != owner.strip():
            return
        # Update mtime without changing content when possible.
        os.utime(path, None)
    except OSError:
        claim_ws_owner(owner)


def release_ws_owner(owner: str) -> None:
    path = marker_path()
    try:
        if not os.path.isfile(path):
            return
        with open(path, "r", encoding="utf-8") as fh:
            current = fh.read().strip()
        if current == owner.strip():
            os.unlink(path)
    except OSError:
        pass


def current_ws_owner() -> Optional[str]:
    path = marker_path()
    try:
        if not os.path.isfile(path):
            return None
        age = time.time() - os.path.getmtime(path)
        if age > _max_age_sec():
            return None
        with open(path, "r", encoding="utf-8") as fh:
            value = fh.read().strip()
        return value or None
    except OSError:
        return None
