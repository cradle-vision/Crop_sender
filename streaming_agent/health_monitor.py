"""System metrics: CPU, RAM, optional stream stats from MediaMTX."""

from __future__ import annotations

import logging
from typing import Any

try:
    import psutil
except ImportError:
    psutil = None  # type: ignore

from streaming_agent.mediamtx_client import MediaMTXClient

logger = logging.getLogger(__name__)


def collect_metrics(
    mtx: MediaMTXClient | None,
    streams_active: int,
) -> dict[str, Any]:
    cpu = ram = None
    if psutil:
        try:
            cpu = int(psutil.cpu_percent(interval=None))
            ram = int(psutil.virtual_memory().percent)
        except Exception as e:
            logger.debug("psutil metrics: %s", e)

    out: dict[str, Any] = {
        "cpu": cpu if cpu is not None else 0,
        "ram": ram if ram is not None else 0,
        "streams_active": streams_active,
    }

    if mtx:
        plist = mtx.list_paths()
        if plist and isinstance(plist, dict):
            items = plist.get("items")
            if isinstance(items, list):
                out["mediamtx_paths"] = len(items)
            elif "itemCount" in plist:
                try:
                    out["mediamtx_paths"] = int(plist["itemCount"])
                except (TypeError, ValueError):
                    pass
    return out
