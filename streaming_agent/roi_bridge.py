"""Call sender-crop ROI/snapshot handlers via local unix socket (no HTTP endpoints)."""

from __future__ import annotations

import json
import os
import socket
from typing import Any


def _socket_path() -> str:
    return os.getenv("SENDER_ROI_SOCKET_PATH", "/tmp/sender-roi.sock").strip() or "/tmp/sender-roi.sock"


def _send_command(body: dict[str, Any]) -> bool:
    path = _socket_path()
    data = (json.dumps(body, ensure_ascii=False) + "\n").encode("utf-8")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(10.0)
        sock.connect(path)
        sock.sendall(data)
        raw = b""
        while b"\n" not in raw and len(raw) < 65536:
            chunk = sock.recv(4096)
            if not chunk:
                break
            raw += chunk
    line = raw.split(b"\n", 1)[0].decode("utf-8").strip()
    if not line:
        return False
    resp = json.loads(line)
    return bool(isinstance(resp, dict) and resp.get("ok"))


def request_snapshot_refresh(smartcamera_id: Any) -> bool:
    return _send_command(
        {"action": "snapshot_refresh", "smartcamera_id": str(smartcamera_id).strip()}
    )


def apply_roi_payload(payload: dict[str, Any]) -> bool:
    body = dict(payload)
    body["action"] = "roi_apply"
    return _send_command(body)
