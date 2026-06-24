"""Call sender-crop ROI/snapshot handlers: unix socket first, HTTP fallback."""

from __future__ import annotations

import json
import os
import socket
import urllib.error
import urllib.request
from typing import Any


def _socket_path() -> str:
    return (
        os.getenv("SENDER_ROI_SOCKET_PATH", "/app/config/sender-roi.sock").strip()
        or "/app/config/sender-roi.sock"
    )


def _http_base() -> str:
    return os.getenv("SENDER_ROI_HTTP_BASE", "http://127.0.0.1:18765").rstrip("/")


def _response_success(payload: Any) -> bool:
    if not isinstance(payload, dict) or not payload.get("ok"):
        return False
    if "applied" in payload:
        return bool(payload.get("applied"))
    if "queued" in payload:
        return bool(payload.get("queued"))
    return True


def _send_via_socket(body: dict[str, Any]) -> bool:
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
    return _response_success(resp)


def _send_via_http(url: str, body: dict[str, Any]) -> bool:
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        if resp.status != 200:
            return False
        payload = json.loads(resp.read().decode("utf-8"))
        return _response_success(payload)


def _send_command(body: dict[str, Any], *, http_path: str) -> bool:
    try:
        return _send_via_socket(body)
    except OSError as e:
        # errno 2 = socket file missing; try HTTP (host network / separate /tmp)
        try:
            return _send_via_http(f"{_http_base()}{http_path}", body)
        except (urllib.error.URLError, OSError, ValueError, json.JSONDecodeError) as http_err:
            raise OSError(
                f"ROI IPC failed: socket ({e}) and HTTP ({http_err}); "
                f"is sender-crop running with SENDER_ROI_IPC_ENABLED=true?"
            ) from http_err


def request_snapshot_refresh(smartcamera_id: Any) -> bool:
    body = {"smartcamera_id": str(smartcamera_id).strip()}
    return _send_command({**body, "action": "snapshot_refresh"}, http_path="/snapshot/refresh")


def apply_roi_payload(payload: dict[str, Any]) -> bool:
    body = dict(payload)
    body["action"] = "roi_apply"
    return _send_command(body, http_path="/roi/apply")
