"""Unix socket IPC: streaming-agent calls functions, sender-crop applies them in-process."""

from __future__ import annotations

import json
import os
import socket
import threading
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from main_agent import MainAgent


def roi_socket_path() -> str:
    return (
        os.getenv("SENDER_ROI_SOCKET_PATH", "/app/config/sender-roi.sock").strip()
        or "/app/config/sender-roi.sock"
    )


class RoiCommandServer:
    def __init__(self, main_agent: "MainAgent", socket_path: Optional[str] = None):
        self._main_agent = main_agent
        self._socket_path = socket_path or roi_socket_path()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._server_sock: Optional[socket.socket] = None

    def start(self) -> bool:
        if self._thread and self._thread.is_alive():
            return True
        path = self._socket_path
        try:
            if os.path.exists(path):
                os.unlink(path)
        except OSError:
            pass
        try:
            self._server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._server_sock.bind(path)
            self._server_sock.listen(8)
            os.chmod(path, 0o666)
        except OSError as e:
            print(f"[ROI IPC] Cannot bind unix socket {path}: {e}")
            return False
        self._stop.clear()
        self._thread = threading.Thread(target=self._serve_loop, daemon=True, name="roi-ipc")
        self._thread.start()
        print(f"[ROI IPC] Listening on unix://{path}")
        return True

    def stop(self) -> None:
        self._stop.set()
        if self._server_sock:
            try:
                self._server_sock.close()
            except OSError:
                pass
            self._server_sock = None
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3.0)
        self._thread = None
        try:
            if os.path.exists(self._socket_path):
                os.unlink(self._socket_path)
        except OSError:
            pass

    def _serve_loop(self) -> None:
        while not self._stop.is_set() and self._server_sock is not None:
            try:
                self._server_sock.settimeout(1.0)
                conn, _ = self._server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                if not self._stop.is_set():
                    print("[ROI IPC] accept error")
                break
            threading.Thread(
                target=self._handle_client,
                args=(conn,),
                daemon=True,
                name="roi-ipc-client",
            ).start()

    def _handle_client(self, conn: socket.socket) -> None:
        try:
            with conn:
                conn.settimeout(5.0)
                raw = b""
                while b"\n" not in raw and len(raw) < 65536:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    raw += chunk
                line = raw.split(b"\n", 1)[0].decode("utf-8").strip()
                if not line:
                    self._reply(conn, {"ok": False, "error": "empty"})
                    return
                body = json.loads(line)
                if not isinstance(body, dict):
                    self._reply(conn, {"ok": False, "error": "invalid json"})
                    return
                action = str(body.get("action", "")).strip()
                cam_id = str(
                    body.get("smartcamera_id") or body.get("camera_id") or ""
                ).strip()
                if not cam_id:
                    self._reply(conn, {"ok": False, "error": "smartcamera_id required"})
                    return
                agent = self._main_agent
                if action == "snapshot_refresh":
                    ok = agent.request_snapshot_refresh(cam_id)
                    self._reply(conn, {"ok": ok, "queued": ok})
                    return
                if action == "roi_apply":
                    ok = agent.apply_roi_from_payload(body)
                    self._reply(conn, {"ok": ok, "applied": ok})
                    return
                self._reply(conn, {"ok": False, "error": f"unknown action: {action}"})
        except Exception as e:
            try:
                self._reply(conn, {"ok": False, "error": str(e)})
            except OSError:
                pass

    @staticmethod
    def _reply(conn: socket.socket, payload: dict) -> None:
        conn.sendall((json.dumps(payload) + "\n").encode("utf-8"))
