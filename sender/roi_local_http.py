"""HTTP fallback for ROI IPC when unix socket is unavailable."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from main_agent import MainAgent


class _Handler(BaseHTTPRequestHandler):
    main_agent: Optional["MainAgent"] = None

    def do_POST(self) -> None:
        path = self.path.rstrip("/")
        agent = _Handler.main_agent
        if agent is None:
            self.send_error(503)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length > 0 else b"{}"
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except (ValueError, json.JSONDecodeError):
            self.send_error(400)
            return
        if not isinstance(body, dict):
            self.send_error(400)
            return

        if path == "/cameras/reload":
            try:
                agent._reload_cameras_from_backend()
                payload = {"ok": True, "reloaded": True, "cameras": len(agent.capture_agents)}
            except Exception as e:
                payload = {"ok": False, "error": str(e)[:500]}
            self._write_json(payload)
            return

        if path == "/control/restart":
            threading.Thread(
                target=agent._request_control_restart,
                daemon=True,
                name="control-restart",
            ).start()
            self._write_json({"ok": True, "restarting": True})
            return

        cam_id = str(
            body.get("smartcamera_id") or body.get("camera_id") or ""
        ).strip()
        if not cam_id:
            self.send_error(400)
            return

        if path == "/snapshot/refresh":
            ok = agent.request_snapshot_refresh(cam_id)
            payload = {"ok": ok, "queued": ok}
        elif path == "/roi/apply":
            ok = agent.apply_roi_from_payload(body)
            payload = {"ok": ok, "applied": ok}
        else:
            self.send_error(404)
            return

        self._write_json(payload)

    def _write_json(self, payload: dict) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args) -> None:
        return


class RoiLocalHttpServer:
    def __init__(self, main_agent: "MainAgent", host: str = "127.0.0.1", port: int = 18765):
        self._main_agent = main_agent
        self._host = host
        self._port = port
        self._httpd: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> bool:
        if self._thread and self._thread.is_alive():
            return True
        _Handler.main_agent = self._main_agent
        try:
            self._httpd = ThreadingHTTPServer((self._host, self._port), _Handler)
        except OSError as e:
            print(f"[ROI HTTP] Cannot bind {self._host}:{self._port}: {e}")
            return False
        self._thread = threading.Thread(
            target=self._httpd.serve_forever,
            daemon=True,
            name="roi-local-http",
        )
        self._thread.start()
        print(f"[ROI HTTP] Fallback http://{self._host}:{self._port}")
        return True

    def stop(self) -> None:
        if self._httpd:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3.0)
        self._thread = None
