"""WebSocket control client for store agents (desired-state apply)."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
from dataclasses import dataclass
from typing import Any, Callable, Optional

from agent_control.env_file import (
    compute_env_hash,
    merge_env_file,
    read_env_file,
    redact_reported_env,
    reported_env_from_os,
)
from agent_control.update import run_software_update

logger = logging.getLogger(__name__)

try:
    import websockets
    from websockets.exceptions import ConnectionClosed
except ImportError:  # pragma: no cover
    websockets = None  # type: ignore
    ConnectionClosed = Exception  # type: ignore


@dataclass
class ControlCallbacks:
    on_reload_cameras: Optional[Callable[[], None]] = None
    on_env_applied: Optional[Callable[[dict[str, str]], None]] = None
    request_restart: Optional[Callable[[], None]] = None


class AgentControlClient:
    def __init__(
        self,
        *,
        backend_url: str,
        agent_id: str,
        auth_token: str,
        env_path: str,
        callbacks: Optional[ControlCallbacks] = None,
        heartbeat_interval_sec: float = 15.0,
    ) -> None:
        self.backend_url = backend_url
        self.agent_id = agent_id
        self.auth_token = auth_token
        self.env_path = env_path
        self.callbacks = callbacks or ControlCallbacks()
        self.heartbeat_interval_sec = max(5.0, float(heartbeat_interval_sec))
        self.update_status = "idle"
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stopped = threading.Event()
        self._ws: Any = None
        self._send_lock: Optional[asyncio.Lock] = None

    @property
    def version(self) -> str:
        return (
            os.getenv("AGENT_VERSION")
            or os.getenv("IMAGE_TAG")
            or os.getenv("HOSTNAME")
            or "unknown"
        )

    def current_env_hash(self) -> str:
        file_env = read_env_file(self.env_path)
        # Prefer file contents; fall back to process env for missing keys.
        merged = reported_env_from_os()
        merged.update(file_env)
        return compute_env_hash(merged)

    def current_reported_env(self) -> dict[str, str]:
        file_env = read_env_file(self.env_path)
        merged = reported_env_from_os()
        merged.update(file_env)
        return merged

    def start_background(self) -> None:
        if websockets is None:
            logger.warning("websockets not installed; agent control disabled")
            return
        if self._thread and self._thread.is_alive():
            return
        self._stopped.clear()
        self._thread = threading.Thread(target=self._thread_main, name="agent-control", daemon=True)
        self._thread.start()
        logger.info(
            "agent control started agent_id=%s url=%s",
            self.agent_id,
            self.backend_url,
        )

    def stop(self) -> None:
        self._stopped.set()
        loop = self._loop
        if loop and loop.is_running():
            asyncio.run_coroutine_threadsafe(self._close_ws(), loop)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._run())
        finally:
            try:
                loop.close()
            except Exception:
                pass
            self._loop = None

    async def _close_ws(self) -> None:
        ws = self._ws
        if ws is None:
            return
        try:
            await ws.close()
        except Exception:
            pass

    async def _run(self) -> None:
        attempt = 0
        while not self._stopped.is_set():
            try:
                headers = [("Authorization", f"Bearer {self.auth_token}")]
                async with websockets.connect(
                    self.backend_url,
                    additional_headers=headers,
                    ping_interval=None,
                    ping_timeout=60,
                    close_timeout=10,
                    open_timeout=30,
                ) as ws:
                    self._ws = ws
                    self._send_lock = asyncio.Lock()
                    attempt = 0
                    await self._send(
                        {
                            "type": "register",
                            "agent_id": self.agent_id,
                            "version": self.version,
                            "env_hash": self.current_env_hash(),
                            "update_status": self.update_status,
                        }
                    )
                    hb = asyncio.create_task(self._heartbeat_loop())
                    try:
                        async for message in ws:
                            if isinstance(message, bytes):
                                message = message.decode("utf-8", errors="replace")
                            await self._handle_incoming(message)
                    finally:
                        hb.cancel()
                        try:
                            await hb
                        except asyncio.CancelledError:
                            pass
                        self._ws = None
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._stopped.is_set():
                    break
                delay = min(60.0, 2.0 * (attempt + 1))
                attempt += 1
                logger.warning("agent control WS error: %s (retry in %ss)", exc, delay)
                await asyncio.sleep(delay)

    async def _send(self, payload: dict[str, Any]) -> bool:
        if self._ws is None or self._send_lock is None:
            return False
        async with self._send_lock:
            try:
                await self._ws.send(json.dumps(payload, ensure_ascii=False))
                return True
            except Exception as exc:
                logger.warning("agent control send failed: %s", exc)
                return False

    async def _heartbeat_loop(self) -> None:
        while not self._stopped.is_set():
            await asyncio.sleep(self.heartbeat_interval_sec)
            await self._send(
                {
                    "type": "heartbeat",
                    "cpu": None,
                    "ram": None,
                    "streams_active": 0,
                    "version": self.version,
                    "env_hash": self.current_env_hash(),
                    "update_status": self.update_status,
                }
            )

    async def _handle_incoming(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(msg, dict):
            return
        t = msg.get("type")
        if t in ("ack", "pong", "ping", "error"):
            if t == "ping":
                await self._send({"type": "pong"})
            return
        if t == "apply_env":
            await self._handle_apply_env(msg)
            return
        if t == "software_update":
            await self._handle_software_update(msg)
            return
        if t == "reload_cameras":
            await self._handle_reload_cameras(msg)
            return
        # Ignore stream commands here — streaming-agent owns those when present.
        if t in ("start_stream", "stop_stream", "viewer_join", "viewer_leave", "roi_updated", "snapshot_refresh"):
            logger.debug("control client ignoring stream command type=%s", t)
            return
        logger.debug("control client unhandled type=%s", t)

    async def _handle_apply_env(self, msg: dict[str, Any]) -> None:
        self.update_status = "applying"
        env = msg.get("env") if isinstance(msg.get("env"), dict) else {}
        revision = msg.get("config_revision")
        try:
            merged = await asyncio.to_thread(merge_env_file, self.env_path, env)
            for key, value in merged.items():
                os.environ[key] = value
            env_hash = compute_env_hash(merged)
            self.update_status = "ok"
            await self._send(
                {
                    "type": "config_applied",
                    "ok": True,
                    "config_revision": revision,
                    "env_hash": env_hash,
                    "version": self.version,
                    "update_status": "ok",
                    "reported_env": redact_reported_env(merged),
                }
            )
            if self.callbacks.on_env_applied:
                try:
                    self.callbacks.on_env_applied(merged)
                except Exception as exc:
                    logger.warning("on_env_applied callback failed: %s", exc)
            # Deferred restart so WS ack can flush.
            loop = asyncio.get_running_loop()
            if self.callbacks.request_restart:
                loop.call_later(1.0, self.callbacks.request_restart)
            else:
                loop.call_later(1.0, self._exit_process)
        except Exception as exc:
            self.update_status = "error"
            await self._send(
                {
                    "type": "config_applied",
                    "ok": False,
                    "config_revision": revision,
                    "update_status": "error",
                    "error": str(exc)[:1000],
                }
            )

    async def _handle_software_update(self, msg: dict[str, Any]) -> None:
        version = str(msg.get("version") or "").strip()
        image = msg.get("image")
        if not version:
            await self._send(
                {
                    "type": "config_applied",
                    "ok": False,
                    "update_status": "error",
                    "error": "missing version",
                }
            )
            return
        self.update_status = "applying"
        ok, detail = await asyncio.to_thread(
            run_software_update, version=version, image=str(image) if image else None
        )
        self.update_status = "ok" if ok else "error"
        await self._send(
            {
                "type": "config_applied",
                "ok": ok,
                "version": version if ok else self.version,
                "env_hash": self.current_env_hash(),
                "update_status": self.update_status,
                "error": None if ok else detail,
            }
        )
        if ok:
            loop = asyncio.get_running_loop()
            if self.callbacks.request_restart:
                loop.call_later(2.0, self.callbacks.request_restart)
            else:
                loop.call_later(2.0, self._exit_process)

    async def _handle_reload_cameras(self, msg: dict[str, Any]) -> None:
        try:
            if self.callbacks.on_reload_cameras:
                await asyncio.to_thread(self.callbacks.on_reload_cameras)
            await self._send({"type": "config_applied", "ok": True, "update_status": "ok"})
        except Exception as exc:
            await self._send(
                {
                    "type": "config_applied",
                    "ok": False,
                    "update_status": "error",
                    "error": str(exc)[:1000],
                }
            )

    @staticmethod
    def _exit_process() -> None:
        logger.info("agent control requesting process exit for restart")
        os._exit(0)


def apply_env_message(env_path: str, msg: dict[str, Any]) -> tuple[bool, str, dict[str, str]]:
    """Synchronous apply_env for reuse from streaming_agent signaling client."""
    env = msg.get("env") if isinstance(msg.get("env"), dict) else {}
    try:
        merged = merge_env_file(env_path, env)
        for key, value in merged.items():
            os.environ[key] = value
        return True, compute_env_hash(merged), redact_reported_env(merged)
    except Exception as exc:
        return False, str(exc), {}
