"""WebSocket client to central backend: register, heartbeat, commands, signaling passthrough."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from streaming_agent.config import AgentConfig
from streaming_agent.health_monitor import collect_metrics
from streaming_agent.mediamtx_client import MediaMTXClient
from streaming_agent.stream_manager import StreamManager

logger = logging.getLogger(__name__)

try:
    import websockets
    from websockets.exceptions import ConnectionClosed
except ImportError as e:
    raise ImportError("Install package 'websockets' for Streaming Agent") from e


def _is_connection_error(exc: BaseException) -> bool:
    """True when the TCP/TLS/WS session is gone (half-open or fully closed)."""
    if isinstance(exc, ConnectionClosed):
        return True
    msg = str(exc).lower()
    needles = (
        "ssl",
        "connection is closed",
        "connection closed",
        "connection reset",
        "broken pipe",
        "eof occurred",
        "transport is closing",
    )
    return any(n in msg for n in needles)


class SignalingClient:
    def __init__(
        self,
        cfg: AgentConfig,
        stream_manager: StreamManager,
        mtx: MediaMTXClient,
    ):
        self.cfg = cfg
        self.stream_manager = stream_manager
        self.mtx = mtx
        self._ws: Any = None
        self._send_lock = asyncio.Lock()
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()

    def _connect_kwargs(self) -> dict[str, Any]:
        headers: list[tuple[str, str]] = []
        if self.cfg.auth_token:
            headers.append(("Authorization", f"Bearer {self.cfg.auth_token}"))
        # Protocol-level pings detect half-open TLS/WS (LB idle timeout, NAT, etc.).
        # ping_interval=None left recv loop alive while send only logged warnings.
        hb = max(5.0, float(self.cfg.heartbeat_interval_sec))
        ping_timeout = min(20.0, max(10.0, hb * 0.8))
        ping_interval = max(ping_timeout + 5.0, hb)
        kwargs: dict[str, Any] = {
            "ping_interval": ping_interval,
            "ping_timeout": ping_timeout,
            "close_timeout": 10,
            "open_timeout": 30,
        }
        if headers:
            # websockets >= 10
            kwargs["additional_headers"] = headers
        return kwargs

    async def _force_reconnect(self, reason: str) -> None:
        """Close WS so the recv loop exits and run() reconnects with backoff."""
        ws = self._ws
        if ws is None:
            return
        logger.warning("Forcing WS reconnect: %s", reason)
        try:
            await ws.close(code=1001, reason=reason[:120])
        except Exception as e:
            logger.debug("ws.close during reconnect: %s", e)

    async def send_json(self, data: dict[str, Any]) -> bool:
        if self._ws is None:
            logger.debug("send_json skipped (no ws): %s", data.get("type"))
            return False
        async with self._send_lock:
            try:
                await self._ws.send(json.dumps(data, ensure_ascii=False))
                return True
            except Exception as e:
                msg_type = data.get("type", "?")
                logger.warning("send_json failed (%s): %s", msg_type, e)
                if _is_connection_error(e):
                    await self._force_reconnect(f"send failed: {msg_type}")
                return False

    async def _heartbeat_loop(self) -> None:
        while not self._stopped.is_set():
            try:
                await asyncio.wait_for(
                    self._stopped.wait(),
                    timeout=self.cfg.heartbeat_interval_sec,
                )
                break
            except asyncio.TimeoutError:
                pass
            mtx_for_metrics = (
                None if self.cfg.delivery == "upstream_rtmp" else self.mtx
            )
            m = collect_metrics(
                mtx_for_metrics, self.stream_manager.streams_active_count()
            )
            ok = await self.send_json(
                {
                    "type": "heartbeat",
                    "cpu": m.get("cpu", 0),
                    "ram": m.get("ram", 0),
                    "streams_active": m.get("streams_active", 0),
                }
            )
            if not ok:
                break

    async def _handle_incoming(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("invalid json: %s", raw[:200])
            return
        if not isinstance(msg, dict):
            return
        t = msg.get("type")
        if t == "ping":
            await self.send_json({"type": "pong"})
            return
        if t == "start_stream":
            cid = msg.get("camera_id")
            sid = msg.get("session_id")
            if cid:
                await self.stream_manager.start_stream(str(cid), session_id=str(sid) if sid else None)
            return
        if t == "stop_stream":
            cid = msg.get("camera_id")
            sid = msg.get("session_id")
            if cid:
                await self.stream_manager.stop_stream(
                    str(cid),
                    force=True,
                    session_id=str(sid) if sid else None,
                )
            return
        if t == "viewer_join":
            cid = msg.get("camera_id")
            if cid:
                await self.stream_manager.viewer_join(str(cid))
            return
        if t == "viewer_leave":
            cid = msg.get("camera_id")
            if cid:
                await self.stream_manager.viewer_leave(str(cid))
            return
        if t == "webrtc_signal":
            # Reserved for future SDP/ICE relay; browser typically uses WHEP URL from stream_status.
            logger.debug("webrtc_signal from backend: %s", msg)
            return
        logger.debug("unhandled message type=%s", t)

    async def on_stream_status(self, camera_id: str, data: dict[str, Any]) -> None:
        await self.send_json(data)

    def _next_backoff_sec(self, attempt: int) -> float:
        seq = [2.0, 5.0, 10.0]
        max_b = self.cfg.reconnect_backoff_max_sec
        if attempt < len(seq):
            return min(seq[attempt], max_b)
        extra = attempt - len(seq) + 1
        return min(max_b, seq[-1] * (2**extra))

    async def run(self) -> None:
        attempt = 0

        while not self._stopped.is_set():
            uri = self.cfg.backend_url
            try:
                logger.info("Connecting backend WS %s", uri)
                connect_fn = websockets.connect
                kwargs = self._connect_kwargs()
                async with connect_fn(uri, **kwargs) as ws:
                    self._ws = ws
                    cams = self.stream_manager.camera_ids()
                    await self.send_json(
                        {
                            "type": "register",
                            "agent_id": self.cfg.agent_id,
                            "cameras": cams,
                        }
                    )
                    logger.info(
                        "WS connected, register sent: agent_id=%s cameras=%s",
                        self.cfg.agent_id,
                        cams,
                    )
                    self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                    attempt = 0
                    try:
                        async for message in ws:
                            if isinstance(message, bytes):
                                message = message.decode("utf-8", errors="replace")
                            await self._handle_incoming(message)
                    finally:
                        self._ws = None
                        if self._heartbeat_task:
                            self._heartbeat_task.cancel()
                            try:
                                await self._heartbeat_task
                            except asyncio.CancelledError:
                                pass
                            self._heartbeat_task = None
            except asyncio.CancelledError:
                raise
            except ConnectionClosed as e:
                if self._stopped.is_set():
                    break
                delay = self._next_backoff_sec(attempt)
                attempt += 1
                logger.warning(
                    "WebSocket closed: attempt=%s code=%s reason=%r (retry in %ss)",
                    attempt,
                    getattr(e, "code", None),
                    getattr(e, "reason", "") or "",
                    delay,
                )
                await asyncio.sleep(delay)
            except Exception as e:
                if self._stopped.is_set():
                    break
                delay = self._next_backoff_sec(attempt)
                attempt += 1
                logger.warning(
                    "%s WebSocket error: %s: %s (retry in %ss)",
                    attempt,
                    type(e).__name__,
                    e,
                    delay,
                )
                await asyncio.sleep(delay)

    def stop(self) -> None:
        self._stopped.set()
