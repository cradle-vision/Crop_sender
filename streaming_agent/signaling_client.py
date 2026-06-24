"""WebSocket client to central backend: register, heartbeat, commands, signaling passthrough."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from streaming_agent.config import AgentConfig
from streaming_agent.health_monitor import collect_metrics
from streaming_agent.mediamtx_client import MediaMTXClient
from streaming_agent.roi_bridge import apply_roi_payload, request_snapshot_refresh
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
        # App-level heartbeat (type=heartbeat) keeps agent online on backend.
        # ping_interval=None: avoid WS protocol pings conflicting with some proxies.
        kwargs: dict[str, Any] = {
            "ping_interval": None,
            "ping_timeout": 60,
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

    async def _send_heartbeat(self) -> bool:
        mtx_for_metrics = (
            None if self.cfg.delivery == "upstream_rtmp" else self.mtx
        )
        m = collect_metrics(
            mtx_for_metrics, self.stream_manager.streams_active_count()
        )
        return await self.send_json(
            {
                "type": "heartbeat",
                "agent_id": self.cfg.agent_id,
                "cpu": m.get("cpu", 0),
                "ram": m.get("ram", 0),
                "streams_active": m.get("streams_active", 0),
            }
        )

    async def _heartbeat_loop(self) -> None:
        tick = 0
        while not self._stopped.is_set():
            try:
                await asyncio.wait_for(
                    self._stopped.wait(),
                    timeout=self.cfg.heartbeat_interval_sec,
                )
                break
            except asyncio.TimeoutError:
                pass
            ok = await self._send_heartbeat()
            tick += 1
            if tick == 1 or tick % 4 == 0:
                logger.info(
                    "heartbeat sent agent_id=%s streams_active=%s ok=%s",
                    self.cfg.agent_id,
                    self.stream_manager.streams_active_count(),
                    ok,
                )
            if not ok:
                logger.warning("heartbeat send failed — WS will reconnect")
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
        if t == "pong":
            return
        if t == "ack":
            logger.info("backend ack: %s", msg.get("message", msg))
            return
        if t in ("register_ack", "registered", "agent_online"):
            logger.info("backend ack: %s", msg)
            return
        if t == "start_stream":
            cid = msg.get("camera_id")
            sid = msg.get("session_id")
            logger.info("backend start_stream camera=%s session=%s", cid, sid)
            if cid:
                await self.stream_manager.start_stream(
                    str(cid), session_id=str(sid) if sid else None
                )
            return
        if t == "stop_stream":
            cid = msg.get("camera_id")
            sid = msg.get("session_id")
            logger.info("backend stop_stream camera=%s session=%s", cid, sid)
            if cid:
                await self.stream_manager.stop_stream(
                    str(cid),
                    force=False,
                    session_id=str(sid) if sid else None,
                )
            return
        if t == "viewer_join":
            cid = msg.get("camera_id")
            logger.info("backend viewer_join camera=%s", cid)
            if cid:
                await self.stream_manager.viewer_join(str(cid))
            return
        if t == "viewer_leave":
            cid = msg.get("camera_id")
            logger.info("backend viewer_leave camera=%s", cid)
            if cid:
                await self.stream_manager.viewer_leave(str(cid))
            return
        if t == "webrtc_signal":
            # Reserved for future SDP/ICE relay; browser typically uses WHEP URL from stream_status.
            logger.debug("webrtc_signal from backend: %s", msg)
            return
        if t == "snapshot_refresh":
            await self._handle_snapshot_refresh(msg)
            return
        if t == "roi_updated":
            await self._handle_roi_updated(msg)
            return
        logger.warning("unhandled backend message type=%s keys=%s", t, list(msg.keys()))

    async def on_stream_status(self, camera_id: str, data: dict[str, Any]) -> None:
        await self.send_json(data)

    async def _handle_snapshot_refresh(self, msg: dict[str, Any]) -> None:
        cam_id = msg.get("smartcamera_id") or msg.get("camera_id")
        try:
            logger.info("snapshot_refresh camera=%s", cam_id)
            ok = await asyncio.to_thread(request_snapshot_refresh, cam_id)
            if ok:
                logger.info("snapshot_refresh queued camera=%s", cam_id)
            else:
                logger.warning("snapshot_refresh not queued camera=%s", cam_id)
        except Exception as e:
            logger.warning("snapshot_refresh failed camera=%s: %s", cam_id, e)

    async def _handle_roi_updated(self, msg: dict[str, Any]) -> None:
        """Apply ROI on sender-crop via local IPC (unix socket or HTTP fallback)."""
        cam_id = msg.get("smartcamera_id") or msg.get("camera_id")
        try:
            if msg.get("refresh_snapshot"):
                await self._handle_snapshot_refresh(msg)
            logger.info("roi_updated camera=%s keys=%s", cam_id, list(msg.keys()))
            ok = await asyncio.to_thread(apply_roi_payload, msg)
            if ok:
                logger.info("roi_updated applied camera=%s", cam_id)
            else:
                logger.warning(
                    "roi_updated not applied camera=%s (unknown camera or empty line data)",
                    cam_id,
                )
        except Exception as e:
            logger.warning("roi_updated failed camera=%s: %s", cam_id, e)

    def _next_backoff_sec(self, attempt: int) -> float:
        initial = max(1.0, float(self.cfg.reconnect_backoff_initial_sec))
        max_b = max(initial, float(self.cfg.reconnect_backoff_max_sec))
        seq = [initial, min(max_b, initial * 2.5), min(max_b, initial * 5.0)]
        if attempt < len(seq):
            return seq[attempt]
        extra = attempt - len(seq) + 1
        return min(max_b, seq[-1] * (2**min(extra, 4)))

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
                    await self._send_heartbeat()
                    logger.info("initial heartbeat sent agent_id=%s", self.cfg.agent_id)
                    await self.stream_manager.restore_wanted_streams()
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
