"""Stream lifecycle: MediaMTX (local WebRTC) and/or upstream RTMP to central ingest."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from streaming_agent.config import AgentConfig, path_name_for_camera
from streaming_agent.mediamtx_client import MediaMTXClient
from streaming_agent.upstream_pusher import (
    UpstreamPusher,
    format_template,
    make_stream_key,
)

logger = logging.getLogger(__name__)

StreamStatusCallback = Callable[[str, dict[str, Any]], Awaitable[None]]


@dataclass
class _StreamState:
    path_name: str
    rtsp_url: str
    viewers: int = 0
    active: bool = False
    session_id: str | None = None
    stop_task: asyncio.Task[None] | None = None


class StreamManager:
    def __init__(
        self,
        cfg: AgentConfig,
        mtx: MediaMTXClient,
        on_status: StreamStatusCallback | None = None,
    ):
        self.cfg = cfg
        self.mtx = mtx
        self._on_status = on_status
        self._cameras = cfg.camera_rtsp_map()
        self._streams: dict[str, _StreamState] = {}
        self._lock = asyncio.Lock()
        self._upstream: UpstreamPusher | None = None
        if cfg.delivery in ("upstream_rtmp", "both"):
            self._upstream = UpstreamPusher(
                cfg.upstream.ffmpeg_path,
                cfg.upstream.rtmp_url_template,
                rtsp_transport=cfg.mediamtx.rtsp_transport,
                extra_args=cfg.upstream.ffmpeg_extra_args,
                transcode=cfg.upstream.transcode,
                on_process_exit=self._on_upstream_ffmpeg_exit,
            )

    def camera_ids(self) -> list[str]:
        return list(self._cameras.keys())

    def streams_active_count(self) -> int:
        return sum(1 for s in self._streams.values() if s.active)

    async def _on_upstream_ffmpeg_exit(
        self, camera_id: str, exit_code: int, stderr_tail: str
    ) -> None:
        """FFmpeg завершился без нашего SIGTERM — сбрасываем active и шлём stream_error на бэкенд."""
        await self.handle_upstream_ffmpeg_exit(camera_id, exit_code, stderr_tail)

    async def handle_upstream_ffmpeg_exit(
        self, camera_id: str, exit_code: int, stderr_tail: str
    ) -> None:
        async with self._lock:
            st = self._streams.get(camera_id)
            if not st or not st.active:
                return
            st.active = False
            sid = st.session_id
            rtsp_url = st.rtsp_url
        detail = (stderr_tail or "").strip()[-1200:]
        payload: dict[str, Any] = {
            "ok": False,
            "camera_id": camera_id,
            "agent_id": self.cfg.agent_id,
            "delivery": self.cfg.delivery,
            "error": "upstream_ffmpeg_exited",
            "ffmpeg_exit_code": exit_code,
            "detail": detail,
        }
        if sid:
            payload["session_id"] = sid
        sk = make_stream_key(self.cfg.agent_id, camera_id)
        payload["stream_key"] = sk
        await self._emit_status(camera_id, "stream_error", payload)
        logger.warning(
            "stream marked inactive after ffmpeg exit camera=%s code=%s",
            camera_id,
            exit_code,
        )
        # Auto-restart upstream stream on abnormal ffmpeg exit.
        if (
            self._upstream
            and self.cfg.delivery in ("upstream_rtmp", "both")
            and exit_code not in (0, -15, -9)
        ):
            await asyncio.sleep(1.0)
            try:
                ok, err, _ = await self._upstream.start(camera_id, rtsp_url, self.cfg.agent_id)
                if ok:
                    async with self._lock:
                        st2 = self._streams.get(camera_id)
                        if st2:
                            st2.active = True
                    logger.info("upstream auto-restarted camera=%s after exit_code=%s", camera_id, exit_code)
                else:
                    logger.warning(
                        "upstream auto-restart failed camera=%s exit_code=%s err=%s",
                        camera_id,
                        exit_code,
                        err,
                    )
            except Exception:
                logger.exception("upstream auto-restart exception camera=%s", camera_id)

    def _playback_url(self, camera_id: str) -> str | None:
        tpl = self.cfg.upstream.playback_url_template.strip()
        if not tpl:
            return None
        sk = make_stream_key(self.cfg.agent_id, camera_id)
        return format_template(
            tpl,
            stream_key=sk,
            agent_id=self.cfg.agent_id,
            camera_id=camera_id,
        )

    async def start_stream(
        self, camera_id: str, session_id: str | None = None
    ) -> dict[str, Any]:
        d = self.cfg.delivery
        if camera_id not in self._cameras:
            payload = {
                "ok": False,
                "error": f"unknown_camera:{camera_id}",
                "camera_id": camera_id,
            }
            if session_id:
                payload["session_id"] = session_id
            return payload

        if d in ("upstream_rtmp", "both") and not self.cfg.upstream.rtmp_url_template.strip():
            err = "upstream_rtmp_url_template_missing"
            logger.error(err)
            payload = {"ok": False, "error": err, "camera_id": camera_id}
            if session_id:
                payload["session_id"] = session_id
            await self._emit_status(camera_id, "error", payload)
            return payload

        async with self._lock:
            path_name = path_name_for_camera(camera_id)
            rtsp_url = self._cameras[camera_id]
            st = self._streams.get(camera_id)
            if st is None:
                st = _StreamState(path_name=path_name, rtsp_url=rtsp_url)
                self._streams[camera_id] = st
            if session_id:
                st.session_id = session_id

            if st.stop_task and not st.stop_task.done():
                st.stop_task.cancel()
                try:
                    await st.stop_task
                except asyncio.CancelledError:
                    pass
                st.stop_task = None

            if st.active:
                payload = self._already_running_payload(
                    camera_id, path_name, st, session_id=session_id
                )
                await self._emit_status(camera_id, "stream_ready", payload)
                return payload

        # upstream
        up_meta: dict[str, Any] = {}
        if d in ("upstream_rtmp", "both") and self._upstream:
            ok, err, up_meta = await self._upstream.start(
                camera_id, rtsp_url, self.cfg.agent_id
            )
            if not ok:
                payload = {
                    "ok": False,
                    "error": err or "upstream_failed",
                    "camera_id": camera_id,
                    "delivery": d,
                }
                if session_id:
                    payload["session_id"] = session_id
                await self._emit_status(camera_id, "error", payload)
                return payload

        # mediamtx
        if d in ("local_webrtc", "both"):
            ok, err = await asyncio.to_thread(
                self.mtx.add_rtsp_path,
                path_name,
                rtsp_url,
                self.cfg.mediamtx.rtsp_transport,
            )
            if not ok:
                logger.error("start_stream mediamtx failed camera=%s: %s", camera_id, err)
                if d in ("upstream_rtmp", "both") and self._upstream:
                    await self._upstream.stop(camera_id)
                payload = {
                    "ok": False,
                    "error": err or "mediamtx_add_failed",
                    "camera_id": camera_id,
                    "delivery": d,
                }
                if session_id:
                    payload["session_id"] = session_id
                await self._emit_status(camera_id, "error", payload)
                return payload

        async with self._lock:
            st = self._streams.get(camera_id)
            if st:
                st.active = True
        logger.info("stream started camera=%s delivery=%s", camera_id, d)

        sk = make_stream_key(self.cfg.agent_id, camera_id)
        payload: dict[str, Any] = {
            "ok": True,
            "camera_id": camera_id,
            "agent_id": self.cfg.agent_id,
            "delivery": d,
            "path": path_name,
            "stream_key": sk,
            "viewers": st.viewers if st else 0,
        }
        if session_id:
            payload["session_id"] = session_id
        elif st and st.session_id:
            payload["session_id"] = st.session_id
        if d in ("upstream_rtmp", "both"):
            payload.update(up_meta)
            pb = self._playback_url(camera_id)
            if pb:
                payload["playback_url"] = pb
        if d in ("local_webrtc", "both"):
            payload["whep_url"] = self._whep_url(path_name)

        await self._emit_status(camera_id, "stream_ready", payload)
        return payload

    def _already_running_payload(
        self,
        camera_id: str,
        path_name: str,
        st: _StreamState,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        d = self.cfg.delivery
        if session_id:
            st.session_id = session_id
        sk = make_stream_key(self.cfg.agent_id, camera_id)
        payload: dict[str, Any] = {
            "ok": True,
            "camera_id": camera_id,
            "agent_id": self.cfg.agent_id,
            "delivery": d,
            "path": path_name,
            "stream_key": sk,
            "viewers": st.viewers,
            "note": "already_running",
        }
        if st.session_id:
            payload["session_id"] = st.session_id
        pb = self._playback_url(camera_id)
        if pb:
            payload["playback_url"] = pb
        if d in ("local_webrtc", "both"):
            payload["whep_url"] = self._whep_url(path_name)
        return payload

    async def stop_stream(
        self, camera_id: str, force: bool = False, session_id: str | None = None
    ) -> dict[str, Any]:
        d = self.cfg.delivery
        async with self._lock:
            st = self._streams.get(camera_id)
            if not st:
                payload = {"ok": True, "camera_id": camera_id, "note": "not_running"}
                if session_id:
                    payload["session_id"] = session_id
                return payload
            if session_id:
                st.session_id = session_id

            if not st.active:
                payload = {
                    "ok": True,
                    "camera_id": camera_id,
                    "note": "already_stopped",
                    "delivery": d,
                }
                if session_id:
                    payload["session_id"] = session_id
                elif st.session_id:
                    payload["session_id"] = st.session_id
                return payload

            if not force and st.viewers > 0:
                payload = {
                    "ok": False,
                    "error": "has_viewers",
                    "camera_id": camera_id,
                    "viewers": st.viewers,
                }
                if st.session_id:
                    payload["session_id"] = st.session_id
                return payload

            if st.stop_task and not st.stop_task.done():
                st.stop_task.cancel()
                st.stop_task = None

            st.active = False

        if self._upstream and d in ("upstream_rtmp", "both"):
            await self._upstream.stop(camera_id)

        mtx_err = None
        if d in ("local_webrtc", "both"):
            ok, mtx_err = await asyncio.to_thread(self.mtx.delete_path, st.path_name)
            if not ok:
                logger.warning("stop_stream mediamtx delete: %s", mtx_err)
            else:
                logger.info("stream stopped mediamtx camera=%s path=%s", camera_id, st.path_name)

        payload = {
            "ok": True,
            "camera_id": camera_id,
            "delivery": d,
            "error": mtx_err,
        }
        if st.session_id:
            payload["session_id"] = st.session_id
        await self._emit_status(camera_id, "stream_stopped", payload)
        return payload

    async def viewer_join(self, camera_id: str) -> dict[str, Any]:
        if camera_id not in self._cameras:
            return {
                "ok": False,
                "error": "unknown_camera",
                "camera_id": camera_id,
            }
        async with self._lock:
            st = self._streams.get(camera_id)
            if not st:
                self._streams[camera_id] = _StreamState(
                    path_name=path_name_for_camera(camera_id),
                    rtsp_url=self._cameras[camera_id],
                    viewers=1,
                    active=False,
                )
                st = self._streams[camera_id]
            else:
                st.viewers += 1

            if st.stop_task and not st.stop_task.done():
                st.stop_task.cancel()
                st.stop_task = None

        await self.start_stream(camera_id)
        async with self._lock:
            st = self._streams.get(camera_id)
            v = st.viewers if st else 0
        return {"ok": True, "camera_id": camera_id, "viewers": v}

    async def viewer_leave(self, camera_id: str) -> dict[str, Any]:
        async with self._lock:
            st = self._streams.get(camera_id)
            if not st:
                return {"ok": True, "camera_id": camera_id, "viewers": 0}
            prev = st.viewers
            st.viewers = max(0, st.viewers - 1)
            v = st.viewers

            if (
                self.cfg.viewer_idle_stop
                and prev > 0
                and v == 0
                and st.active
            ):
                st.stop_task = asyncio.create_task(
                    self._delayed_stop(camera_id, self.cfg.idle_grace_sec)
                )

        return {"ok": True, "camera_id": camera_id, "viewers": v}

    async def _delayed_stop(self, camera_id: str, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            async with self._lock:
                st = self._streams.get(camera_id)
                if not st or st.viewers > 0:
                    return
            await self.stop_stream(camera_id, force=True)
        except asyncio.CancelledError:
            raise

    def _whep_url(self, path_name: str) -> str:
        base = self.cfg.mediamtx.public_webrtc_base.rstrip("/")
        return f"{base}/{path_name}/whep"

    async def _emit_status(
        self, camera_id: str, kind: str, data: dict[str, Any]
    ) -> None:
        if self._on_status:
            await self._on_status(
                camera_id,
                {"type": "stream_status", "status": kind, **data},
            )
