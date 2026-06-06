"""Stream lifecycle: MediaMTX (local WebRTC) and/or upstream RTMP to central ingest."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

# FFmpeg exits that usually mean RTMP slot still held — extra wait before republish.
_RTMP_FAST_FAIL_EXIT = frozenset({251, 224, 187, -32})
_MAX_UI_ERROR_ATTEMPTS = 3
_STABLE_UPTIME_SEC = 45.0
_CORRUPT_SUBSTREAM_MARKERS = (
    "corrupt decoded",
    "cabac decode",
    "no frame!",
    "invalid data found when processing input",
    "error while decoding mb",
)
_RTSP_DROP_MARKERS = (
    "end of file",
    "failed reading rtsp",
    "method setup failed",
    "server returned 4",
)

from streaming_agent.config import (
    AgentConfig,
    path_name_for_camera,
    relay_path_for_camera,
    relay_rtsp_url,
)
from streaming_agent.config import _STREAMING_FALLBACK_SCALE
from streaming_agent.rtsp_urls import (
    cam_webs_concurrent_stream_url,
    cam_webs_major_url,
    is_cam_webs_style_main,
)
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
    sessions: set[str] = field(default_factory=set)
    stop_task: asyncio.Task[None] | None = None


class StreamManager:
    def __init__(
        self,
        cfg: AgentConfig,
        mtx: MediaMTXClient,
        on_status: StreamStatusCallback | None = None,
        config_path: str | None = None,
    ):
        import os

        self.cfg = cfg
        self.mtx = mtx
        self._on_status = on_status
        self._config_path = (
            config_path
            or os.getenv("STREAMING_AGENT_CONFIG")
            or "config/streaming-agent.yaml"
        )
        self._cameras = dict(cfg.camera_rtsp_map())
        self._camera_scales = dict(cfg.camera_scale_map())
        self._camera_transports = dict(cfg.camera_transport_map())
        self._camera_mains = cfg.camera_main_rtsp_map()
        self._relay_active: set[str] = set()
        self._streams: dict[str, _StreamState] = {}
        self._streaming_wanted: set[str] = set()
        self._restart_attempts: dict[str, int] = {}
        self._stream_start_mono: dict[str, float] = {}
        self._restart_tasks: dict[str, asyncio.Task[None]] = {}
        self._last_stop_mono: dict[str, float] = {}
        self._last_rtmp_fail_mono: dict[str, float] = {}
        self._start_locks: dict[str, asyncio.Lock] = {}
        self._lock = asyncio.Lock()
        self._upstream: UpstreamPusher | None = None
        if cfg.delivery in ("upstream_rtmp", "both"):
            self._upstream = UpstreamPusher(
                cfg.upstream.ffmpeg_path,
                cfg.upstream.rtmp_url_template,
                rtsp_transport=cfg.mediamtx.rtsp_transport,
                extra_args=cfg.upstream.ffmpeg_extra_args,
                transcode=cfg.upstream.transcode,
                transcode_opts=cfg.upstream.transcode_opts,
                on_process_exit=self._on_upstream_ffmpeg_exit,
            )

    def camera_ids(self) -> list[str]:
        return list(self._cameras.keys())

    def reload_cameras_from_config(self) -> bool:
        """Re-read cameras.yaml without ffprobe (instant — no 10s+ block per start)."""
        try:
            from streaming_agent.config import (
                CameraEntry,
                _resolve_streaming_rtsp_url,
                load_config,
            )

            prev = {c.id: c for c in self.cfg.cameras}
            cfg = load_config(self._config_path, probe_cameras=False)
            merged: list[CameraEntry] = []
            for c in cfg.cameras:
                p = prev.get(c.id)
                if c.streaming_rtsp_url.strip():
                    merged.append(
                        CameraEntry(
                            id=c.id,
                            rtsp_url=c.streaming_rtsp_url.strip(),
                            main_rtsp_url=(c.main_rtsp_url or c.rtsp_url).strip(),
                            streaming_rtsp_url=c.streaming_rtsp_url.strip(),
                            rtsp_vendor=c.rtsp_vendor or (p.rtsp_vendor if p else ""),
                            streaming_scale_filter=(
                                c.streaming_scale_filter
                                or (p.streaming_scale_filter if p else "")
                            ),
                            streaming_rtsp_transport=(
                                c.streaming_rtsp_transport
                                or (p.streaming_rtsp_transport if p else "")
                            ),
                        )
                    )
                elif p:
                    merged.append(
                        CameraEntry(
                            id=c.id,
                            rtsp_url=p.rtsp_url,
                            main_rtsp_url=(c.main_rtsp_url or c.rtsp_url).strip(),
                            streaming_rtsp_url=p.streaming_rtsp_url,
                            rtsp_vendor=c.rtsp_vendor or p.rtsp_vendor,
                            streaming_scale_filter=p.streaming_scale_filter,
                            streaming_rtsp_transport=p.streaming_rtsp_transport,
                        )
                    )
                else:
                    merged.append(
                        _resolve_streaming_rtsp_url(
                            c,
                            use_substream=cfg.use_substream,
                            ffmpeg_path=cfg.upstream.ffmpeg_path,
                        )
                    )
            cfg.cameras = merged
            self.cfg = cfg
            self._cameras = dict(cfg.camera_rtsp_map())
            self._camera_scales = dict(cfg.camera_scale_map())
            self._camera_transports = dict(cfg.camera_transport_map())
            self._camera_mains = cfg.camera_main_rtsp_map()
            for camera_id, url in self._cameras.items():
                st = self._streams.get(camera_id)
                if st:
                    st.rtsp_url = url
            logger.info("camera config reloaded ids=%s", list(self._cameras.keys()))
            return True
        except Exception:
            logger.exception("camera config reload failed")
            return False

    def streams_active_count(self) -> int:
        if self._upstream:
            return self._upstream.active_count()
        return sum(1 for s in self._streams.values() if s.active)

    def _ffmpeg_running(self, camera_id: str) -> bool:
        return bool(self._upstream and self._upstream.is_running(camera_id))

    def _start_lock(self, camera_id: str) -> asyncio.Lock:
        if camera_id not in self._start_locks:
            self._start_locks[camera_id] = asyncio.Lock()
        return self._start_locks[camera_id]

    def _publisher_uptime(self, camera_id: str) -> float:
        started = self._stream_start_mono.get(camera_id)
        if started is None:
            return 0.0
        return max(0.0, time.monotonic() - started)

    async def _wait_rtmp_republish_slot(self, camera_id: str) -> None:
        """After stop or RTMP drop, ingest may still hold the stream key briefly."""
        cooldown = max(5.0, float(self.cfg.rtmp_republish_cooldown_sec))
        now = time.monotonic()
        last = max(
            self._last_stop_mono.get(camera_id, 0.0),
            self._last_rtmp_fail_mono.get(camera_id, 0.0),
        )
        if last <= 0:
            return
        wait = cooldown - (now - last)
        if wait > 0:
            logger.info(
                "camera %s: RTMP republish cooldown %.1fs (reopen/restart)",
                camera_id,
                wait,
            )
            await asyncio.sleep(wait)

    def _sync_active_flag(self, camera_id: str) -> None:
        """Keep stream state aligned with the real FFmpeg process."""
        st = self._streams.get(camera_id)
        if not st:
            return
        running = self._ffmpeg_running(camera_id)
        if st.active and not running:
            st.active = False

    async def _on_upstream_ffmpeg_exit(
        self, camera_id: str, exit_code: int, stderr_tail: str
    ) -> None:
        """FFmpeg завершился без нашего SIGTERM — сбрасываем active и шлём stream_error на бэкенд."""
        await self.handle_upstream_ffmpeg_exit(camera_id, exit_code, stderr_tail)

    def _maybe_fallback_from_corrupt_substream(
        self, camera_id: str, stderr_tail: str
    ) -> None:
        """Runtime switch: bad substream → main URL + downscale (camera 9 style issues)."""
        if self._camera_scales.get(camera_id, "").strip():
            return
        low = (stderr_tail or "").lower()
        if not any(m in low for m in _CORRUPT_SUBSTREAM_MARKERS):
            return
        main = self._camera_mains.get(camera_id, "").strip()
        if not main or self._cameras.get(camera_id) == main:
            return
        self._cameras[camera_id] = main
        self._camera_scales[camera_id] = _STREAMING_FALLBACK_SCALE
        st = self._streams.get(camera_id)
        if st:
            st.rtsp_url = main
        logger.warning(
            "camera %s: corrupt substream detected — switching to main + downscale 640x360",
            camera_id,
        )

    def _restore_canonical_rtsp_url(self, camera_id: str) -> str:
        """Config-resolved URL — never keep runtime mutations that break Cam-Webs."""
        for entry in self.cfg.cameras:
            if entry.id == camera_id:
                url = entry.rtsp_url.strip()
                self._cameras[camera_id] = url
                st = self._streams.get(camera_id)
                if st:
                    st.rtsp_url = url
                return url
        return self._cameras.get(camera_id, "")

    def _maybe_fixup_rtsp_after_drop(
        self, camera_id: str, stderr_tail: str
    ) -> None:
        """Cam-Webs: never switch to main/h264major (sender-crop already uses it)."""
        low = (stderr_tail or "").lower()
        if not any(m in low for m in _RTSP_DROP_MARKERS):
            return
        main = self._camera_mains.get(camera_id, "").strip()
        if not is_cam_webs_style_main(main, ""):
            return
        current = self._cameras.get(camera_id, "").strip().lower()
        if "/streaming/channels/102" in current:
            logger.info(
                "camera %s: RTSP drop on concurrent /102 — will retry same URL",
                camera_id,
            )
            return
        fixed = cam_webs_concurrent_stream_url(main)
        if not fixed or fixed == self._cameras.get(camera_id):
            return
        self._cameras[camera_id] = fixed
        self._camera_scales.setdefault(camera_id, _STREAMING_FALLBACK_SCALE)
        st = self._streams.get(camera_id)
        if st:
            st.rtsp_url = fixed
        logger.warning(
            "camera %s: RTSP drop — reverting to concurrent stream %s",
            camera_id,
            fixed.split("@")[-1] if "@" in fixed else fixed,
        )

    def _cancel_restart_task(self, camera_id: str) -> None:
        task = self._restart_tasks.pop(camera_id, None)
        if task and not task.done():
            task.cancel()

    def _stream_rtsp_url(self, camera_id: str) -> str:
        return self._cameras.get(camera_id, "")

    def _stream_scale_filter(self, camera_id: str) -> str:
        return self._camera_scales.get(camera_id, "")

    def _stream_rtsp_transport(self, camera_id: str) -> str:
        return self._camera_transports.get(camera_id, "tcp")

    def _use_rtsp_relay(self) -> bool:
        return bool(
            self.cfg.mediamtx.rtsp_relay_enabled
            and self.cfg.delivery in ("upstream_rtmp", "both")
        )

    async def _ensure_rtsp_relay(self, camera_id: str, camera_rtsp_url: str) -> tuple[bool, str]:
        """MediaMTX pulls camera once; FFmpeg reads stable localhost RTSP."""
        if not self._use_rtsp_relay():
            return True, camera_rtsp_url

        path = relay_path_for_camera(camera_id)
        transport = self._stream_rtsp_transport(camera_id)
        ok, err = await asyncio.to_thread(
            self.mtx.ensure_relay_path,
            path,
            camera_rtsp_url,
            transport,
        )
        if not ok:
            logger.error(
                "relay setup failed camera=%s path=%s err=%s — direct camera RTSP",
                camera_id,
                path,
                err,
            )
            return True, camera_rtsp_url

        self._relay_active.add(camera_id)
        local_url = relay_rtsp_url(self.cfg.mediamtx.rtsp_listen_base, camera_id)
        ready = await asyncio.to_thread(self.mtx.wait_path_ready, path, 12.0)
        if not ready:
            logger.warning(
                "relay not ready yet camera=%s path=%s — starting FFmpeg anyway",
                camera_id,
                path,
            )
        logger.info(
            "relay ready camera=%s path=%s transport=%s -> %s",
            camera_id,
            path,
            transport,
            local_url.split("@")[-1] if "@" in local_url else local_url,
        )
        return True, local_url

    async def _release_rtsp_relay(self, camera_id: str) -> None:
        if camera_id not in self._relay_active:
            return
        path = relay_path_for_camera(camera_id)
        ok, err = await asyncio.to_thread(self.mtx.delete_path, path)
        self._relay_active.discard(camera_id)
        if ok:
            logger.info("relay released camera=%s path=%s", camera_id, path)
        else:
            logger.warning("relay release failed camera=%s path=%s err=%s", camera_id, path, err)

    async def handle_upstream_ffmpeg_exit(
        self, camera_id: str, exit_code: int, stderr_tail: str
    ) -> None:
        if camera_id not in self._streaming_wanted:
            return

        if self._camera_scales.get(camera_id, "").strip():
            # Cam-Webs /102 path: do not switch to main on corrupt frames.
            pass
        else:
            self._maybe_fallback_from_corrupt_substream(camera_id, stderr_tail)
        self._maybe_fixup_rtsp_after_drop(camera_id, stderr_tail)
        self._restore_canonical_rtsp_url(camera_id)

        async with self._lock:
            st = self._streams.get(camera_id)
            if not st:
                sid = None
            else:
                st.active = False
                sid = st.session_id
            rtsp_url = self._stream_rtsp_url(camera_id)

        started_mono = self._stream_start_mono.get(camera_id)
        uptime = (
            time.monotonic() - started_mono if started_mono is not None else 0.0
        )
        if uptime >= _STABLE_UPTIME_SEC:
            self._restart_attempts[camera_id] = 0

        if exit_code in _RTMP_FAST_FAIL_EXIT:
            self._last_rtmp_fail_mono[camera_id] = time.monotonic()

        detail = (stderr_tail or "").strip()[-1200:]
        sk = make_stream_key(self.cfg.agent_id, camera_id)
        will_restart = (
            camera_id in self._streaming_wanted
            and self._upstream is not None
            and self.cfg.delivery in ("upstream_rtmp", "both")
            and exit_code not in (-15, -9)
        )

        logger.warning(
            "stream marked inactive after ffmpeg exit camera=%s code=%s uptime=%.1fs",
            camera_id,
            exit_code,
            uptime,
        )

        if will_restart:
            attempt = self._restart_attempts.get(camera_id, 0) + 1
            self._restart_attempts[camera_id] = attempt
            # Avoid flashing stream_error on every brief RTMP glitch while we recover.
            if attempt <= _MAX_UI_ERROR_ATTEMPTS:
                await self._emit_status(
                    camera_id,
                    "stream_reconnecting",
                    {
                        "ok": True,
                        "camera_id": camera_id,
                        "agent_id": self.cfg.agent_id,
                        "delivery": self.cfg.delivery,
                        "stream_key": sk,
                        "ffmpeg_exit_code": exit_code,
                        "attempt": attempt,
                        "detail": detail,
                        **({"session_id": sid} if sid else {}),
                    },
                )
            else:
                await self._emit_status(
                    camera_id,
                    "stream_error",
                    {
                        "ok": False,
                        "camera_id": camera_id,
                        "agent_id": self.cfg.agent_id,
                        "delivery": self.cfg.delivery,
                        "error": "upstream_ffmpeg_exited",
                        "ffmpeg_exit_code": exit_code,
                        "detail": detail,
                        "stream_key": sk,
                        "attempt": attempt,
                        **({"session_id": sid} if sid else {}),
                    },
                )
            self._schedule_upstream_restart(
                camera_id, rtsp_url, exit_code, sid, uptime=uptime
            )
            return

        await self._emit_status(
            camera_id,
            "stream_error",
            {
                "ok": False,
                "camera_id": camera_id,
                "agent_id": self.cfg.agent_id,
                "delivery": self.cfg.delivery,
                "error": "upstream_ffmpeg_exited",
                "ffmpeg_exit_code": exit_code,
                "detail": detail,
                "stream_key": sk,
                **({"session_id": sid} if sid else {}),
            },
        )

    def _schedule_upstream_restart(
        self,
        camera_id: str,
        rtsp_url: str,
        exit_code: int,
        session_id: str | None,
        uptime: float = 0.0,
    ) -> None:
        if camera_id not in self._streaming_wanted:
            return
        prev = self._restart_tasks.get(camera_id)
        if prev and not prev.done():
            return

        async def _run() -> None:
            try:
                await self._restart_upstream_after_exit(
                    camera_id, rtsp_url, exit_code, session_id, uptime=uptime
                )
            finally:
                self._restart_tasks.pop(camera_id, None)

        self._restart_tasks[camera_id] = asyncio.create_task(
            _run(), name=f"restart-{camera_id}"
        )

    def _restart_delay_sec(
        self, attempt: int, exit_code: int, uptime: float = 0.0
    ) -> float:
        base = max(1.0, float(self.cfg.restart_base_delay_sec))
        cap = max(base, float(self.cfg.restart_max_delay_sec))
        delay = min(cap, base * (2 ** min(max(attempt - 1, 0), 4)))
        if exit_code in _RTMP_FAST_FAIL_EXIT:
            delay = max(delay, float(self.cfg.rtmp_republish_cooldown_sec))
            # Brief RTMP drop (<15s) — old publisher slot may still be held.
            if uptime > 0 and uptime < 15.0:
                delay = max(delay, 20.0)
        return delay

    async def _restart_upstream_after_exit(
        self,
        camera_id: str,
        rtsp_url: str,
        exit_code: int,
        session_id: str | None,
        uptime: float = 0.0,
    ) -> None:
        """Retry forever while backend still wants this stream (camera offline, slow net, RTMP blip)."""
        if not self._upstream:
            return
        while camera_id in self._streaming_wanted:
            self.reload_cameras_from_config()
            attempt = max(1, self._restart_attempts.get(camera_id, 0))
            delay = self._restart_delay_sec(attempt, exit_code, uptime=uptime)
            rtsp_url = self._restore_canonical_rtsp_url(camera_id) or rtsp_url
            scale = self._stream_scale_filter(camera_id)
            logger.info(
                "upstream auto-restart camera=%s in %.1fs (attempt=%s last_exit=%s)",
                camera_id,
                delay,
                attempt,
                exit_code,
            )
            await self._upstream.stop(camera_id)
            self._last_stop_mono[camera_id] = time.monotonic()
            await asyncio.sleep(delay)
            await self._wait_rtmp_republish_slot(camera_id)
            if camera_id not in self._streaming_wanted:
                return
            try:
                _, input_url = await self._ensure_rtsp_relay(camera_id, rtsp_url)
                ok, err, up_meta = await self._upstream.start(
                    camera_id,
                    input_url,
                    self.cfg.agent_id,
                    scale_filter=scale,
                    from_relay=self._use_rtsp_relay() and camera_id in self._relay_active,
                )
                if ok:
                    async with self._lock:
                        st2 = self._streams.get(camera_id)
                        if st2:
                            st2.active = True
                    self._stream_start_mono[camera_id] = time.monotonic()
                    sk = make_stream_key(self.cfg.agent_id, camera_id)
                    payload: dict[str, Any] = {
                        "ok": True,
                        "camera_id": camera_id,
                        "agent_id": self.cfg.agent_id,
                        "delivery": self.cfg.delivery,
                        "stream_key": sk,
                        "note": "reconnected",
                    }
                    if session_id:
                        payload["session_id"] = session_id
                    payload.update(up_meta)
                    pb = self._playback_url(camera_id)
                    if pb:
                        payload["playback_url"] = pb
                    await self._emit_status(camera_id, "stream_ready", payload)
                    logger.info(
                        "upstream auto-restarted camera=%s after exit_code=%s",
                        camera_id,
                        exit_code,
                    )
                    return
                self._restart_attempts[camera_id] = attempt + 1
                exit_code = 251
                logger.warning(
                    "upstream start failed camera=%s (attempt=%s) err=%s — retrying",
                    camera_id,
                    attempt,
                    err,
                )
            except Exception:
                logger.exception(
                    "upstream auto-restart exception camera=%s attempt=%s",
                    camera_id,
                    attempt,
                )

    async def restore_wanted_streams(self) -> None:
        """After WS reconnect, restart streams the backend had asked for."""
        for camera_id in list(self._streaming_wanted):
            if self._upstream and self._upstream.is_running(camera_id):
                continue
            task = self._restart_tasks.get(camera_id)
            if task and not task.done():
                continue
            async with self._lock:
                st = self._streams.get(camera_id)
                if st and st.active:
                    continue
                sid = st.session_id if st else None
            logger.info("restoring wanted stream camera=%s", camera_id)
            await self.start_stream(camera_id, session_id=sid)

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
        async with self._start_lock(camera_id):
            return await self._start_stream_locked(camera_id, session_id)

    async def _start_stream_locked(
        self, camera_id: str, session_id: str | None = None
    ) -> dict[str, Any]:
        self.reload_cameras_from_config()
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
                st.sessions.add(session_id)
                st.session_id = session_id
            st.viewers = len(st.sessions)

            if st.stop_task and not st.stop_task.done():
                st.stop_task.cancel()
                try:
                    await st.stop_task
                except asyncio.CancelledError:
                    pass
                st.stop_task = None

            self._streaming_wanted.add(camera_id)
            self._cancel_restart_task(camera_id)
            self._sync_active_flag(camera_id)
            if self._ffmpeg_running(camera_id):
                st.active = True
                payload = self._already_running_payload(
                    camera_id, path_name, st, session_id=session_id
                )
                logger.info(
                    "stream joined camera=%s session=%s viewers=%s (ffmpeg up %.1fs)",
                    camera_id,
                    session_id or st.session_id,
                    st.viewers,
                    self._publisher_uptime(camera_id),
                )
                await self._emit_status(camera_id, "stream_ready", payload)
                return payload
            if st.active and not self._ffmpeg_running(camera_id):
                st.active = False
                logger.warning(
                    "camera %s marked active but ffmpeg not running — restarting publish",
                    camera_id,
                )

        # upstream
        up_meta: dict[str, Any] = {}
        if d in ("upstream_rtmp", "both") and self._upstream:
            rtsp_url = self._restore_canonical_rtsp_url(camera_id) or rtsp_url
            await self._wait_rtmp_republish_slot(camera_id)
            self._stream_start_mono[camera_id] = time.monotonic()
            _, input_url = await self._ensure_rtsp_relay(camera_id, rtsp_url)
            ok, err, up_meta = await self._upstream.start(
                camera_id,
                input_url,
                self.cfg.agent_id,
                scale_filter=self._stream_scale_filter(camera_id),
                from_relay=self._use_rtsp_relay() and camera_id in self._relay_active,
            )
            if not ok:
                self._stream_start_mono.pop(camera_id, None)
                payload = {
                    "ok": False,
                    "error": err or "upstream_failed",
                    "camera_id": camera_id,
                    "delivery": d,
                    "note": "retry_scheduled",
                }
                if session_id:
                    payload["session_id"] = session_id
                await self._emit_status(camera_id, "stream_reconnecting", payload)
                self._schedule_upstream_restart(
                    camera_id, rtsp_url, 0, session_id
                )
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
            self._streaming_wanted.add(camera_id)
            self._stream_start_mono[camera_id] = time.monotonic()
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
        st = None
        path_name = ""
        already_inactive = False

        async with self._lock:
            if force:
                self._streaming_wanted.discard(camera_id)
                self._restart_attempts.pop(camera_id, None)
                self._stream_start_mono.pop(camera_id, None)
                self._cancel_restart_task(camera_id)

            st = self._streams.get(camera_id)
            if force and st:
                st.sessions.clear()
                st.viewers = 0
            if not st:
                if session_id:
                    pass
            else:
                if session_id:
                    st.sessions.discard(session_id)
                    st.viewers = len(st.sessions)
                    if st.sessions:
                        st.session_id = next(iter(st.sessions))
                    else:
                        st.session_id = session_id
                path_name = st.path_name
                already_inactive = not st.active

                if not force and st.viewers > 0:
                    logger.info(
                        "stop_stream ignored camera=%s session=%s viewers=%s still watching",
                        camera_id,
                        session_id,
                        st.viewers,
                    )
                    payload = {
                        "ok": True,
                        "note": "other_viewers_active",
                        "camera_id": camera_id,
                        "viewers": st.viewers,
                    }
                    if session_id:
                        payload["session_id"] = session_id
                    return payload

                if (
                    not force
                    and self.cfg.viewer_idle_stop
                    and st.viewers == 0
                    and st.active
                ):
                    st.stop_task = asyncio.create_task(
                        self._delayed_stop(camera_id, self.cfg.idle_grace_sec)
                    )
                    payload = {
                        "ok": True,
                        "camera_id": camera_id,
                        "viewers": 0,
                        "note": "idle_stop_scheduled",
                    }
                    if session_id:
                        payload["session_id"] = session_id
                    logger.info(
                        "stop_stream camera=%s session=%s — idle stop in %.0fs",
                        camera_id,
                        session_id,
                        self.cfg.idle_grace_sec,
                    )
                    return payload

                if st.stop_task and not st.stop_task.done():
                    st.stop_task.cancel()
                    st.stop_task = None

                if not force and st.viewers == 0:
                    self._streaming_wanted.discard(camera_id)
                    self._restart_attempts.pop(camera_id, None)
                    self._cancel_restart_task(camera_id)

                if force or st.active:
                    st.active = False
                    if not force and st.viewers == 0:
                        self._streaming_wanted.discard(camera_id)
                        self._restart_attempts.pop(camera_id, None)

        if self._upstream and d in ("upstream_rtmp", "both"):
            if force or self._upstream.is_running(camera_id):
                await self._upstream.stop(camera_id, reason="stop_stream" if force else "")
                self._last_stop_mono[camera_id] = time.monotonic()
            if force:
                await self._release_rtsp_relay(camera_id)

        if not st:
            payload = {"ok": True, "camera_id": camera_id, "note": "not_running"}
            if session_id:
                payload["session_id"] = session_id
            return payload

        if already_inactive and not force:
            if st and st.viewers == 0:
                self._streaming_wanted.discard(camera_id)
                self._restart_attempts.pop(camera_id, None)
                self._cancel_restart_task(camera_id)
                if self._upstream and self._upstream.is_running(camera_id):
                    await self._upstream.stop(camera_id, reason="stop_after_inactive")
                    self._last_stop_mono[camera_id] = time.monotonic()
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

        mtx_err = None
        if d in ("local_webrtc", "both") and path_name:
            ok, mtx_err = await asyncio.to_thread(self.mtx.delete_path, path_name)
            if not ok:
                logger.warning("stop_stream mediamtx delete: %s", mtx_err)
            else:
                logger.info("stream stopped mediamtx camera=%s path=%s", camera_id, path_name)

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

    async def stop_all(self) -> None:
        """Graceful shutdown: stop every FFmpeg publish and clear wanted set."""
        camera_ids = set(self._streaming_wanted)
        camera_ids.update(self._cameras.keys())
        camera_ids.update(self._relay_active)
        for camera_id in camera_ids:
            await self.stop_stream(camera_id, force=True)
        if self._upstream:
            await self._upstream.stop_all()

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
