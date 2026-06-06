"""Push camera RTSP to central ingest (RTMP) via FFmpeg — works through NAT without static IP."""

from __future__ import annotations

import asyncio
import logging
import shutil
from typing import Any, Awaitable, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from streaming_agent.config import TranscodeConfig

logger = logging.getLogger(__name__)

# (camera_id, exit_code, stderr_tail)
UpstreamExitCallback = Callable[[str, int, str], Awaitable[None]] | None


def make_stream_key(agent_id: str, camera_id: str) -> str:
    """Stable id for ingest + playback mapping (200 agents × N cameras)."""
    a = "".join(c if c.isalnum() or c in "-_" else "_" for c in agent_id.strip())
    c = "".join(c if c.isalnum() or c in "-_" else "_" for c in camera_id.strip())
    return f"{a}_{c}"


def format_template(tpl: str, *, stream_key: str, agent_id: str, camera_id: str) -> str:
    return tpl.format(
        stream_key=stream_key,
        agent_id=agent_id,
        camera_id=camera_id,
    )


class UpstreamPusher:
    """RTSP → RTMP: libx264 transcode (stable timestamps) or H.264 copy when transcode is off."""

    def __init__(
        self,
        ffmpeg_path: str,
        rtmp_url_template: str,
        rtsp_transport: str = "tcp",
        extra_args: list[str] | None = None,
        transcode: bool = True,
        transcode_opts: "TranscodeConfig | None" = None,
        on_process_exit: UpstreamExitCallback = None,
    ):
        self.ffmpeg_path = ffmpeg_path
        self.rtmp_url_template = rtmp_url_template
        self.rtsp_transport = rtsp_transport
        self.extra_args = extra_args or []
        self.transcode = transcode
        self.transcode_opts = transcode_opts
        self.on_process_exit = on_process_exit
        self._procs: dict[str, asyncio.subprocess.Process] = {}
        self._stderr_bufs: dict[str, list[bytes]] = {}
        self._stderr_tasks: dict[str, asyncio.Task[None]] = {}
        # Bumped on stop/replace so stale _watch_process tasks ignore their exit.
        self._proc_epoch: dict[str, int] = {}

    def _transcode_video_args(self, scale_filter: str = "") -> list[str]:
        from streaming_agent.config import TranscodeConfig

        from streaming_agent.config import _DEFAULT_PTS_FILTER

        o = self.transcode_opts or TranscodeConfig()
        scaled = bool((scale_filter or o.scale_filter or "").strip())
        # HEVC main + downscale: short GOP so RTMP gets keyframes every ~3s at 3fps.
        gop = min(o.gop, 9) if scaled else o.gop
        keyint_min = min(o.keyint_min, 9) if scaled else o.keyint_min
        args: list[str] = []
        vf = (scale_filter or o.scale_filter or "").strip() or _DEFAULT_PTS_FILTER
        args.extend(["-vf", vf])
        if o.fps > 0:
            args.extend(["-fps_mode", "cfr"])
        args.extend(
            [
                "-c:v",
                "libx264",
                "-preset",
                o.preset,
                "-crf",
                str(o.crf),
                "-tune",
                o.tune,
                "-pix_fmt",
                o.pix_fmt,
            ]
        )
        if o.fps > 0:
            args.extend(["-r", str(o.fps)])
        args.extend(
            [
                "-g",
                str(gop),
                "-keyint_min",
                str(keyint_min),
                "-sc_threshold",
                "0",
                "-bf",
                str(o.bf),
                "-profile:v",
                o.profile,
                "-level",
                o.level,
            ]
        )
        if scaled:
            args.extend(["-force_key_frames", "expr:gte(t,n_forced*1)"])
        if o.x264_params:
            args.extend(["-x264-params", o.x264_params])
        args.extend(
            [
                "-maxrate",
                o.maxrate,
                "-bufsize",
                o.bufsize,
                "-an",
                "-avoid_negative_ts",
                "make_zero",
            ]
        )
        if o.max_muxing_queue_size > 0:
            args.extend(["-max_muxing_queue_size", str(o.max_muxing_queue_size)])
        if o.threads > 0:
            args.extend(["-threads", str(o.threads)])
        return args

    @staticmethod
    def _is_local_relay(rtsp_url: str) -> bool:
        low = (rtsp_url or "").lower()
        return "127.0.0.1" in low or "localhost" in low

    def _effective_rtsp_transport(self, scale_filter: str = "") -> str:
        if (scale_filter or "").strip():
            return "udp"
        return (self.rtsp_transport or "tcp").lower()

    def _ffmpeg_args(
        self,
        rtsp_url: str,
        rtmp: str,
        scale_filter: str = "",
        *,
        from_relay: bool = False,
    ) -> list[str]:
        """RTSP → FLV/RTMP: copy (H.264) or libx264 transcode (H.265 / broken PTS)."""
        ffmpeg = shutil.which(self.ffmpeg_path) or self.ffmpeg_path
        needs_heavy_input = bool((scale_filter or "").strip())
        relay_input = from_relay or self._is_local_relay(rtsp_url)
        transport = "tcp" if relay_input else self._effective_rtsp_transport(scale_filter)
        head: list[str] = [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "warning",
            "-fflags",
            "+genpts+discardcorrupt+igndts+nobuffer",
            "-err_detect",
            "ignore_err",
            "-ec",
            "guess_mvs+deblock",
            "-max_delay",
            "5000000",
            "-rtsp_transport",
            transport,
            "-timeout",
            "5000000",
        ]
        if transport == "tcp":
            head.extend(["-rtsp_flags", "prefer_tcp"])
        if needs_heavy_input and not relay_input:
            head.extend(
                [
                    "-probesize",
                    "2000000",
                    "-analyzeduration",
                    "2000000",
                    "-rtbufsize",
                    "100M",
                    "-thread_queue_size",
                    "4096",
                ]
            )
        elif relay_input:
            head.extend(["-probesize", "1000000", "-analyzeduration", "1000000"])
        if not self.transcode:
            head.extend(["-use_wallclock_as_timestamps", "1"])
        head.extend(
            [
                "-i",
                rtsp_url,
                "-map",
                "0:v:0",
            ]
        )
        flv_live = [
            "-muxdelay",
            "0",
            "-muxpreload",
            "0",
            "-f",
            "flv",
            "-flvflags",
            "no_duration_filesize",
        ]
        if self.transcode:
            mid: list[str] = [
                *self._transcode_video_args(scale_filter),
                *flv_live,
            ]
        else:
            mid = ["-c", "copy", *flv_live, "-avoid_negative_ts", "make_zero"]
        return head + mid + self.extra_args + [rtmp]

    def active_count(self) -> int:
        return sum(1 for p in self._procs.values() if p.returncode is None)

    def is_running(self, camera_id: str) -> bool:
        p = self._procs.get(camera_id)
        return p is not None and p.returncode is None

    async def _drain_stderr(self, camera_id: str, proc: asyncio.subprocess.Process) -> None:
        buf = self._stderr_bufs.setdefault(camera_id, [])
        if proc.stderr is None:
            return
        try:
            while True:
                chunk = await proc.stderr.read(4096)
                if not chunk:
                    break
                buf.append(chunk)
                if sum(len(c) for c in buf) > 16000:
                    buf[:] = buf[-4:]
        except Exception:
            pass

    def _stderr_tail(self, camera_id: str) -> str:
        chunks = self._stderr_bufs.pop(camera_id, [])
        if not chunks:
            return ""
        return b"".join(chunks).decode("utf-8", errors="replace")[-2000:]

    async def start(
        self,
        camera_id: str,
        rtsp_url: str,
        agent_id: str,
        scale_filter: str = "",
        *,
        from_relay: bool = False,
    ) -> tuple[bool, str | None, dict[str, Any]]:
        if self.is_running(camera_id):
            sk = make_stream_key(agent_id, camera_id)
            rtmp = format_template(
                self.rtmp_url_template,
                stream_key=sk,
                agent_id=agent_id,
                camera_id=camera_id,
            )
            return True, None, {"stream_key": sk, "rtmp_dest": _redact_url(rtmp), "note": "already_running"}

        if camera_id in self._procs:
            await self.stop(camera_id, reason="replace")
            await asyncio.sleep(1.0)

        if not self.rtmp_url_template.strip():
            return False, "upstream_rtmp_url_template_empty", {}

        sk = make_stream_key(agent_id, camera_id)
        rtmp = format_template(
            self.rtmp_url_template,
            stream_key=sk,
            agent_id=agent_id,
            camera_id=camera_id,
        )

        relay_input = from_relay or self._is_local_relay(rtsp_url)
        args = self._ffmpeg_args(
            rtsp_url, rtmp, scale_filter, from_relay=relay_input
        )
        if self.transcode:
            o = self.transcode_opts
            preset = o.preset if o else "ultrafast"
            if scale_filter.strip():
                src = "local-relay" if relay_input else self._effective_rtsp_transport(scale_filter)
                logger.info(
                    "upstream mode=transcode(libx264/%s) camera=%s input=%s — downscale while encode",
                    preset,
                    camera_id,
                    src,
                )
            else:
                logger.info(
                    "upstream mode=transcode(libx264/%s) camera=%s — stable timestamps for RTMP",
                    preset,
                    camera_id,
                )
        else:
            logger.info(
                "upstream mode=copy camera=%s — H.264 RTSP; for H.265/bad timestamps set "
                "STREAMING_UPSTREAM_TRANSCODE=true or transcode: true in streaming-agent.yaml",
                camera_id,
            )

        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception as e:
            logger.exception("upstream ffmpeg start failed")
            return False, str(e), {}

        epoch = self._proc_epoch.get(camera_id, 0) + 1
        self._proc_epoch[camera_id] = epoch
        self._procs[camera_id] = proc
        self._stderr_bufs[camera_id] = []
        drain = asyncio.create_task(self._drain_stderr(camera_id, proc))
        self._stderr_tasks[camera_id] = drain
        asyncio.create_task(self._watch_process(camera_id, proc, drain, epoch))

        await asyncio.sleep(1.0)
        if proc.returncode is not None:
            await drain
            err = self._stderr_tail(camera_id)
            self._stderr_tasks.pop(camera_id, None)
            self._procs.pop(camera_id, None)
            return False, err or "ffmpeg_exited", {"stream_key": sk}

        logger.info(
            "upstream started camera=%s stream_key=%s -> %s",
            camera_id,
            sk,
            _redact_url(rtmp),
        )
        return True, None, {"stream_key": sk, "rtmp_dest": _redact_url(rtmp)}

    async def stop(self, camera_id: str, *, reason: str = "") -> None:
        self._proc_epoch[camera_id] = self._proc_epoch.get(camera_id, 0) + 1
        proc = self._procs.pop(camera_id, None)
        drain = self._stderr_tasks.pop(camera_id, None)
        if drain and not drain.done():
            drain.cancel()
            try:
                await drain
            except asyncio.CancelledError:
                pass
        self._stderr_bufs.pop(camera_id, None)
        if not proc:
            return
        if proc.returncode is not None:
            return
        try:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=8.0)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
        except ProcessLookupError:
            pass
        if reason:
            logger.info("upstream stopped camera=%s reason=%s", camera_id, reason)
        else:
            logger.info("upstream stopped camera=%s", camera_id)

    async def stop_all(self) -> None:
        for camera_id in list(self._procs.keys()):
            await self.stop(camera_id, reason="shutdown")

    async def _watch_process(
        self,
        camera_id: str,
        proc: asyncio.subprocess.Process,
        drain: asyncio.Task[None],
        epoch: int,
    ) -> None:
        try:
            await proc.wait()
        except asyncio.CancelledError:
            raise
        finally:
            if not drain.done():
                try:
                    await asyncio.wait_for(drain, timeout=2.0)
                except Exception:
                    pass
            err_tail = self._stderr_tail(camera_id)
            self._stderr_tasks.pop(camera_id, None)
            rc = proc.returncode if proc.returncode is not None else -1
            stale = self._proc_epoch.get(camera_id, 0) != epoch
            if stale:
                logger.debug(
                    "upstream ffmpeg exit ignored (stale) camera=%s code=%s epoch=%s",
                    camera_id,
                    rc,
                    epoch,
                )
                return
            logger.info(
                "upstream ffmpeg ended camera=%s code=%s stderr_tail=%s",
                camera_id,
                rc,
                (err_tail[-1200:] if err_tail else "(empty)"),
            )
            if self._procs.get(camera_id) is proc:
                self._procs.pop(camera_id, None)
            if self.on_process_exit and rc not in (-15, -9):
                try:
                    await self.on_process_exit(camera_id, rc, err_tail)
                except Exception:
                    logger.exception("on_process_exit failed camera=%s", camera_id)
            elif rc not in (0, -15, -9, None):
                logger.warning(
                    "upstream ffmpeg abnormal exit camera=%s code=%s",
                    camera_id,
                    rc,
                )


def _redact_url(url: str) -> str:
    if "@" not in url:
        return url
    try:
        head, tail = url.split("://", 1)
        if "@" in tail:
            host_part = tail.split("@", 1)[-1]
            return f"{head}://***@{host_part}"
    except Exception:
        pass
    return url
