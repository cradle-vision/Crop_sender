"""Push camera RTSP to central ingest (RTMP) via FFmpeg — works through NAT without static IP."""

from __future__ import annotations

import asyncio
import logging
import shutil
from typing import Any, Awaitable, Callable

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
    """RTSP → RTMP: default stream copy (H.264); optional transcode to H.264 for H.265/HEVC or bad timestamps."""

    def __init__(
        self,
        ffmpeg_path: str,
        rtmp_url_template: str,
        rtsp_transport: str = "tcp",
        extra_args: list[str] | None = None,
        transcode: bool = False,
        on_process_exit: UpstreamExitCallback = None,
    ):
        self.ffmpeg_path = ffmpeg_path
        self.rtmp_url_template = rtmp_url_template
        self.rtsp_transport = rtsp_transport
        self.extra_args = extra_args or []
        self.transcode = transcode
        self.on_process_exit = on_process_exit
        self._procs: dict[str, asyncio.subprocess.Process] = {}

    def _ffmpeg_args(self, rtsp_url: str, rtmp: str) -> list[str]:
        """RTSP → FLV/RTMP: copy (H.264) or libx264 transcode (H.265 / broken PTS)."""
        ffmpeg = shutil.which(self.ffmpeg_path) or self.ffmpeg_path
        head: list[str] = [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "warning",
            "-fflags",
            "+genpts+discardcorrupt+igndts",
            "-err_detect",
            "ignore_err",
            "-max_delay",
            "5000000",
            "-rtsp_transport",
            self.rtsp_transport,
        ]
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
                "-vf",
                "setpts=PTS-STARTPTS,scale=1920:1080:force_original_aspect_ratio=decrease,pad=ceil(iw/2)*2:ceil(ih/2)*2,format=yuv420p,setsar=1",
                "-fps_mode",
                "cfr",
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "23",
                "-tune",
                "zerolatency",
                "-pix_fmt",
                "yuv420p",
                "-r",
                "3",
                "-g",
                "6",
                "-keyint_min",
                "6",
                "-sc_threshold",
                "0",
                "-bf",
                "0",
                "-profile:v",
                "main",
                "-level",
                "4.1",
                "-x264-params",
                "keyint=6:min-keyint=6:scenecut=0:repeat-headers=1:aud=1",
                "-maxrate",
                "2M",
                "-bufsize",
                "4M",
                "-an",
                "-avoid_negative_ts",
                "make_zero",
                "-max_muxing_queue_size",
                "2048",
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

    async def start(
        self,
        camera_id: str,
        rtsp_url: str,
        agent_id: str,
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

        if not self.rtmp_url_template.strip():
            return False, "upstream_rtmp_url_template_empty", {}

        sk = make_stream_key(agent_id, camera_id)
        rtmp = format_template(
            self.rtmp_url_template,
            stream_key=sk,
            agent_id=agent_id,
            camera_id=camera_id,
        )

        args = self._ffmpeg_args(rtsp_url, rtmp)
        if self.transcode:
            logger.info(
                "upstream mode=transcode(libx264) camera=%s — H.265/HEVC or bad timestamps → H.264 for FLV/RTMP (higher CPU)",
                camera_id,
            )
        else:
            logger.info(
                "upstream mode=copy camera=%s — H.264 RTSP; for H.265 set STREAMING_UPSTREAM_TRANSCODE=true",
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

        self._procs[camera_id] = proc
        asyncio.create_task(self._watch_stderr(camera_id, proc))

        # Do not wait for connection — check returncode shortly
        await asyncio.sleep(0.3)
        if proc.returncode is not None:
            err = await self._read_stderr_tail(proc)
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
        proc = self._procs.pop(camera_id, None)
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

    @staticmethod
    async def _read_stderr_tail(proc: asyncio.subprocess.Process) -> str:
        if proc.stderr is None:
            return ""
        try:
            data = await asyncio.wait_for(proc.stderr.read(), timeout=2.0)
            return data.decode("utf-8", errors="replace")[-800:]
        except Exception:
            return ""

    async def _watch_stderr(self, camera_id: str, proc: asyncio.subprocess.Process) -> None:
        try:
            await proc.wait()
        except asyncio.CancelledError:
            raise
        finally:
            err_tail = ""
            if proc.stderr:
                try:
                    data = await asyncio.wait_for(proc.stderr.read(), timeout=4.0)
                    err_tail = data.decode("utf-8", errors="replace")[-2000:]
                except Exception:
                    pass
            rc = proc.returncode if proc.returncode is not None else -1
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
