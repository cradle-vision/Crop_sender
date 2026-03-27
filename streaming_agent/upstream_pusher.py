"""Push camera RTSP to central ingest (RTMP) via FFmpeg — works through NAT without static IP."""

from __future__ import annotations

import asyncio
import logging
import shutil
from typing import Any

logger = logging.getLogger(__name__)


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
    """One FFmpeg process per camera: RTSP → RTMP (copy, no transcode when possible)."""

    def __init__(
        self,
        ffmpeg_path: str,
        rtmp_url_template: str,
        rtsp_transport: str = "tcp",
        extra_args: list[str] | None = None,
    ):
        self.ffmpeg_path = ffmpeg_path
        self.rtmp_url_template = rtmp_url_template
        self.rtsp_transport = rtsp_transport
        self.extra_args = extra_args or []
        self._procs: dict[str, asyncio.subprocess.Process] = {}

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

        ffmpeg = shutil.which(self.ffmpeg_path) or self.ffmpeg_path
        sk = make_stream_key(agent_id, camera_id)
        rtmp = format_template(
            self.rtmp_url_template,
            stream_key=sk,
            agent_id=agent_id,
            camera_id=camera_id,
        )

        args = [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "warning",
            "-rtsp_transport",
            self.rtsp_transport,
            "-i",
            rtsp_url,
            "-c",
            "copy",
            "-f",
            "flv",
        ]
        args.extend(self.extra_args)
        args.append(rtmp)

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

    async def stop(self, camera_id: str) -> None:
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
            if self._procs.get(camera_id) is proc:
                self._procs.pop(camera_id, None)
            if proc.returncode not in (0, None, -15, -9):  # SIGTERM/SIGKILL ok
                err = await self._read_stderr_tail(proc)
                logger.warning(
                    "upstream ffmpeg exit camera=%s code=%s err=%s",
                    camera_id,
                    proc.returncode,
                    err[:500],
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
