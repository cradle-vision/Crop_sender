"""Entry point: load config, connect backend, manage MediaMTX streams."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

from agent_control.ws_owner import claim_ws_owner, refresh_ws_owner, release_ws_owner
from streaming_agent.config import load_config
from streaming_agent.mediamtx_client import MediaMTXClient
from streaming_agent.signaling_client import SignalingClient
from streaming_agent.stream_manager import StreamManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("streaming_agent.main")


async def _run_async(config_path: str | None) -> None:
    enabled = os.getenv("STREAMING_AGENT_ENABLED")
    if enabled is not None and enabled.strip().lower() in ("0", "false", "no", "off"):
        logger.info("STREAMING_AGENT_ENABLED is false; exiting (streaming disabled).")
        return
    claim_ws_owner("streaming")
    logger.info("Claimed WS ownership marker (streaming)")
    try:
        cfg = load_config(config_path)
        mtx = MediaMTXClient(cfg.mediamtx.api_url)
        stream_manager = StreamManager(cfg, mtx, on_status=None, config_path=config_path)
        signaling = SignalingClient(cfg, stream_manager, mtx)
        stream_manager._on_status = signaling.on_stream_status  # noqa: SLF001

        loop = asyncio.get_running_loop()
        runner = asyncio.create_task(signaling.run(), name="signaling_run")
        shutdown = asyncio.Event()

        async def _refresh_owner_loop() -> None:
            while not shutdown.is_set():
                try:
                    refresh_ws_owner("streaming")
                except Exception:
                    logger.debug("ws owner refresh failed", exc_info=True)
                try:
                    await asyncio.wait_for(shutdown.wait(), timeout=30.0)
                except asyncio.TimeoutError:
                    pass

        owner_task = asyncio.create_task(_refresh_owner_loop(), name="ws_owner_refresh")

        async def _shutdown(*_: object) -> None:
            if shutdown.is_set():
                return
            shutdown.set()
            logger.info("Shutdown requested — stopping streams and signaling")
            signaling.stop()
            try:
                await stream_manager.stop_all()
            except Exception:
                logger.exception("stop_all failed during shutdown")
            runner.cancel()
            owner_task.cancel()

        def _request_shutdown() -> None:
            asyncio.create_task(_shutdown())

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_shutdown)
            except NotImplementedError:
                pass

        try:
            await runner
        except asyncio.CancelledError:
            logger.info("Signaling task cancelled")
        finally:
            if not shutdown.is_set():
                await _shutdown()
    finally:
        release_ws_owner("streaming")
        logger.info("Released WS ownership marker")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Streaming Agent (RTSP → MediaMTX → WebRTC)")
    p.add_argument(
        "--config",
        "-c",
        help="Path to streaming-agent.yaml (default: STREAMING_AGENT_CONFIG or config/streaming-agent.yaml)",
    )
    args = p.parse_args(argv)
    try:
        asyncio.run(_run_async(args.config))
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
