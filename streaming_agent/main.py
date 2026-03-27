"""Entry point: load config, connect backend, manage MediaMTX streams."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

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
    cfg = load_config(config_path)
    mtx = MediaMTXClient(cfg.mediamtx.api_url)
    stream_manager = StreamManager(cfg, mtx, on_status=None)
    signaling = SignalingClient(cfg, stream_manager, mtx)
    stream_manager._on_status = signaling.on_stream_status  # noqa: SLF001

    loop = asyncio.get_running_loop()
    runner = asyncio.create_task(signaling.run(), name="signaling_run")

    def _stop(*_: object) -> None:
        logger.info("Shutdown requested")
        signaling.stop()
        runner.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    try:
        await runner
    except asyncio.CancelledError:
        logger.info("Signaling task cancelled")


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
