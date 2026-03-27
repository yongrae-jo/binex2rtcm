"""CLI entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
import shutil

from .config import load_config
from .logging_utils import configure_logging
from .pipeline import ConversionService

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Real-time BINEX/RTCM GNSS stream transcoder")
    parser.add_argument("--config", help="Path to TOML configuration file")
    parser.add_argument("--duration", type=float, default=None, help="Stop after N seconds")
    monitor_group = parser.add_mutually_exclusive_group()
    monitor_group.add_argument("--monitor", dest="monitor", action="store_true", help="Enable live console monitor")
    monitor_group.add_argument("--no-monitor", dest="monitor", action="store_false", help="Disable live console monitor")
    parser.set_defaults(monitor=None)
    parser.add_argument("--clear-runs", action="store_true", help="Clear runs directory and exit")
    parser.add_argument("--runs-dir", default="runs", help="Runs directory used with --clear-runs")
    return parser


async def _run(config_path: str, duration_s: float | None = None, monitor: bool | None = None) -> int:
    config = load_config(config_path)
    if duration_s is not None:
        config.run_duration_s = duration_s if duration_s > 0 else None
    if monitor is not None:
        config.monitor.enabled = monitor
    configure_logging(config.logging_level)
    LOGGER.info("starting binex2rtcm with %d inputs and %d outputs", len(config.inputs), len(config.outputs))
    service = ConversionService(config)
    await service.run()
    return 0


def _clear_runs(runs_dir: str) -> int:
    target = Path(runs_dir).expanduser()
    resolved = target.resolve()
    if resolved == resolved.parent:
        raise ValueError(f"refusing to clear root directory: {target}")
    if target.exists() and not target.is_dir():
        raise ValueError(f"runs path is not a directory: {target}")
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    LOGGER.info("cleared runs directory: %s", target)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.clear_runs:
            configure_logging("INFO")
            return _clear_runs(args.runs_dir)
        if not args.config:
            parser.error("--config is required unless --clear-runs is used")
        return asyncio.run(
            _run(
                str(Path(args.config).expanduser()),
                duration_s=args.duration,
                monitor=args.monitor,
            )
        )
    except KeyboardInterrupt:
        LOGGER.info("shutdown requested by user")
        return 130
