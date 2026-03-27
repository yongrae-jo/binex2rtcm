"""Console monitor for live stream status."""

from __future__ import annotations

import asyncio
import sys
import time

from .stats import RuntimeStats


class ConsoleMonitor:
    def __init__(self, stats: RuntimeStats, interval_s: float = 1.0) -> None:
        self._stats = stats
        self._interval_s = interval_s
        self._stop = asyncio.Event()

    async def run(self) -> None:
        while not self._stop.is_set():
            self._render()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval_s)
            except TimeoutError:
                continue
        self._render(final=True)

    def stop(self) -> None:
        self._stop.set()

    def _status(self, last_activity: float) -> str:
        if last_activity == 0.0:
            return "waiting"
        return "active" if (time.monotonic() - last_activity) <= max(self._interval_s * 3.0, 3.0) else "quiet"

    def _render(self, final: bool = False) -> None:
        lines = [
            "== binex2rtcm monitor ==",
            f"uptime: {self._stats.uptime_s():.1f}s",
            "",
            "[inputs]",
        ]
        for item in self._stats.inputs.values():
            lines.extend(
                [
                    (
                        f"- {item.name} [{item.session}] ({item.kind}) status={self._status(item.last_activity_monotonic)} "
                        f"bytes={item.bytes_in} chunks={item.chunks_in} frames={item.binex_frames} "
                        f"epochs={item.epochs} eph={item.ephemerides} rtcm={item.rtcm_messages} "
                        f"capture={item.capture_bytes} ignored={item.ignored_records} errors={item.errors}"
                    ),
                    f"  last_error: {item.last_error}" if item.last_error else "  last_error: -",
                ]
            )
        lines.append("")
        lines.append("[outputs]")
        for item in self._stats.outputs.values():
            lines.extend(
                [
                    (
                        f"- {item.name} [{item.session}] ({item.kind}) status={self._status(item.last_activity_monotonic)} "
                        f"bytes={item.bytes_out} writes={item.writes} errors={item.errors}"
                    ),
                    f"  last_error: {item.last_error}" if item.last_error else "  last_error: -",
                ]
            )
        text = "\n".join(lines)
        if sys.stdout.isatty():
            sys.stdout.write("\x1b[2J\x1b[H" + text + ("\n" if final else ""))
            sys.stdout.flush()
        else:
            print(text, flush=True)
