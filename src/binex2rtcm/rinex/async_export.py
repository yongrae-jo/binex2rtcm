"""Background RINEX export queue."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from .segment import RinexSegmentSnapshot

LOGGER = logging.getLogger(__name__)
_EXPORT_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="binex2rtcm-rinex")


@dataclass(slots=True)
class _QueuedExport:
    snapshot: RinexSegmentSnapshot
    segment_path: Path
    generated_at: datetime | None = None


class BackgroundRinexExporter:
    def __init__(self, name: str, on_error: Callable[[Exception], None] | None = None) -> None:
        self._name = name
        self._on_error = on_error
        self._queue: asyncio.Queue[_QueuedExport | None] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name=f"{self._name}-rinex-export")

    def submit(
        self,
        snapshot: RinexSegmentSnapshot | None,
        segment_path: Path,
        generated_at: datetime | None = None,
    ) -> None:
        if snapshot is None or snapshot.empty():
            return
        if self._task is None:
            raise RuntimeError("BackgroundRinexExporter must be started before submit")
        self._queue.put_nowait(_QueuedExport(snapshot, segment_path, generated_at))

    async def close(self) -> None:
        if self._task is None:
            return
        await self._queue.put(None)
        await self._task
        self._task = None

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            queued = await self._queue.get()
            try:
                if queued is None:
                    return
                await loop.run_in_executor(
                    _EXPORT_EXECUTOR,
                    queued.snapshot.export,
                    queued.segment_path,
                    queued.generated_at,
                )
            except Exception as exc:  # pragma: no cover - filesystem dependent
                LOGGER.warning("background RINEX export failed for %s: %s", self._name, exc)
                if self._on_error is not None:
                    self._on_error(exc)
            finally:
                self._queue.task_done()
