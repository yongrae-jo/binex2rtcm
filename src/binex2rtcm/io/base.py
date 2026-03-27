"""Async stream interfaces."""

from __future__ import annotations

import abc
import asyncio
from collections.abc import AsyncIterator
from datetime import datetime
import logging

LOGGER = logging.getLogger(__name__)


class InputAdapter(abc.ABC):
    @abc.abstractmethod
    async def iter_chunks(self) -> AsyncIterator[bytes]:
        raise NotImplementedError


class OutputAdapter(abc.ABC):
    @abc.abstractmethod
    async def start(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def write(self, data: bytes, logical_time: datetime | None = None) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError


class QueuedOutputAdapter(OutputAdapter):
    """Shared backpressure-aware output implementation."""

    def __init__(self, max_queue: int = 512) -> None:
        self._queue: asyncio.Queue[tuple[bytes, datetime | None] | None] = asyncio.Queue(maxsize=max_queue)
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name=f"{self.__class__.__name__}-writer")

    async def write(self, data: bytes, logical_time: datetime | None = None) -> None:
        await self._queue.put((data, logical_time))

    async def close(self) -> None:
        if self._task is None:
            return
        await self._queue.put(None)
        await self._task
        self._task = None

    async def _run(self) -> None:
        while True:
            queued = await self._queue.get()
            if queued is None:
                break
            data, logical_time = queued
            try:
                await self._write(data, logical_time)
            except Exception as exc:  # pragma: no cover - transport dependent
                LOGGER.warning("%s write failed: %s", self.__class__.__name__, exc)

    @abc.abstractmethod
    async def _write(self, data: bytes, logical_time: datetime | None = None) -> None:
        raise NotImplementedError
