"""TCP client/server adapters."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator

from ..config import InputConfig, OutputConfig
from ..errors import StreamError
from ..logging_utils import append_input_error
from .base import InputAdapter, QueuedOutputAdapter
from .reconnect import (
    RECONNECT_FAILURE_COOLDOWN_THRESHOLD,
    next_reconnect_delay_s,
)

LOGGER = logging.getLogger(__name__)


class TcpClientInput(InputAdapter):
    def __init__(self, config: InputConfig) -> None:
        self._config = config

    async def iter_chunks(self) -> AsyncIterator[bytes]:
        consecutive_failures = 0
        while True:
            writer = None
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self._config.host, self._config.port),
                    timeout=self._config.connect_timeout_s,
                )
                consecutive_failures = 0
                LOGGER.info("connected TCP input %s:%s", self._config.host, self._config.port)
                while chunk := await reader.read(self._config.chunk_size):
                    yield chunk
                raise StreamError("TCP input disconnected")
            except asyncio.CancelledError:  # pragma: no cover
                raise
            except Exception as exc:
                consecutive_failures += 1
                delay_s = next_reconnect_delay_s(self._config.reconnect_delay_s, consecutive_failures)
                cooldown_active = consecutive_failures >= RECONNECT_FAILURE_COOLDOWN_THRESHOLD
                message = f"TCP input reconnect after error: {exc} (failure #{consecutive_failures}, retry in {delay_s:.0f}s)"
                if cooldown_active:
                    message += " after repeated failures"
                append_input_error(self._config, "WARNING", message)
                LOGGER.warning(message)
                yield b""
                await asyncio.sleep(delay_s)
            finally:
                try:
                    if writer is not None:
                        writer.close()
                        await writer.wait_closed()
                except Exception:
                    pass


class TcpClientOutput(QueuedOutputAdapter):
    def __init__(self, config: OutputConfig) -> None:
        super().__init__(config.max_queue)
        self._config = config
        self._writer: asyncio.StreamWriter | None = None

    async def _write(self, data: bytes, logical_time=None) -> None:
        if self._writer is None:
            _, self._writer = await asyncio.open_connection(self._config.host, self._config.port)
        try:
            self._writer.write(data)
            await self._writer.drain()
        except Exception:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
            raise

    async def close(self) -> None:
        await super().close()
        if self._writer is not None:
            self._writer.close()
            await self._writer.wait_closed()
            self._writer = None


class TcpServerOutput(QueuedOutputAdapter):
    def __init__(self, config: OutputConfig) -> None:
        super().__init__(config.max_queue)
        self._config = config
        self._clients: set[asyncio.StreamWriter] = set()
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._on_client,
            host=self._config.host,
            port=self._config.port,
        )
        LOGGER.info("TCP output server listening on %s:%s", self._config.host, self._config.port)
        await super().start()

    async def _on_client(
        self,
        _reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self._clients.add(writer)
        peer = writer.get_extra_info("peername")
        LOGGER.info("TCP output client connected: %s", peer)
        try:
            await writer.wait_closed()
        finally:
            self._clients.discard(writer)
            LOGGER.info("TCP output client disconnected: %s", peer)

    async def _write(self, data: bytes, logical_time=None) -> None:
        closed: list[asyncio.StreamWriter] = []
        for writer in tuple(self._clients):
            try:
                writer.write(data)
                await writer.drain()
            except Exception:
                closed.append(writer)
        for writer in closed:
            self._clients.discard(writer)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def close(self) -> None:
        await super().close()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        for writer in tuple(self._clients):
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        self._clients.clear()
