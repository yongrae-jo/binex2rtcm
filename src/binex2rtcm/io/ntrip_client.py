"""Async NTRIP client input."""

from __future__ import annotations

import asyncio
import base64
import logging
from collections.abc import AsyncIterator

from ..config import InputConfig
from ..errors import StreamError
from ..logging_utils import append_input_error
from .base import InputAdapter
from .reconnect import (
    RECONNECT_FAILURE_COOLDOWN_THRESHOLD,
    next_reconnect_delay_s,
)

LOGGER = logging.getLogger(__name__)


def _nmea_checksum(sentence_body: str) -> str:
    checksum = 0
    for char in sentence_body:
        checksum ^= ord(char)
    return f"{checksum:02X}"


def _format_lat_lon(lat_deg: float, lon_deg: float) -> tuple[str, str, str, str]:
    lat_abs = abs(lat_deg)
    lon_abs = abs(lon_deg)
    lat_d = int(lat_abs)
    lon_d = int(lon_abs)
    lat_m = (lat_abs - lat_d) * 60.0
    lon_m = (lon_abs - lon_d) * 60.0
    return (
        f"{lat_d:02d}{lat_m:07.4f}",
        "N" if lat_deg >= 0 else "S",
        f"{lon_d:03d}{lon_m:07.4f}",
        "E" if lon_deg >= 0 else "W",
    )


def build_gga(lat_deg: float, lon_deg: float, height_m: float) -> bytes:
    lat, lat_hemi, lon, lon_hemi = _format_lat_lon(lat_deg, lon_deg)
    body = f"GPGGA,000000.00,{lat},{lat_hemi},{lon},{lon_hemi},1,12,1.0,{height_m:.1f},M,0.0,M,,"
    return f"${body}*{_nmea_checksum(body)}\r\n".encode("ascii")


class _ChunkedTransferDecoder:
    def __init__(self) -> None:
        self._buffer = bytearray()
        self._chunk_size: int | None = None
        self._finished = False

    @property
    def finished(self) -> bool:
        return self._finished

    def feed(self, data: bytes) -> list[bytes]:
        if self._finished:
            return []
        self._buffer.extend(data)
        decoded: list[bytes] = []
        while True:
            if self._chunk_size is None:
                marker = self._buffer.find(b"\r\n")
                if marker < 0:
                    break
                raw_size = bytes(self._buffer[:marker])
                del self._buffer[: marker + 2]
                token = raw_size.split(b";", 1)[0].strip()
                if not token:
                    continue
                try:
                    self._chunk_size = int(token, 16)
                except ValueError as exc:
                    raise StreamError(f"invalid NTRIP chunk size: {raw_size!r}") from exc
                if self._chunk_size == 0:
                    self._finished = True
                    self._buffer.clear()
                    break
            if len(self._buffer) < self._chunk_size + 2:
                break
            payload = bytes(self._buffer[: self._chunk_size])
            terminator = bytes(self._buffer[self._chunk_size : self._chunk_size + 2])
            if terminator != b"\r\n":
                raise StreamError("invalid NTRIP chunk terminator")
            del self._buffer[: self._chunk_size + 2]
            self._chunk_size = None
            if payload:
                decoded.append(payload)
        return decoded


def _uses_chunked_transfer(header: str) -> bool:
    for line in header.split("\r\n")[1:]:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        if key.strip().lower() == "transfer-encoding" and "chunked" in value.lower():
            return True
    return False


class NtripClientInput(InputAdapter):
    def __init__(self, config: InputConfig) -> None:
        self._config = config

    async def iter_chunks(self) -> AsyncIterator[bytes]:
        consecutive_failures = 0
        while True:
            writer = None
            gga_task = None
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self._config.host, self._config.port),
                    timeout=self._config.connect_timeout_s,
                )
                LOGGER.info(
                    "connected to NTRIP %s:%s/%s",
                    self._config.host,
                    self._config.port,
                    self._config.mountpoint,
                )
                writer.write(self._request_bytes())
                await writer.drain()
                header, remainder = await self._read_header(reader)
                if not ("200 OK" in header or header.startswith("ICY 200")):
                    raise StreamError(f"NTRIP connect failed: {header!r}")
                consecutive_failures = 0
                if self._config.send_nmea_gga and self._config.source_position_llh is not None:
                    gga_task = asyncio.create_task(self._gga_loop(writer), name="ntrip-gga")
                async for chunk in self._iter_body(reader, header, remainder):
                    yield chunk
            except asyncio.CancelledError:  # pragma: no cover - cooperative shutdown
                raise
            except Exception as exc:
                consecutive_failures += 1
                delay_s = next_reconnect_delay_s(self._config.reconnect_delay_s, consecutive_failures)
                cooldown_active = consecutive_failures >= RECONNECT_FAILURE_COOLDOWN_THRESHOLD
                message = (
                    f"NTRIP reconnect after error: {exc} "
                    f"(failure #{consecutive_failures}, retry in {delay_s:.0f}s)"
                )
                if cooldown_active:
                    message += " after repeated failures"
                append_input_error(self._config, "WARNING", message)
                LOGGER.warning(message)
                yield b""
                await asyncio.sleep(delay_s)
            finally:
                try:
                    if gga_task is not None:
                        gga_task.cancel()
                        await gga_task
                except Exception:
                    pass
                try:
                    if writer is not None:
                        writer.close()
                        await writer.wait_closed()
                except Exception:
                    pass

    def _request_bytes(self) -> bytes:
        mountpoint = self._config.mountpoint or ""
        lines = [
            f"GET /{mountpoint} HTTP/1.1",
            f"Host: {self._config.host}:{self._config.port}",
            "Ntrip-Version: Ntrip/2.0",
            "User-Agent: binex2rtcm/0.1",
            "Connection: close",
        ]
        if self._config.username or self._config.password:
            userpass = f"{self._config.username or ''}:{self._config.password or ''}"
            token = base64.b64encode(userpass.encode("utf-8")).decode("ascii")
            lines.append(f"Authorization: Basic {token}")
        return ("\r\n".join(lines) + "\r\n\r\n").encode("ascii")

    async def _read_header(self, reader: asyncio.StreamReader) -> tuple[str, bytes]:
        buffer = bytearray()
        while b"\r\n\r\n" not in buffer:
            chunk = await reader.read(1)
            if not chunk:
                raise StreamError("NTRIP server closed before header completion")
            buffer.extend(chunk)
        raw_header, remainder = buffer.split(b"\r\n\r\n", 1)
        return raw_header.decode("latin-1", errors="replace"), bytes(remainder)

    async def _iter_body(
        self,
        reader: asyncio.StreamReader,
        header: str,
        remainder: bytes,
    ) -> AsyncIterator[bytes]:
        if not _uses_chunked_transfer(header):
            if remainder:
                yield remainder
            while chunk := await reader.read(self._config.chunk_size):
                yield chunk
            return

        decoder = _ChunkedTransferDecoder()
        if remainder:
            for chunk in decoder.feed(remainder):
                yield chunk
        while not decoder.finished:
            chunk = await reader.read(self._config.chunk_size)
            if not chunk:
                raise StreamError("NTRIP chunked body truncated before terminator")
            for payload in decoder.feed(chunk):
                yield payload

    async def _gga_loop(self, writer: asyncio.StreamWriter) -> None:
        assert self._config.source_position_llh is not None
        lat, lon, height = self._config.source_position_llh
        while True:
            writer.write(build_gga(lat, lon, height))
            await writer.drain()
            await asyncio.sleep(self._config.gga_interval_s)
