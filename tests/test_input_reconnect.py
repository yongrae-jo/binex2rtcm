from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

from binex2rtcm.config import InputConfig
from binex2rtcm.errors import StreamError
from binex2rtcm.io.ntrip_client import NtripClientInput
from binex2rtcm.io.tcp import TcpClientInput


class _FakeWriter:
    def write(self, _data: bytes) -> None:
        return None

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        return None

    async def wait_closed(self) -> None:
        return None


class _EmptyTcpReader:
    async def read(self, _size: int) -> bytes:
        return b""


async def _failing_ntrip_body(self, _reader, _header: str, _remainder: bytes):
    if False:  # pragma: no cover - keeps this as an async generator
        yield b""
    raise StreamError("NTRIP body failed before payload")


async def _empty_ntrip_body(self, _reader, _header: str, _remainder: bytes):
    if False:  # pragma: no cover - keeps this as an async generator
        yield b""
    return


class InputReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def test_tcp_failures_accumulate_until_payload_is_received(self) -> None:
        adapter = TcpClientInput(
            InputConfig(name="tcp", kind="tcp_client", host="127.0.0.1", port=9000, reconnect_delay_s=5.0)
        )
        messages: list[str] = []

        def capture_error(_config: InputConfig, _level: str, message: str) -> None:
            messages.append(message)

        with (
            patch("binex2rtcm.io.tcp.append_input_error", side_effect=capture_error),
            patch(
                "binex2rtcm.io.tcp.asyncio.open_connection",
                new=AsyncMock(return_value=(_EmptyTcpReader(), _FakeWriter())),
            ),
            patch("binex2rtcm.io.tcp.asyncio.sleep", new=AsyncMock(return_value=None)),
        ):
            generator = adapter.iter_chunks()
            self.assertEqual(await anext(generator), b"")
            self.assertEqual(await anext(generator), b"")
            await generator.aclose()

        self.assertEqual(len(messages), 2)
        self.assertIn("failure #1", messages[0])
        self.assertIn("failure #2", messages[1])

    async def test_tcp_failure_count_resets_after_cooldown_wait(self) -> None:
        adapter = TcpClientInput(
            InputConfig(name="tcp", kind="tcp_client", host="127.0.0.1", port=9000, reconnect_delay_s=5.0)
        )
        messages: list[str] = []
        sleep_mock = AsyncMock(return_value=None)

        def capture_error(_config: InputConfig, _level: str, message: str) -> None:
            messages.append(message)

        with (
            patch("binex2rtcm.io.tcp.append_input_error", side_effect=capture_error),
            patch(
                "binex2rtcm.io.tcp.asyncio.open_connection",
                new=AsyncMock(side_effect=StreamError("connect failed")),
            ),
            patch("binex2rtcm.io.tcp.asyncio.sleep", new=sleep_mock),
        ):
            generator = adapter.iter_chunks()
            for _ in range(6):
                self.assertEqual(await anext(generator), b"")
            await generator.aclose()

        self.assertEqual(len(messages), 6)
        self.assertIn("failure #5", messages[4])
        self.assertIn("retry in 3600s", messages[4])
        self.assertIn("failure #1", messages[5])
        self.assertEqual(sleep_mock.await_args_list[4].args[0], 3600.0)

    async def test_ntrip_failures_accumulate_until_payload_is_received(self) -> None:
        adapter = NtripClientInput(
            InputConfig(
                name="ntrip",
                kind="ntrip_client",
                host="127.0.0.1",
                port=2101,
                mountpoint="TEST",
                reconnect_delay_s=5.0,
            )
        )
        messages: list[str] = []

        def capture_error(_config: InputConfig, _level: str, message: str) -> None:
            messages.append(message)

        with (
            patch("binex2rtcm.io.ntrip_client.append_input_error", side_effect=capture_error),
            patch(
                "binex2rtcm.io.ntrip_client.asyncio.open_connection",
                new=AsyncMock(return_value=(object(), _FakeWriter())),
            ),
            patch("binex2rtcm.io.ntrip_client.asyncio.sleep", new=AsyncMock(return_value=None)),
            patch.object(
                NtripClientInput,
                "_read_header",
                new=AsyncMock(return_value=("ICY 200 OK\r\n\r\n", b"")),
            ),
            patch.object(NtripClientInput, "_iter_body", new=_failing_ntrip_body),
        ):
            generator = adapter.iter_chunks()
            self.assertEqual(await anext(generator), b"")
            self.assertEqual(await anext(generator), b"")
            await generator.aclose()

        self.assertEqual(len(messages), 2)
        self.assertIn("failure #1", messages[0])
        self.assertIn("failure #2", messages[1])

    async def test_ntrip_failure_count_resets_after_cooldown_wait(self) -> None:
        adapter = NtripClientInput(
            InputConfig(
                name="ntrip",
                kind="ntrip_client",
                host="127.0.0.1",
                port=2101,
                mountpoint="TEST",
                reconnect_delay_s=5.0,
            )
        )
        messages: list[str] = []
        sleep_mock = AsyncMock(return_value=None)

        def capture_error(_config: InputConfig, _level: str, message: str) -> None:
            messages.append(message)

        with (
            patch("binex2rtcm.io.ntrip_client.append_input_error", side_effect=capture_error),
            patch(
                "binex2rtcm.io.ntrip_client.asyncio.open_connection",
                new=AsyncMock(side_effect=StreamError("connect failed")),
            ),
            patch("binex2rtcm.io.ntrip_client.asyncio.sleep", new=sleep_mock),
        ):
            generator = adapter.iter_chunks()
            for _ in range(6):
                self.assertEqual(await anext(generator), b"")
            await generator.aclose()

        self.assertEqual(len(messages), 6)
        self.assertIn("failure #5", messages[4])
        self.assertIn("retry in 3600s", messages[4])
        self.assertIn("failure #1", messages[5])
        self.assertEqual(sleep_mock.await_args_list[4].args[0], 3600.0)

    async def test_ntrip_disconnect_without_payload_still_waits_before_retry(self) -> None:
        adapter = NtripClientInput(
            InputConfig(
                name="ntrip",
                kind="ntrip_client",
                host="127.0.0.1",
                port=2101,
                mountpoint="TEST",
                reconnect_delay_s=5.0,
            )
        )
        messages: list[str] = []
        sleep_mock = AsyncMock(return_value=None)

        def capture_error(_config: InputConfig, _level: str, message: str) -> None:
            messages.append(message)

        with (
            patch("binex2rtcm.io.ntrip_client.append_input_error", side_effect=capture_error),
            patch(
                "binex2rtcm.io.ntrip_client.asyncio.open_connection",
                new=AsyncMock(return_value=(object(), _FakeWriter())),
            ),
            patch("binex2rtcm.io.ntrip_client.asyncio.sleep", new=sleep_mock),
            patch.object(
                NtripClientInput,
                "_read_header",
                new=AsyncMock(return_value=("ICY 200 OK\r\n\r\n", b"")),
            ),
            patch.object(NtripClientInput, "_iter_body", new=_empty_ntrip_body),
        ):
            generator = adapter.iter_chunks()
            for _ in range(6):
                self.assertEqual(await anext(generator), b"")
            await generator.aclose()

        self.assertEqual(len(messages), 6)
        self.assertIn("failure #5", messages[4])
        self.assertIn("retry in 3600s", messages[4])
        self.assertIn("failure #1", messages[5])
        self.assertEqual(sleep_mock.await_args_list[4].args[0], 3600.0)


if __name__ == "__main__":
    unittest.main()
