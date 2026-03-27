"""BINEX record framing.

This implementation currently targets the RTKLIB-supported subset:
big-endian, regular CRC, forward records (sync = 0xE2).
"""

from __future__ import annotations

import binascii
from dataclasses import dataclass

from ..errors import ProtocolError

BINEX_SYNC_BIG_ENDIAN_REGULAR = 0xE2
SUPPORTED_RECORD_IDS = {0x00, 0x01, 0x02, 0x03, 0x7D, 0x7E, 0x7F}


def xor_checksum(data: bytes) -> int:
    checksum = 0
    for value in data:
        checksum ^= value
    return checksum


def crc16_ccitt_zero(data: bytes) -> int:
    return binascii.crc_hqx(data, 0)


def parse_binex_uint(data: bytes, offset: int = 0) -> tuple[int, int]:
    value = 0
    for index in range(4):
        byte = data[offset + index]
        if index < 3:
            value = (value << 7) + (byte & 0x7F)
            if not (byte & 0x80):
                return value, index + 1
        else:
            value = (value << 8) + byte
            return value, 4
    raise ProtocolError("Invalid BINEX variable-length integer")


@dataclass(slots=True)
class BinexFrame:
    record_id: int
    payload: bytes
    raw: bytes


class BinexFramer:
    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, chunk: bytes) -> list[BinexFrame]:
        self._buffer.extend(chunk)
        frames: list[BinexFrame] = []
        while True:
            frame = self._extract_one()
            if frame is None:
                break
            frames.append(frame)
        return frames

    def reset(self) -> None:
        self._buffer.clear()

    def _extract_one(self) -> BinexFrame | None:
        if len(self._buffer) < 4:
            return None
        while len(self._buffer) >= 2:
            if self._buffer[0] == BINEX_SYNC_BIG_ENDIAN_REGULAR and self._buffer[1] in SUPPORTED_RECORD_IDS:
                break
            self._buffer.pop(0)
        if len(self._buffer) < 4:
            return None
        payload_len, header_len = parse_binex_uint(self._buffer, 2)
        frame_len_without_crc = payload_len + header_len + 2
        if frame_len_without_crc - 1 > 4096:
            self._buffer.pop(0)
            raise ProtocolError(f"BINEX frame length too large: {frame_len_without_crc - 1}")
        crc_len = 1 if frame_len_without_crc - 1 < 128 else 2
        total_len = frame_len_without_crc + crc_len
        if len(self._buffer) < total_len:
            return None
        raw = bytes(self._buffer[:total_len])
        del self._buffer[:total_len]
        checksum_input = raw[1:frame_len_without_crc]
        if crc_len == 1:
            expected = raw[frame_len_without_crc]
            actual = xor_checksum(checksum_input)
        else:
            expected = int.from_bytes(raw[frame_len_without_crc : frame_len_without_crc + 2], "big")
            actual = crc16_ccitt_zero(checksum_input)
        if expected != actual:
            raise ProtocolError(
                f"BINEX checksum mismatch for record 0x{raw[1]:02X}: expected {expected:#x}, actual {actual:#x}"
            )
        payload_start = 2 + header_len
        payload_end = payload_start + payload_len
        return BinexFrame(record_id=raw[1], payload=raw[payload_start:payload_end], raw=raw)
