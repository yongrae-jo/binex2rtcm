"""RTCM frame extraction for binary log replay."""

from __future__ import annotations

from ..errors import ProtocolError


def crc24q(data: bytes) -> int:
    crc = 0
    for byte in data:
        crc ^= byte << 16
        for _ in range(8):
            crc <<= 1
            if crc & 0x1000000:
                crc ^= 0x1864CFB
            crc &= 0xFFFFFF
    return crc


class RtcmFramer:
    """Split a byte stream into RTCM 3 frames."""

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, data: bytes) -> list[bytes]:
        self._buffer.extend(data)
        frames: list[bytes] = []
        while len(self._buffer) >= 3:
            if self._buffer[0] != 0xD3:
                del self._buffer[0]
                continue
            length = ((self._buffer[1] & 0x03) << 8) | self._buffer[2]
            frame_length = 3 + length + 3
            if len(self._buffer) < frame_length:
                break
            frame = bytes(self._buffer[:frame_length])
            del self._buffer[:frame_length]
            body = frame[:-3]
            crc = (frame[-3] << 16) | (frame[-2] << 8) | frame[-1]
            if crc24q(body) != crc:
                raise ProtocolError("RTCM CRC mismatch")
            frames.append(frame)
        return frames

    def reset(self) -> None:
        self._buffer.clear()
