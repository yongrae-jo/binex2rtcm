"""Bit-level writer for RTCM frames."""

from __future__ import annotations


class BitBuffer:
    def __init__(self) -> None:
        self._buffer = bytearray()
        self.bit_length = 0

    def _ensure(self, count: int) -> None:
        size = (self.bit_length + count + 7) // 8
        if size > len(self._buffer):
            self._buffer.extend(b"\x00" * (size - len(self._buffer)))

    def _set_bit(self, pos: int, value: int) -> None:
        byte_index = pos // 8
        bit_index = 7 - (pos % 8)
        if value:
            self._buffer[byte_index] |= 1 << bit_index
        else:
            self._buffer[byte_index] &= ~(1 << bit_index)

    def set_unsigned(self, pos: int, bits: int, value: int) -> None:
        for offset in range(bits):
            shift = bits - 1 - offset
            self._set_bit(pos + offset, (value >> shift) & 0x01)

    def append_unsigned(self, value: int, bits: int) -> None:
        self._ensure(bits)
        self.set_unsigned(self.bit_length, bits, value & ((1 << bits) - 1))
        self.bit_length += bits

    def append_signed(self, value: int, bits: int) -> None:
        if value < 0:
            value = (1 << bits) + value
        self.append_unsigned(value, bits)

    def append_sign_magnitude(self, value: int, bits: int) -> None:
        self.append_unsigned(1 if value < 0 else 0, 1)
        self.append_unsigned(abs(value), bits - 1)

    def append_ascii(self, text: str) -> None:
        for char in text:
            self.append_unsigned(ord(char), 8)

    def append_bytes(self, data: bytes) -> None:
        for value in data:
            self.append_unsigned(value, 8)

    def pad_to_byte(self) -> None:
        remainder = self.bit_length % 8
        if remainder:
            self.append_unsigned(0, 8 - remainder)

    def to_bytes(self) -> bytes:
        return bytes(self._buffer[: (self.bit_length + 7) // 8])
