"""Runtime statistics for monitoring."""

from __future__ import annotations

from dataclasses import dataclass, field
import time


@dataclass(slots=True)
class InputStats:
    name: str
    kind: str
    session: str = "default"
    bytes_in: int = 0
    chunks_in: int = 0
    binex_frames: int = 0
    epochs: int = 0
    ephemerides: int = 0
    rtcm_messages: int = 0
    capture_bytes: int = 0
    ignored_records: int = 0
    errors: int = 0
    last_error: str = ""
    last_activity_monotonic: float = 0.0

    def mark_activity(self, byte_count: int = 0) -> None:
        self.bytes_in += byte_count
        if byte_count:
            self.chunks_in += 1
        self.last_activity_monotonic = time.monotonic()


@dataclass(slots=True)
class OutputStats:
    name: str
    kind: str
    session: str = "default"
    bytes_out: int = 0
    writes: int = 0
    errors: int = 0
    last_error: str = ""
    last_activity_monotonic: float = 0.0

    def mark_activity(self, byte_count: int) -> None:
        self.bytes_out += byte_count
        self.writes += 1
        self.last_activity_monotonic = time.monotonic()


@dataclass(slots=True)
class RuntimeStats:
    inputs: dict[str, InputStats] = field(default_factory=dict)
    outputs: dict[str, OutputStats] = field(default_factory=dict)
    started_monotonic: float = field(default_factory=time.monotonic)

    def uptime_s(self) -> float:
        return time.monotonic() - self.started_monotonic
