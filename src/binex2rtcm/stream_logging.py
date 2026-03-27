"""Helpers for time-aligned binary stream logging."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from .gnss_time import gpst_datetime, utc_to_gpst_seconds

SUPPORTED_LOG_INTERVALS = ("5M", "10M", "15M", "30M", "1H", "24H")


def current_gpst_calendar_datetime() -> datetime:
    utc_now = datetime.now(UTC)
    return gpst_datetime(utc_to_gpst_seconds(utc_now))


def normalize_log_interval(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().upper()
    if normalized not in SUPPORTED_LOG_INTERVALS:
        choices = ", ".join(SUPPORTED_LOG_INTERVALS)
        raise ValueError(f"unsupported log interval {value!r}; expected one of: {choices}")
    return normalized


def aligned_interval_start(now: datetime, interval: str) -> datetime:
    interval = normalize_log_interval(interval) or ""
    if interval.endswith("M"):
        minutes = int(interval[:-1])
        aligned_minute = (now.minute // minutes) * minutes
        return now.replace(minute=aligned_minute, second=0, microsecond=0)
    if interval == "1H":
        return now.replace(minute=0, second=0, microsecond=0)
    if interval == "24H":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    raise ValueError(f"unsupported log interval {interval!r}")


@dataclass(slots=True, frozen=True)
class LogSegment:
    path: Path
    interval: str | None
    started_at: datetime
    closed_at: datetime


@dataclass(slots=True)
class RotatingBinaryLog:
    path: Path
    interval: str | None = None
    on_close: Callable[[LogSegment], None] | None = None
    _fp: object | None = None
    _pending: bytearray = field(default_factory=bytearray, init=False, repr=False)
    _window_start: datetime | None = None
    _segment_started_at: datetime | None = None
    _active_path: Path | None = None

    def __post_init__(self) -> None:
        self.path = self.path.expanduser()
        self.interval = normalize_log_interval(self.interval)

    @property
    def active_path(self) -> Path:
        if self._active_path is not None:
            return self._active_path
        if self._segment_started_at is None:
            return self.path
        return self._stamped_path(self._segment_started_at)

    @property
    def segment_started_at(self) -> datetime | None:
        return self._segment_started_at

    def write(self, data: bytes, now: datetime | None = None) -> int:
        if not data:
            return 0
        current_time = now if now is not None else None
        if current_time is None and self._segment_started_at is None:
            self._pending.extend(data)
            return len(data)
        if current_time is not None:
            self._advance_segment(current_time)
        elif self._segment_started_at is None:
            self._pending.extend(data)
            return len(data)
        self._ensure_open(self.active_path)
        assert self._fp is not None
        if self._pending:
            self._fp.write(self._pending)
            self._pending.clear()
        self._fp.write(data)
        self._fp.flush()
        return len(data)

    def close(self, now: datetime | None = None) -> LogSegment | None:
        if self._segment_started_at is None and self._pending:
            path_time = now if now is not None else current_gpst_calendar_datetime()
            self._advance_segment(path_time)
            self._ensure_open(self.active_path)
            assert self._fp is not None
            self._fp.write(self._pending)
            self._fp.flush()
            self._pending.clear()
        if self._segment_started_at is None or self._active_path is None:
            return None
        current_time = datetime.now(UTC)
        segment = LogSegment(
            path=self._active_path,
            interval=self.interval,
            started_at=self._segment_started_at,
            closed_at=current_time,
        )
        if self._fp is not None:
            self._fp.close()
            self._fp = None
        self._window_start = None
        self._segment_started_at = None
        self._active_path = None
        if self.on_close is not None:
            self.on_close(segment)
        return segment

    def _advance_segment(self, current_time: datetime) -> None:
        if self.interval is None:
            if self._segment_started_at is None:
                self._segment_started_at = current_time
                self._active_path = self._stamped_path(self._segment_started_at)
            return

        window_start = aligned_interval_start(current_time, self.interval)
        previous_window_start = self._window_start
        if previous_window_start != window_start:
            if previous_window_start is not None:
                self.close(window_start)
            self._window_start = window_start
            self._segment_started_at = current_time if previous_window_start is None else window_start
            self._active_path = self._stamped_path(self._segment_started_at)
        elif self._segment_started_at is None:
            self._segment_started_at = current_time
            self._active_path = self._stamped_path(self._segment_started_at)

    def _ensure_open(self, path: Path) -> None:
        if self._fp is not None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = path.open("ab")

    def _stamped_path(self, when: datetime) -> Path:
        suffix = "".join(self.path.suffixes)
        stem = self.path.name[: -len(suffix)] if suffix else self.path.name
        stamped = f"{stem}_{when.strftime('%Y%m%d_%H%M%S')}{suffix}"
        return self.path.with_name(stamped)
