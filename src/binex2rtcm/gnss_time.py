"""GNSS time conversion helpers.

Internal time representation uses GPST seconds from the GPS epoch.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from math import fmod

GPS_EPOCH = datetime(1980, 1, 6, tzinfo=UTC)
SECONDS_PER_WEEK = 604800.0
SECONDS_PER_DAY = 86400.0
BDS_TO_GPS_WEEK_OFFSET = 1356
BDS_TO_GPS_SECONDS_OFFSET = 14.0

# GPST-UTC offsets effective from these UTC dates.
LEAP_SECONDS: list[tuple[datetime, int]] = [
    (datetime(1981, 7, 1, tzinfo=UTC), 1),
    (datetime(1982, 7, 1, tzinfo=UTC), 2),
    (datetime(1983, 7, 1, tzinfo=UTC), 3),
    (datetime(1985, 7, 1, tzinfo=UTC), 4),
    (datetime(1988, 1, 1, tzinfo=UTC), 5),
    (datetime(1990, 1, 1, tzinfo=UTC), 6),
    (datetime(1991, 1, 1, tzinfo=UTC), 7),
    (datetime(1992, 7, 1, tzinfo=UTC), 8),
    (datetime(1993, 7, 1, tzinfo=UTC), 9),
    (datetime(1994, 7, 1, tzinfo=UTC), 10),
    (datetime(1996, 1, 1, tzinfo=UTC), 11),
    (datetime(1997, 7, 1, tzinfo=UTC), 12),
    (datetime(1999, 1, 1, tzinfo=UTC), 13),
    (datetime(2006, 1, 1, tzinfo=UTC), 14),
    (datetime(2009, 1, 1, tzinfo=UTC), 15),
    (datetime(2012, 7, 1, tzinfo=UTC), 16),
    (datetime(2015, 7, 1, tzinfo=UTC), 17),
    (datetime(2017, 1, 1, tzinfo=UTC), 18),
]


def gpst_datetime(seconds: float) -> datetime:
    return GPS_EPOCH + timedelta(seconds=seconds)


def gps_utc_offset(utc_dt: datetime) -> int:
    offset = 0
    for effective, value in LEAP_SECONDS:
        if utc_dt >= effective:
            offset = value
        else:
            break
    return offset


def utc_to_gpst_seconds(utc_dt: datetime) -> float:
    return (utc_dt - GPS_EPOCH).total_seconds() + gps_utc_offset(utc_dt)


def gpst_to_utc_datetime(seconds: float) -> datetime:
    # A short fixed-point iteration is enough because leap offsets are piecewise
    # constant.
    utc_dt = gpst_datetime(seconds)
    for _ in range(3):
        utc_dt = GPS_EPOCH + timedelta(seconds=seconds - gps_utc_offset(utc_dt))
    return utc_dt


def gps_from_week_tow(week: int, tow: float) -> float:
    return week * SECONDS_PER_WEEK + tow


def gps_to_week_tow(seconds: float) -> tuple[int, float]:
    week = int(seconds // SECONDS_PER_WEEK)
    tow = seconds - week * SECONDS_PER_WEEK
    return week, tow


def bdt_from_week_tow(week: int, tow: float) -> float:
    return gps_from_week_tow(week + BDS_TO_GPS_WEEK_OFFSET, tow + BDS_TO_GPS_SECONDS_OFFSET)


def gps_to_bdt_week_tow(seconds: float) -> tuple[int, float]:
    shifted = seconds - BDS_TO_GPS_SECONDS_OFFSET
    week, tow = gps_to_week_tow(shifted)
    return week - BDS_TO_GPS_WEEK_OFFSET, tow


def adjust_week(base_seconds: float, tow: float) -> float:
    week, tow_prev = gps_to_week_tow(base_seconds)
    if tow < tow_prev - 302400.0:
        tow += SECONDS_PER_WEEK
    elif tow > tow_prev + 302400.0:
        tow -= SECONDS_PER_WEEK
    return gps_from_week_tow(week, tow)


def adjust_day(base_seconds: float, tod: float) -> float:
    dt = gpst_datetime(base_seconds)
    tod_prev = dt.hour * 3600.0 + dt.minute * 60.0 + dt.second + dt.microsecond * 1e-6
    if tod < tod_prev - 43200.0:
        tod += SECONDS_PER_DAY
    elif tod > tod_prev + 43200.0:
        tod -= SECONDS_PER_DAY
    day_start = datetime(dt.year, dt.month, dt.day, tzinfo=UTC)
    return (day_start - GPS_EPOCH).total_seconds() + tod


def glonass_day_index(utc_plus_3h: datetime) -> int:
    year_start = datetime(utc_plus_3h.year - (utc_plus_3h.year % 4), 1, 1, tzinfo=UTC)
    return int((utc_plus_3h - year_start).total_seconds() // SECONDS_PER_DAY) + 1


@dataclass(frozen=True, slots=True)
class GNSSTime:
    """Internal GPST-based timestamp."""

    gps_seconds: float

    @classmethod
    def from_gps_week_tow(cls, week: int, tow: float) -> "GNSSTime":
        return cls(gps_from_week_tow(week, tow))

    @classmethod
    def from_bdt_week_tow(cls, week: int, tow: float) -> "GNSSTime":
        return cls(bdt_from_week_tow(week, tow))

    @property
    def datetime_gpst(self) -> datetime:
        return gpst_datetime(self.gps_seconds)

    @property
    def datetime_utc(self) -> datetime:
        return gpst_to_utc_datetime(self.gps_seconds)

    def gps_week_tow(self) -> tuple[int, float]:
        return gps_to_week_tow(self.gps_seconds)

    def bdt_week_tow(self) -> tuple[int, float]:
        return gps_to_bdt_week_tow(self.gps_seconds)

    def add(self, seconds: float) -> "GNSSTime":
        return GNSSTime(self.gps_seconds + seconds)

    def seconds_of_week(self) -> float:
        return self.gps_week_tow()[1]

    def seconds_of_day(self) -> float:
        return fmod(self.gps_seconds, SECONDS_PER_DAY)

    def __sub__(self, other: "GNSSTime") -> float:
        return self.gps_seconds - other.gps_seconds
