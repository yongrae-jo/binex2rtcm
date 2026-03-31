"""RINEX 3.05 formatting helpers."""

from __future__ import annotations

from datetime import UTC, datetime
import math

from ..gnss_time import GNSSTime, gps_utc_offset
from ..model.signals import Constellation
from ..model.station import StationInfo

RINEX_VERSION = 3.05
OBS_PROGRAM = "binex2rtcm"
OBSERVATION_LABEL = "OBSERVATION DATA"
NAVIGATION_LABEL = "N: GNSS NAV DATA"
SYSTEM_TEXT = {
    None: "M: Mixed",
    Constellation.GPS: "G: GPS",
    Constellation.GLO: "R: GLONASS",
    Constellation.GAL: "E: Galileo",
    Constellation.BDS: "C: BeiDou",
    Constellation.QZS: "J: QZSS",
    Constellation.SBS: "S: SBAS Payload",
    Constellation.IRN: "I: NavIC",
}
SYSTEM_CODE = {
    Constellation.GPS: "G",
    Constellation.GLO: "R",
    Constellation.GAL: "E",
    Constellation.BDS: "C",
    Constellation.QZS: "J",
    Constellation.IRN: "I",
    Constellation.SBS: "S",
}
RINEX_SYSTEM_ORDER = (
    Constellation.GPS,
    Constellation.GLO,
    Constellation.GAL,
    Constellation.BDS,
    Constellation.QZS,
    Constellation.IRN,
    Constellation.SBS,
)
RINEX_SYSTEM_RANK = {system: index for index, system in enumerate(RINEX_SYSTEM_ORDER)}


def header_line(content: str, label: str) -> str:
    return f"{content[:60]:<60}{label:<20}\n"


def rinex_datetime(now: datetime | None = None) -> str:
    value = (now or datetime.now(UTC)).astimezone(UTC)
    return value.strftime("%Y%m%d %H%M%S UTC")


def system_text(system: Constellation | None) -> str:
    return SYSTEM_TEXT[system]


def system_code(system: Constellation) -> str:
    return SYSTEM_CODE[system]


def rinex_sat_id(system: Constellation, prn: int) -> str:
    if system is Constellation.QZS:
        prn = prn - 192 if prn >= 193 else prn
        return f"J{prn:02d}"
    if system is Constellation.SBS:
        prn = prn - 100 if prn >= 100 else prn
        return f"S{prn:02d}"
    if system is Constellation.GPS:
        return f"G{prn:02d}"
    if system is Constellation.GLO:
        return f"R{prn:02d}"
    if system is Constellation.GAL:
        return f"E{prn:02d}"
    if system is Constellation.BDS:
        return f"C{prn:02d}"
    if system is Constellation.IRN:
        return f"I{prn:02d}"
    return f"{system.value[0]}{prn:02d}"


def rinex_sat_sort_key(system: Constellation, prn: int) -> tuple[int, int, str]:
    return RINEX_SYSTEM_RANK.get(system, len(RINEX_SYSTEM_RANK)), prn, rinex_sat_id(system, prn)


def format_obs_epoch(time: GNSSTime, satellites: int, clock_offset_s: float | None = None) -> str:
    dt = time.datetime_gpst
    second = dt.second + dt.microsecond * 1e-6
    base = f"> {dt.year:04d} {dt.month:02d} {dt.day:02d} {dt.hour:02d} {dt.minute:02d}{second:11.7f}  0{satellites:3d}"
    if clock_offset_s is None:
        return f"{base}{'':21s}\n"
    return f"{base}{clock_offset_s:21.12f}\n"


def format_obs_value(value: float | None, lli: int | None = None) -> str:
    if value is None or not math.isfinite(value) or abs(value) >= 1e9:
        return " " * 16
    lli_text = " " if lli is None or lli <= 0 else str(min(lli, 9))
    return f"{value:14.3f}{lli_text} "


def format_obs_header_time(dt: datetime, time_system: str | None = "GPS") -> str:
    second = dt.second + dt.microsecond * 1e-6
    system = "" if not time_system else time_system[:3]
    return f"{dt.year:6d}{dt.month:6d}{dt.day:6d}{dt.hour:6d}{dt.minute:6d}{second:13.7f}{'':5s}{system:<3}{'':9s}"


def format_nav_value(value: float) -> str:
    if not math.isfinite(value):
        value = 0.0
    return f"{value:19.12E}".replace("E", "D")


def nav_epoch_fields(dt: datetime) -> str:
    second = int(round(dt.second + dt.microsecond * 1e-6))
    return f"{dt.year:04d} {dt.month:02d} {dt.day:02d} {dt.hour:02d} {dt.minute:02d} {second:02d}"


def nav_first_line_prefix(sat_id: str, dt: datetime) -> str:
    return f"{sat_id:<3} {nav_epoch_fields(dt)}"


def nav_continuation_prefix() -> str:
    return "    "


def obs_header_type_line() -> str:
    content = f"{RINEX_VERSION:9.2f}{'':11}{OBSERVATION_LABEL:<20}{system_text(None):<20}"
    return header_line(content, "RINEX VERSION / TYPE")


def nav_header_type_line(system: Constellation | None = None) -> str:
    content = f"{RINEX_VERSION:9.2f}{'':11}{NAVIGATION_LABEL:<20}{system_text(system):<20}"
    return header_line(content, "RINEX VERSION / TYPE")


def program_line(now: datetime | None = None) -> str:
    return header_line(f"{OBS_PROGRAM:<20}{'':<20}{rinex_datetime(now):<20}", "PGM / RUN BY / DATE")


def station_marker_name(station: StationInfo | None, preferred: str | None = None) -> str:
    if preferred:
        return preferred[:60]
    if station is None:
        return ""
    for value in (station.marker_name, station.site_name, station.site_identifier):
        if value:
            return value
    return ""


def antenna_type_text(station: StationInfo | None) -> str:
    if station is None:
        return ""
    descriptor = station.antenna_descriptor[:16].ljust(16)
    radome = station.antenna_radome[:4].ljust(4)
    return f"{descriptor}{radome}".rstrip()


def receiver_version_text(station: StationInfo | None) -> str:
    return "" if station is None else station.receiver_version


def leap_seconds(time: GNSSTime | None) -> int:
    if time is None:
        return gps_utc_offset(datetime.now(UTC))
    return gps_utc_offset(time.datetime_utc)
