"""RINEX 3.05 observation writer."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import math
from pathlib import Path

from ..errors import UnsupportedMessageError
from ..model.observation import EpochObservations, SatelliteObservation
from ..model.signals import Constellation, signal_definition
from ..model.station import StationInfo
from .header import (
    RINEX_SYSTEM_ORDER,
    antenna_type_text,
    format_obs_header_time,
    format_obs_epoch,
    format_obs_value,
    header_line,
    leap_seconds,
    obs_header_type_line,
    program_line,
    rinex_sat_id,
    rinex_sat_sort_key,
    station_marker_name,
    system_code,
)

OBSERVABLE_PREFIXES = ("C", "L", "D", "S")


def _signal_sort_key(system: Constellation, label: str) -> tuple[int, str]:
    try:
        definition = signal_definition(system, label)
        return definition.slot, definition.label
    except UnsupportedMessageError:
        return 999, label


def _has_observable_value(value: float) -> bool:
    return math.isfinite(value) and value != 0.0


def _obs_codes_by_system(epochs: list[EpochObservations]) -> dict[Constellation, list[str]]:
    prefixes_by_system: dict[Constellation, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for epoch in epochs:
        for satellite in epoch.satellites:
            for signal in satellite.signals:
                if _has_observable_value(signal.pseudorange_m):
                    prefixes_by_system[satellite.system][signal.signal_label].add("C")
                if _has_observable_value(signal.carrier_cycles):
                    prefixes_by_system[satellite.system][signal.signal_label].add("L")
                if _has_observable_value(signal.doppler_hz):
                    prefixes_by_system[satellite.system][signal.signal_label].add("D")
                if _has_observable_value(signal.cnr_dbhz):
                    prefixes_by_system[satellite.system][signal.signal_label].add("S")

    codes_by_system: dict[Constellation, list[str]] = {}
    for system, prefixes_by_label in prefixes_by_system.items():
        ordered_labels = sorted(prefixes_by_label, key=lambda label: _signal_sort_key(system, label))
        codes_by_system[system] = [
            f"{prefix}{label}"
            for label in ordered_labels
            for prefix in OBSERVABLE_PREFIXES
            if prefix in prefixes_by_label[label]
        ]
    return codes_by_system


def _obs_type_lines(system: Constellation, codes: list[str]) -> list[str]:
    if not codes:
        return []
    prefix = system_code(system)
    lines: list[str] = []
    for index in range(0, len(codes), 13):
        chunk = codes[index : index + 13]
        if index == 0:
            content = f"{prefix} {len(codes):4d}" + "".join(f" {code:>3s}" for code in chunk)
        else:
            content = " " * 6 + "".join(f" {code:>3s}" for code in chunk)
        lines.append(header_line(content, "SYS / # / OBS TYPES"))
    return lines


def _interval_seconds(epochs: list[EpochObservations]) -> float | None:
    if len(epochs) < 2:
        return None
    deltas = []
    for previous, current in zip(epochs, epochs[1:]):
        delta = current.time - previous.time
        if delta > 0:
            deltas.append(delta)
    if not deltas:
        return None
    first = deltas[0]
    if any(abs(value - first) > 1e-3 for value in deltas[1:]):
        return None
    return first


def _glonass_slot_lines(epochs: list[EpochObservations]) -> list[str]:
    slots: dict[int, int] = {}
    for epoch in epochs:
        for satellite in epoch.by_system(Constellation.GLO):
            if satellite.glonass_fcn is not None:
                slots[satellite.prn] = satellite.glonass_fcn
    if not slots:
        return []

    items = sorted(slots.items())
    lines: list[str] = []
    for start in range(0, len(items), 8):
        chunk = items[start : start + 8]
        count = len(items) if start == 0 else 0
        content = f"{count:3d}" if start == 0 else "   "
        for prn, fcn in chunk:
            content += f" R{prn:02d} {fcn:2d}"
        lines.append(header_line(content, "GLONASS SLOT / FRQ #"))
    return lines


def _glonass_code_phase_bias_line() -> str:
    content = ""
    for code in ("C1C", "C1P", "C2C", "C2P"):
        content += f" {code:>3s}{0.0:8.3f}"
    return header_line(content, "GLONASS COD/PHS/BIS")


def _header_lines(
    station: StationInfo | None,
    epochs: list[EpochObservations],
    generated_at: datetime | None = None,
    marker_name: str | None = None,
) -> list[str]:
    first_time = epochs[0].time if epochs else None
    last_time = epochs[-1].time if epochs else None
    codes_by_system = _obs_codes_by_system(epochs)
    receiver_serial = "" if station is None else station.receiver_serial[:20]
    receiver_type = "" if station is None else station.receiver_type[:20]
    receiver_version = "" if station is None else station.receiver_version[:20]
    antenna_serial = "" if station is None else station.antenna_serial[:20]
    lines = [
        obs_header_type_line(),
        program_line(generated_at),
        header_line(station_marker_name(station, marker_name), "MARKER NAME"),
        header_line("" if station is None else station.site_identifier[:20], "MARKER NUMBER"),
        header_line("" if station is None else "GEODETIC", "MARKER TYPE"),
        header_line("", "OBSERVER / AGENCY"),
        header_line(f"{receiver_serial:<20}{receiver_type:<20}{receiver_version:<20}", "REC # / TYPE / VERS"),
        header_line(f"{antenna_serial:<20}{antenna_type_text(station):<20}", "ANT # / TYPE"),
    ]
    if station is None:
        lines.append(header_line(f"{0.0:14.4f}{0.0:14.4f}{0.0:14.4f}{'':18s}", "APPROX POSITION XYZ"))
        lines.append(header_line(f"{0.0:14.4f}{0.0:14.4f}{0.0:14.4f}{'':18s}", "ANTENNA: DELTA H/E/N"))
    else:
        lines.append(
            header_line(
                f"{station.ecef_xyz_m[0]:14.4f}{station.ecef_xyz_m[1]:14.4f}{station.ecef_xyz_m[2]:14.4f}{'':18s}",
                "APPROX POSITION XYZ",
            )
        )
        lines.append(
            header_line(
                f"{station.antenna_height_m:14.4f}{0.0:14.4f}{0.0:14.4f}{'':18s}",
                "ANTENNA: DELTA H/E/N",
            )
        )

    for system in RINEX_SYSTEM_ORDER:
        lines.extend(_obs_type_lines(system, codes_by_system.get(system, [])))

    lines.append(header_line("DBHZ", "SIGNAL STRENGTH UNIT"))

    interval = _interval_seconds(epochs)
    if interval is not None:
        lines.append(header_line(f"{interval:10.3f}{'':50s}", "INTERVAL"))

    if first_time is not None:
        dt = first_time.datetime_gpst
        lines.append(header_line(format_obs_header_time(dt, "GPS"), "TIME OF FIRST OBS"))
    if last_time is not None:
        dt = last_time.datetime_gpst
        lines.append(header_line(format_obs_header_time(dt, "GPS"), "TIME OF LAST OBS"))

    lines.extend(_glonass_slot_lines(epochs))
    if any(epoch.by_system(Constellation.GLO) for epoch in epochs):
        lines.append(_glonass_code_phase_bias_line())

    lines.append(header_line(f"{leap_seconds(first_time):6d}", "LEAP SECONDS"))
    lines.append(header_line("", "END OF HEADER"))
    return lines


def _field_value(code: str, satellite: SatelliteObservation) -> str:
    signal_map = {signal.signal_label: signal for signal in satellite.signals}
    signal = signal_map.get(code[1:])
    if signal is None:
        return format_obs_value(None)
    if code.startswith("C"):
        return format_obs_value(signal.pseudorange_m)
    if code.startswith("L"):
        return format_obs_value(signal.carrier_cycles, signal.lli)
    if code.startswith("D"):
        return format_obs_value(signal.doppler_hz)
    if code.startswith("S"):
        return format_obs_value(signal.cnr_dbhz)
    return format_obs_value(None)


def _epoch_lines(epoch: EpochObservations, codes_by_system: dict[Constellation, list[str]]) -> list[str]:
    satellites = sorted(epoch.satellites, key=lambda item: rinex_sat_sort_key(item.system, item.prn))
    lines = [format_obs_epoch(epoch.time, len(satellites), epoch.receiver_clock_offset_s)]
    for satellite in satellites:
        fields = "".join(_field_value(code, satellite) for code in codes_by_system.get(satellite.system, []))
        lines.append(f"{rinex_sat_id(satellite.system, satellite.prn)}{fields}\n")
    return lines


class RinexObsWriter:
    """Write buffered observation epochs into a RINEX 3.05 file."""

    def write(
        self,
        path: Path,
        station: StationInfo | None,
        epochs: list[EpochObservations],
        generated_at: datetime | None = None,
        marker_name: str | None = None,
    ) -> Path | None:
        if not epochs:
            return None
        codes_by_system = _obs_codes_by_system(epochs)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="ascii", newline="") as fp:
            for line in _header_lines(station, epochs, generated_at, marker_name):
                fp.write(line)
            for epoch in epochs:
                for line in _epoch_lines(epoch, codes_by_system):
                    fp.write(line)
        return path
