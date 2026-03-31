"""Normalized constellation and signal metadata."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from ..errors import UnsupportedMessageError

CLIGHT = 299792458.0
FREQ1_GLO = 1602.0e6
DFRQ1_GLO = 0.5625e6
FREQ2_GLO = 1246.0e6
DFRQ2_GLO = 0.4375e6


class Constellation(StrEnum):
    GPS = "GPS"
    GLO = "GLO"
    GAL = "GAL"
    BDS = "BDS"
    QZS = "QZS"
    SBS = "SBS"
    IRN = "IRN"


@dataclass(frozen=True, slots=True)
class SignalDefinition:
    label: str
    band: str
    msm_id: int
    slot: int
    carrier_hz: float


SYSTEM_PREFIX = {
    Constellation.GPS: "G",
    Constellation.GLO: "R",
    Constellation.GAL: "E",
    Constellation.BDS: "C",
    Constellation.QZS: "J",
    Constellation.SBS: "S",
    Constellation.IRN: "I",
}

MSM_BASE = {
    Constellation.GPS: 1070,
    Constellation.GLO: 1080,
    Constellation.GAL: 1090,
    Constellation.SBS: 1100,
    Constellation.QZS: 1110,
    Constellation.BDS: 1120,
    Constellation.IRN: 1130,
}

EPHEMERIS_MESSAGE = {
    Constellation.GPS: 1019,
    Constellation.GLO: 1020,
    Constellation.IRN: 1041,
    Constellation.BDS: 1042,
    Constellation.QZS: 1044,
    Constellation.GAL: 1045,
}

SIGNAL_MAP: dict[Constellation, dict[str, SignalDefinition]] = {
    Constellation.GPS: {
        "1C": SignalDefinition("1C", "L1", 2, 1, 1575.42e6),
        "1P": SignalDefinition("1P", "L1", 3, 1, 1575.42e6),
        "1W": SignalDefinition("1W", "L1", 4, 1, 1575.42e6),
        "1Y": SignalDefinition("1Y", "L1", 5, 1, 1575.42e6),
        "1M": SignalDefinition("1M", "L1", 6, 1, 1575.42e6),
        "2C": SignalDefinition("2C", "L2", 8, 2, 1227.60e6),
        "2P": SignalDefinition("2P", "L2", 9, 2, 1227.60e6),
        "2D": SignalDefinition("2D", "L2", 9, 2, 1227.60e6),
        "2W": SignalDefinition("2W", "L2", 10, 2, 1227.60e6),
        "2Y": SignalDefinition("2Y", "L2", 11, 2, 1227.60e6),
        "2M": SignalDefinition("2M", "L2", 12, 2, 1227.60e6),
        "2S": SignalDefinition("2S", "L2", 15, 2, 1227.60e6),
        "2L": SignalDefinition("2L", "L2", 16, 2, 1227.60e6),
        "2X": SignalDefinition("2X", "L2", 17, 2, 1227.60e6),
        "5I": SignalDefinition("5I", "L5", 22, 3, 1176.45e6),
        "5Q": SignalDefinition("5Q", "L5", 23, 3, 1176.45e6),
        "5X": SignalDefinition("5X", "L5", 24, 3, 1176.45e6),
        "1S": SignalDefinition("1S", "L1", 30, 1, 1575.42e6),
        "1L": SignalDefinition("1L", "L1", 31, 1, 1575.42e6),
        "1X": SignalDefinition("1X", "L1", 32, 1, 1575.42e6),
    },
    Constellation.GLO: {
        "1C": SignalDefinition("1C", "G1", 2, 1, FREQ1_GLO),
        "1P": SignalDefinition("1P", "G1", 3, 1, FREQ1_GLO),
        "2C": SignalDefinition("2C", "G2", 8, 2, FREQ2_GLO),
        "2P": SignalDefinition("2P", "G2", 9, 2, FREQ2_GLO),
        "3I": SignalDefinition("3I", "G3", 11, 3, 1202.025e6),
        "3Q": SignalDefinition("3Q", "G3", 12, 3, 1202.025e6),
        "3X": SignalDefinition("3X", "G3", 13, 3, 1202.025e6),
    },
    Constellation.GAL: {
        "1C": SignalDefinition("1C", "E1", 2, 1, 1575.42e6),
        "1A": SignalDefinition("1A", "E1", 3, 1, 1575.42e6),
        "1B": SignalDefinition("1B", "E1", 4, 1, 1575.42e6),
        "1X": SignalDefinition("1X", "E1", 5, 1, 1575.42e6),
        "1Z": SignalDefinition("1Z", "E1", 6, 1, 1575.42e6),
        "6C": SignalDefinition("6C", "E6", 8, 4, 1278.75e6),
        "6A": SignalDefinition("6A", "E6", 9, 4, 1278.75e6),
        "6B": SignalDefinition("6B", "E6", 10, 4, 1278.75e6),
        "6X": SignalDefinition("6X", "E6", 11, 4, 1278.75e6),
        "6Z": SignalDefinition("6Z", "E6", 12, 4, 1278.75e6),
        "7I": SignalDefinition("7I", "E5b", 14, 3, 1207.14e6),
        "7Q": SignalDefinition("7Q", "E5b", 15, 3, 1207.14e6),
        "7X": SignalDefinition("7X", "E5b", 16, 3, 1207.14e6),
        "8I": SignalDefinition("8I", "E5ab", 18, 2, 1191.795e6),
        "8Q": SignalDefinition("8Q", "E5ab", 19, 2, 1191.795e6),
        "8X": SignalDefinition("8X", "E5ab", 20, 2, 1191.795e6),
        "5I": SignalDefinition("5I", "E5a", 22, 2, 1176.45e6),
        "5Q": SignalDefinition("5Q", "E5a", 23, 2, 1176.45e6),
        "5X": SignalDefinition("5X", "E5a", 24, 2, 1176.45e6),
    },
    Constellation.SBS: {
        "1C": SignalDefinition("1C", "L1", 2, 1, 1575.42e6),
        "5I": SignalDefinition("5I", "L5", 22, 2, 1176.45e6),
        "5Q": SignalDefinition("5Q", "L5", 23, 2, 1176.45e6),
        "5X": SignalDefinition("5X", "L5", 24, 2, 1176.45e6),
    },
    Constellation.QZS: {
        "1C": SignalDefinition("1C", "L1", 2, 1, 1575.42e6),
        "6S": SignalDefinition("6S", "LEX", 9, 4, 1278.75e6),
        "6L": SignalDefinition("6L", "LEX", 10, 4, 1278.75e6),
        "6X": SignalDefinition("6X", "LEX", 11, 4, 1278.75e6),
        "2S": SignalDefinition("2S", "L2", 15, 2, 1227.60e6),
        "2L": SignalDefinition("2L", "L2", 16, 2, 1227.60e6),
        "2X": SignalDefinition("2X", "L2", 17, 2, 1227.60e6),
        "5I": SignalDefinition("5I", "L5", 22, 3, 1176.45e6),
        "5Q": SignalDefinition("5Q", "L5", 23, 3, 1176.45e6),
        "5X": SignalDefinition("5X", "L5", 24, 3, 1176.45e6),
        "1S": SignalDefinition("1S", "L1", 30, 1, 1575.42e6),
        "1L": SignalDefinition("1L", "L1", 31, 1, 1575.42e6),
        "1X": SignalDefinition("1X", "L1", 32, 1, 1575.42e6),
    },
    Constellation.BDS: {
        "2I": SignalDefinition("2I", "B1I", 2, 1, 1561.098e6),
        "2Q": SignalDefinition("2Q", "B1I", 3, 1, 1561.098e6),
        "2X": SignalDefinition("2X", "B1I", 4, 1, 1561.098e6),
        "6I": SignalDefinition("6I", "B3", 8, 3, 1268.52e6),
        "6Q": SignalDefinition("6Q", "B3", 9, 3, 1268.52e6),
        "6X": SignalDefinition("6X", "B3", 10, 3, 1268.52e6),
        "7I": SignalDefinition("7I", "B2I", 14, 2, 1207.14e6),
        "7Q": SignalDefinition("7Q", "B2I", 15, 2, 1207.14e6),
        "7X": SignalDefinition("7X", "B2I", 16, 2, 1207.14e6),
        "5D": SignalDefinition("5D", "B2a", 22, 4, 1176.45e6),
        "5P": SignalDefinition("5P", "B2a", 23, 4, 1176.45e6),
        "5X": SignalDefinition("5X", "B2a", 24, 4, 1176.45e6),
        "7D": SignalDefinition("7D", "B2b", 25, 2, 1207.14e6),
        "1D": SignalDefinition("1D", "B1C", 30, 5, 1575.42e6),
        "1P": SignalDefinition("1P", "B1C", 31, 5, 1575.42e6),
        "1X": SignalDefinition("1X", "B1C", 32, 5, 1575.42e6),
    },
    Constellation.IRN: {
        "5A": SignalDefinition("5A", "L5", 22, 1, 1176.45e6),
    },
}

DEFAULT_SIGNAL_PRIORITY: dict[Constellation, dict[str, int]] = {
    system: {label: index for index, label in enumerate(reversed(signals.keys()), start=1)}
    for system, signals in SIGNAL_MAP.items()
}


def satellite_id(system: Constellation, prn: int) -> str:
    return f"{SYSTEM_PREFIX[system]}{prn:02d}"


def signal_definition(system: Constellation, label: str) -> SignalDefinition:
    try:
        return SIGNAL_MAP[system][label]
    except KeyError as exc:  # pragma: no cover - defensive
        raise UnsupportedMessageError(f"Unsupported signal {system}:{label}") from exc


def msm_message_number(system: Constellation, level: int) -> int:
    if level < 1 or level > 7:
        raise UnsupportedMessageError(f"Unsupported MSM level: {level}")
    return MSM_BASE[system] + level


def carrier_frequency_hz(system: Constellation, label: str, glonass_fcn: int | None = None) -> float:
    definition = signal_definition(system, label)
    if system is Constellation.GLO and glonass_fcn is not None:
        if definition.band == "G1":
            return FREQ1_GLO + DFRQ1_GLO * glonass_fcn
        if definition.band == "G2":
            return FREQ2_GLO + DFRQ2_GLO * glonass_fcn
    return definition.carrier_hz


def wavelength_m(system: Constellation, label: str, glonass_fcn: int | None = None) -> float:
    return CLIGHT / carrier_frequency_hz(system, label, glonass_fcn)
