"""Normalized ephemeris model."""

from __future__ import annotations

from dataclasses import dataclass, field

from ..gnss_time import GNSSTime
from .signals import Constellation, satellite_id


@dataclass(slots=True)
class EphemerisBase:
    system: Constellation
    prn: int
    toe: GNSSTime

    @property
    def sat_id(self) -> str:
        return satellite_id(self.system, self.prn)


@dataclass(slots=True)
class KeplerEphemeris(EphemerisBase):
    week: int
    toes: float
    toc: GNSSTime
    ttr: GNSSTime
    iode: int
    iodc: int
    f0: float
    f1: float
    f2: float
    deln: float
    m0: float
    e: float
    sqrt_a: float
    cuc: float
    cus: float
    crc: float
    crs: float
    cic: float
    cis: float
    omega0: float
    omega: float
    i0: float
    omega_dot: float
    idot: float
    sva: int
    svh: int
    tgd: tuple[float, float] = (0.0, 0.0)
    code: int = 0
    flag: int = 0
    fit: float = 0.0
    source: str = ""


@dataclass(slots=True)
class GlonassEphemeris(EphemerisBase):
    tof: GNSSTime
    taun: float
    gamn: float
    dtaun: float
    position_m: tuple[float, float, float]
    velocity_mps: tuple[float, float, float]
    acceleration_mps2: tuple[float, float, float]
    svh: int
    frequency_channel: int
    age: int
    iode: int


@dataclass(slots=True)
class SbasEphemeris(EphemerisBase):
    tof: GNSSTime
    af0: float
    position_m: tuple[float, float, float]
    velocity_mps: tuple[float, float, float]
    acceleration_mps2: tuple[float, float, float]
    svh: int
    sva: int


Ephemeris = KeplerEphemeris | GlonassEphemeris | SbasEphemeris
