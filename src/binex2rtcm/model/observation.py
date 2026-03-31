"""Normalized observation model."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from ..gnss_time import GNSSTime
from .signals import Constellation, satellite_id


@dataclass(slots=True)
class SignalObservation:
    signal_label: str
    pseudorange_m: float
    carrier_cycles: float
    doppler_hz: float
    cnr_dbhz: float
    frequency_slot: int
    source_priority: int = 0
    lock_time_s: float = 0.0
    half_cycle_ambiguity: bool = False
    slip_detected: bool = False
    lli: int = 0


@dataclass(slots=True)
class SatelliteObservation:
    system: Constellation
    prn: int
    signals: list[SignalObservation] = field(default_factory=list)
    glonass_fcn: int | None = None

    @property
    def sat_id(self) -> str:
        return satellite_id(self.system, self.prn)


@dataclass(slots=True)
class EpochObservations:
    time: GNSSTime
    satellites: list[SatelliteObservation]
    receiver_clock_offset_s: float | None = None

    def by_system(self, system: Constellation) -> list[SatelliteObservation]:
        return [sat for sat in self.satellites if sat.system is system]

    def systems(self) -> set[Constellation]:
        return {sat.system for sat in self.satellites}

    def __iter__(self) -> Iterable[SatelliteObservation]:
        return iter(self.satellites)
