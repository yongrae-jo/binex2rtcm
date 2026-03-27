"""RTCM scheduler payload models."""

from __future__ import annotations

from dataclasses import dataclass

from ..model.ephemeris import Ephemeris
from ..model.observation import EpochObservations, SatelliteObservation
from ..model.signals import Constellation
from ..model.station import StationInfo


@dataclass(slots=True)
class StationMessage:
    message_type: int
    station: StationInfo


@dataclass(slots=True)
class GlonassBiasMessage:
    message_type: int


@dataclass(slots=True)
class EphemerisMessage:
    message_type: int
    ephemeris: Ephemeris


@dataclass(slots=True)
class MsmMessage:
    message_type: int
    system: Constellation
    msm_level: int
    epoch: EpochObservations
    satellites: list[SatelliteObservation]


ScheduledPayload = StationMessage | GlonassBiasMessage | EphemerisMessage | MsmMessage
