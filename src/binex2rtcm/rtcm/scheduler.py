"""RTCM emission scheduler."""

from __future__ import annotations

from dataclasses import dataclass, field

from ..config import SchedulerConfig
from ..model.ephemeris import Ephemeris
from ..model.observation import EpochObservations
from ..model.signals import Constellation, EPHEMERIS_MESSAGE, msm_message_number
from ..model.station import StationInfo
from .messages import EphemerisMessage, GlonassBiasMessage, MsmMessage, ScheduledPayload, StationMessage


@dataclass(slots=True)
class RtcmScheduler:
    config: SchedulerConfig
    station: StationInfo | None = None
    _ephemerides: dict[tuple[Constellation, int], Ephemeris] = field(init=False, default_factory=dict)
    _last_metadata_emit_s: float = field(init=False, default=-1e18)
    _last_ephemeris_emit_s: dict[tuple[Constellation, int], float] = field(init=False, default_factory=dict)

    def bootstrap(self) -> list[ScheduledPayload]:
        if not self.config.emit_metadata_on_start:
            return []
        messages = self._metadata_messages()
        if not messages:
            return []
        self._last_metadata_emit_s = 0.0
        return messages

    def ingest(self, item: object) -> list[ScheduledPayload]:
        if isinstance(item, StationInfo):
            self.station = item
            return self._metadata_messages()
        if isinstance(item, Ephemeris):
            key = (item.system, item.prn)
            self._ephemerides[key] = item
            self._last_ephemeris_emit_s[key] = item.toe.gps_seconds
            message_type = EPHEMERIS_MESSAGE.get(item.system)
            if self.config.emit_ephemeris_on_change and message_type is not None:
                return [EphemerisMessage(message_type, item)]
            return []
        if isinstance(item, EpochObservations):
            messages: list[ScheduledPayload] = []
            now_s = item.time.gps_seconds
            if now_s - self._last_metadata_emit_s >= self.config.metadata_interval_s:
                messages.extend(self._metadata_messages())
                self._last_metadata_emit_s = now_s
            for key, eph in list(self._ephemerides.items()):
                if now_s - self._last_ephemeris_emit_s.get(key, -1e18) >= self.config.ephemeris_interval_s:
                    message_type = EPHEMERIS_MESSAGE.get(eph.system)
                    if message_type is None:
                        continue
                    messages.append(EphemerisMessage(message_type, eph))
                    self._last_ephemeris_emit_s[key] = now_s
            for system in sorted(item.systems(), key=lambda value: value.value):
                satellites = item.by_system(system)
                if not satellites:
                    continue
                msm_level = int(self.config.msm_level_by_system.get(system.value, 7))
                if msm_level < 4 or msm_level > 7:
                    continue
                messages.append(
                    MsmMessage(
                        message_type=msm_message_number(system, msm_level),
                        system=system,
                        msm_level=msm_level,
                        epoch=item,
                        satellites=satellites,
                    )
                )
            return messages
        return []

    def _station_messages(self) -> list[ScheduledPayload]:
        if self.station is None:
            return []
        return [
            StationMessage(1006, self.station),
            StationMessage(1008, self.station),
            StationMessage(1033, self.station),
        ]

    def _metadata_messages(self) -> list[ScheduledPayload]:
        messages = self._station_messages()
        if not messages:
            return []
        messages.append(GlonassBiasMessage(message_type=1230))
        return messages
