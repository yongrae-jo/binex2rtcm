"""BINEX emission scheduler.

The BINEX side can forward a whole mixed-constellation epoch in one record, so
the scheduler only needs to decide *when* station metadata and ephemerides are
re-emitted around observation epochs.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ..config import SchedulerConfig
from ..model.ephemeris import Ephemeris
from ..model.observation import EpochObservations
from ..model.station import StationInfo


@dataclass(slots=True)
class BinexScheduler:
    """Schedule normalized items for BINEX re-encoding."""

    config: SchedulerConfig
    station: StationInfo | None = None
    _ephemerides: dict[tuple[object, int], Ephemeris] = field(init=False, default_factory=dict)
    _last_metadata_emit_s: float = field(init=False, default=-1e18)
    _last_ephemeris_emit_s: dict[tuple[object, int], float] = field(init=False, default_factory=dict)

    def bootstrap(self) -> list[object]:
        if not self.config.emit_metadata_on_start or self.station is None:
            return []
        self._last_metadata_emit_s = 0.0
        return [self.station]

    def ingest(self, item: object) -> list[object]:
        if isinstance(item, StationInfo):
            self.station = item
            return [item]

        if isinstance(item, Ephemeris):
            key = (item.system, item.prn)
            self._ephemerides[key] = item
            self._last_ephemeris_emit_s[key] = item.toe.gps_seconds
            return [item] if self.config.emit_ephemeris_on_change else []

        if not isinstance(item, EpochObservations):
            return []

        messages: list[object] = []
        now_s = item.time.gps_seconds

        if self.station is not None and now_s - self._last_metadata_emit_s >= self.config.metadata_interval_s:
            messages.append(self.station)
            self._last_metadata_emit_s = now_s

        for key, eph in list(self._ephemerides.items()):
            if now_s - self._last_ephemeris_emit_s.get(key, -1e18) >= self.config.ephemeris_interval_s:
                messages.append(eph)
                self._last_ephemeris_emit_s[key] = now_s

        messages.append(item)
        return messages
