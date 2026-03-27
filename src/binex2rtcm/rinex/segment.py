"""Buffered RINEX segment exporter."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from ..config import RinexExportConfig
from ..model.ephemeris import Ephemeris, GlonassEphemeris, KeplerEphemeris, SbasEphemeris
from ..model.observation import EpochObservations, SatelliteObservation
from ..model.station import StationInfo
from .header import rinex_sat_sort_key
from .nav_writer import RinexNavWriter
from .obs_writer import RinexObsWriter


def _ephemeris_key(eph: Ephemeris) -> tuple[object, ...]:
    if isinstance(eph, KeplerEphemeris):
        return (eph.system, eph.prn, eph.week, eph.toes, eph.iode, eph.iodc)
    if isinstance(eph, GlonassEphemeris):
        return (eph.system, eph.prn, eph.toe.gps_seconds, eph.frequency_channel, eph.iode)
    if isinstance(eph, SbasEphemeris):
        return (eph.system, eph.prn, eph.toe.gps_seconds, eph.tof.gps_seconds)
    return (eph.system, eph.prn, eph.toe.gps_seconds)


@dataclass(slots=True)
class RinexSegmentBuffer:
    config: RinexExportConfig
    marker_name: str = ""
    station: StationInfo | None = None
    epochs: list[EpochObservations] = field(default_factory=list)
    _ephemerides: dict[tuple[object, ...], Ephemeris] = field(default_factory=dict)
    _obs_writer: RinexObsWriter = field(default_factory=RinexObsWriter)
    _nav_writer: RinexNavWriter = field(default_factory=RinexNavWriter)

    def ingest_station(self, station: StationInfo) -> None:
        self.station = station

    def ingest_epoch(self, epoch: EpochObservations) -> None:
        if self.epochs and abs(self.epochs[-1].time.gps_seconds - epoch.time.gps_seconds) < 1e-6:
            merged = self._merge_epoch(self.epochs[-1], epoch)
            self.epochs[-1] = merged
            return
        self.epochs.append(epoch)

    def ingest_ephemeris(self, ephemeris: Ephemeris) -> None:
        self._ephemerides[_ephemeris_key(ephemeris)] = ephemeris

    def empty(self) -> bool:
        return not self.epochs and not self._ephemerides

    def export(self, segment_path: Path, generated_at: datetime | None = None) -> list[Path]:
        if not self.config.enabled:
            return []

        written: list[Path] = []
        if self.config.observation:
            obs_path = segment_path.with_suffix(".obs")
            result = self._obs_writer.write(obs_path, self.station, self.epochs, generated_at, self.marker_name)
            if result is not None:
                written.append(result)
        if self.config.navigation:
            nav_path = segment_path.with_suffix(".nav")
            result = self._nav_writer.write(nav_path, list(self._ephemerides.values()), generated_at)
            if result is not None:
                written.append(result)
        return written

    def reset(self, preserve_station: bool = True) -> None:
        if not preserve_station:
            self.station = None
        self.epochs.clear()
        self._ephemerides.clear()

    def _merge_epoch(self, left: EpochObservations, right: EpochObservations) -> EpochObservations:
        satellites: dict[tuple[object, int], SatelliteObservation] = {}
        for source in (*left.satellites, *right.satellites):
            key = (source.system, source.prn)
            current = satellites.get(key)
            if current is None:
                satellites[key] = source
                continue
            signal_map = {signal.signal_label: signal for signal in current.signals}
            for signal in source.signals:
                signal_map[signal.signal_label] = signal
            satellites[key] = SatelliteObservation(
                system=source.system,
                prn=source.prn,
                signals=[signal_map[label] for label in sorted(signal_map)],
                glonass_fcn=source.glonass_fcn if source.glonass_fcn is not None else current.glonass_fcn,
            )
        return EpochObservations(
            time=left.time,
            receiver_clock_offset_s=left.receiver_clock_offset_s
            if left.receiver_clock_offset_s is not None
            else right.receiver_clock_offset_s,
            satellites=sorted(satellites.values(), key=lambda item: rinex_sat_sort_key(item.system, item.prn)),
        )
