"""Buffered RINEX segment exporter."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import re

from ..config import RinexExportConfig
from ..model.ephemeris import Ephemeris, GlonassEphemeris, KeplerEphemeris, SbasEphemeris
from ..model.observation import EpochObservations, SatelliteObservation
from ..model.station import StationInfo
from .crx import convert_observation_rnx_to_crx
from .header import rinex_sat_sort_key
from .nav_writer import RinexNavWriter
from .obs_writer import RinexObsWriter

_STAMPED_SEGMENT_RE = re.compile(r"^(?P<stem>.+)_(?P<tag>\d{8}_\d{6})$")


def _ephemeris_key(eph: Ephemeris) -> tuple[object, ...]:
    if isinstance(eph, KeplerEphemeris):
        return (eph.system, eph.prn, eph.week, eph.toes, eph.iode, eph.iodc)
    if isinstance(eph, GlonassEphemeris):
        return (eph.system, eph.prn, eph.toe.gps_seconds, eph.frequency_channel, eph.iode)
    if isinstance(eph, SbasEphemeris):
        return (eph.system, eph.prn, eph.toe.gps_seconds, eph.tof.gps_seconds)
    return (eph.system, eph.prn, eph.toe.gps_seconds)


def _segment_name_parts(segment_path: Path, generated_at: datetime | None = None) -> tuple[str, str]:
    suffix = "".join(segment_path.suffixes)
    base_name = segment_path.name[: -len(suffix)] if suffix else segment_path.name
    match = _STAMPED_SEGMENT_RE.match(base_name)
    if match is not None:
        return match.group("stem"), match.group("tag")
    time_tag = generated_at.strftime("%Y%m%d_%H%M%S") if generated_at is not None else datetime.now().strftime("%Y%m%d_%H%M%S")
    return base_name, time_tag


def build_rinex_artifact_path(
    segment_path: Path,
    file_code: str,
    generated_at: datetime | None = None,
    suffix: str = ".rnx",
) -> Path:
    stem, time_tag = _segment_name_parts(segment_path, generated_at)
    return segment_path.with_name(f"{stem}_{file_code}_{time_tag}{suffix}")


@dataclass(slots=True)
class RinexSegmentSnapshot:
    config: RinexExportConfig
    marker_name: str = ""
    station: StationInfo | None = None
    epochs: list[EpochObservations] = field(default_factory=list)
    ephemerides: dict[tuple[object, ...], Ephemeris] = field(default_factory=dict)
    _obs_writer: RinexObsWriter = field(default_factory=RinexObsWriter)
    _nav_writer: RinexNavWriter = field(default_factory=RinexNavWriter)

    def empty(self) -> bool:
        return not self.epochs and not self.ephemerides

    def export(self, segment_path: Path, generated_at: datetime | None = None) -> list[Path]:
        if not self.config.enabled or self.empty():
            return []

        written: list[Path] = []
        if self.config.observation:
            obs_path = build_rinex_artifact_path(segment_path, "MO", generated_at)
            result = self._obs_writer.write(obs_path, self.station, self.epochs, generated_at, self.marker_name)
            if result is not None:
                written.append(result)
                if self.config.crx:
                    crx_path = convert_observation_rnx_to_crx(result)
                    if crx_path is not None:
                        written.append(crx_path)
        if self.config.navigation:
            nav_path = build_rinex_artifact_path(segment_path, "MN", generated_at)
            result = self._nav_writer.write(nav_path, list(self.ephemerides.values()), generated_at)
            if result is not None:
                written.append(result)
        return written


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
        snapshot = RinexSegmentSnapshot(
            config=self.config,
            marker_name=self.marker_name,
            station=self.station,
            epochs=list(self.epochs),
            ephemerides=dict(self._ephemerides),
        )
        return snapshot.export(segment_path, generated_at)

    def detach_snapshot(self, preserve_station: bool = True) -> RinexSegmentSnapshot | None:
        if self.empty():
            return None
        snapshot = RinexSegmentSnapshot(
            config=self.config,
            marker_name=self.marker_name,
            station=self.station,
            epochs=self.epochs,
            ephemerides=self._ephemerides,
        )
        if not preserve_station:
            self.station = None
        self.epochs = []
        self._ephemerides = {}
        return snapshot

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
