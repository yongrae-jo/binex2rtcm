"""Reference station metadata."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class StationInfo:
    station_id: int
    ecef_xyz_m: tuple[float, float, float]
    antenna_height_m: float = 0.0
    antenna_descriptor: str = ""
    antenna_radome: str = ""
    antenna_serial: str = ""
    receiver_type: str = ""
    receiver_version: str = ""
    receiver_serial: str = ""
    marker_name: str = ""
    site_name: str = ""
    site_identifier: str = ""
    metadata_format: str = ""
