"""Reference station metadata."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class StationInfo:
    station_id: int
    ecef_xyz_m: tuple[float, float, float] | None = None
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

    def has_position(self) -> bool:
        return self.ecef_xyz_m is not None

    def has_antenna_metadata(self) -> bool:
        return any((self.antenna_descriptor, self.antenna_radome, self.antenna_serial))

    def has_receiver_metadata(self) -> bool:
        return any((self.receiver_type, self.receiver_version, self.receiver_serial))

    def has_identity_metadata(self) -> bool:
        return any((self.marker_name, self.site_name, self.site_identifier, self.metadata_format))

    def has_any_metadata(self) -> bool:
        return (
            self.has_position()
            or abs(self.antenna_height_m) > 0.0
            or self.has_antenna_metadata()
            or self.has_receiver_metadata()
            or self.has_identity_metadata()
            or self.station_id > 0
        )
