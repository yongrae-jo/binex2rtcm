"""RTCM 3.x message encoder.

Several field formulas are aligned with RTKLIB's `rtcm3e.c` encoder logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
import logging
import math

from ..errors import UnsupportedMessageError
from ..gnss_time import GNSSTime, glonass_day_index, gpst_to_utc_datetime
from ..model.ephemeris import (
    Ephemeris,
    GALILEO_INAV_DATA_SOURCE,
    GlonassEphemeris,
    KeplerEphemeris,
    is_galileo_inav_data_source,
)
from ..model.observation import SatelliteObservation, SignalObservation
from ..model.signals import (
    CLIGHT,
    Constellation,
    EPHEMERIS_MESSAGE,
    carrier_frequency_hz,
    signal_definition,
    wavelength_m,
)
from ..model.station import StationInfo
from .bitbuffer import BitBuffer
from .messages import EphemerisMessage, GlonassBiasMessage, MsmMessage, ScheduledPayload, StationMessage

LOGGER = logging.getLogger(__name__)

SC2RAD = math.pi
RANGE_MS = CLIGHT * 0.001
P2_5 = 2.0 ** -5
P2_6 = 2.0 ** -6
P2_10 = 2.0 ** -10
P2_11 = 2.0 ** -11
P2_19 = 2.0 ** -19
P2_20 = 2.0 ** -20
P2_24 = 2.0 ** -24
P2_29 = 2.0 ** -29
P2_30 = 2.0 ** -30
P2_31 = 2.0 ** -31
P2_32 = 2.0 ** -32
P2_33 = 2.0 ** -33
P2_34 = 2.0 ** -34
P2_40 = 2.0 ** -40
P2_43 = 2.0 ** -43
P2_46 = 2.0 ** -46
P2_50 = 2.0 ** -50
P2_55 = 2.0 ** -55
P2_59 = 2.0 ** -59
P2_66 = 2.0 ** -66

MSM_MAX_CELLS = 64


def _round(value: float) -> int:
    return int(math.floor(value + 0.5))


def _round_u(value: float) -> int:
    return int(math.floor(value + 0.5))


def _canonical_signal_label(system: Constellation, label: str) -> str:
    if system is Constellation.GPS:
        if label in {"1Y", "1M", "1N"}:
            return "1P"
        if label in {"2D", "2Y", "2M", "2N"}:
            return "2P"
    return label


def _has_meaningful_observable(signal: SignalObservation) -> bool:
    return any(
        math.isfinite(value) and value != 0.0
        for value in (
            signal.pseudorange_m,
            signal.carrier_cycles,
            signal.doppler_hz,
            signal.cnr_dbhz,
        )
    )


def _qzs_satellite_number(prn: int) -> int:
    # Some BINEX sources report QZSS as 193-202 while others use 1-10.
    return prn - 192 if prn >= 193 else prn


def _msm_satellite_number(system: Constellation, prn: int) -> int:
    if system is Constellation.QZS:
        return _qzs_satellite_number(prn)
    if system is Constellation.SBS:
        return prn - 119
    return prn


def _crc24q(data: bytes) -> int:
    crc = 0
    for byte in data:
        crc ^= byte << 16
        for _ in range(8):
            crc <<= 1
            if crc & 0x1000000:
                crc ^= 0x1864CFB
            crc &= 0xFFFFFF
    return crc


def _append_signed_38(body: BitBuffer, value: int) -> None:
    body.append_signed(value, 38)


def _append_string_field(body: BitBuffer, value: str, max_length: int = 31) -> None:
    trimmed = value[:max_length]
    body.append_unsigned(len(trimmed), 8)
    body.append_ascii(trimmed)


def _antenna_descriptor_text(station: StationInfo) -> str:
    descriptor = station.antenna_descriptor.strip()
    radome = station.antenna_radome.strip()
    if not radome:
        return descriptor
    if not descriptor:
        return radome
    if descriptor.endswith(radome):
        return descriptor
    if len(descriptor) <= 16 and len(radome) <= 4:
        return f"{descriptor:<16}{radome}"
    return f"{descriptor} {radome}"


def _msm_lock(lock_s: float) -> int:
    if lock_s < 0.032:
        return 0
    if lock_s < 0.064:
        return 1
    if lock_s < 0.128:
        return 2
    if lock_s < 0.256:
        return 3
    if lock_s < 0.512:
        return 4
    if lock_s < 1.024:
        return 5
    if lock_s < 2.048:
        return 6
    if lock_s < 4.096:
        return 7
    if lock_s < 8.192:
        return 8
    if lock_s < 16.384:
        return 9
    if lock_s < 32.768:
        return 10
    if lock_s < 65.536:
        return 11
    if lock_s < 131.072:
        return 12
    if lock_s < 262.144:
        return 13
    if lock_s < 524.288:
        return 14
    return 15


def _msm_lock_ex(lock_s: float) -> int:
    lock_ms = int(lock_s * 1000.0)
    if lock_s < 0.0:
        return 0
    if lock_s < 0.064:
        return lock_ms
    if lock_s < 0.128:
        return (lock_ms + 64) // 2
    if lock_s < 0.256:
        return (lock_ms + 256) // 4
    if lock_s < 0.512:
        return (lock_ms + 768) // 8
    if lock_s < 1.024:
        return (lock_ms + 2048) // 16
    if lock_s < 2.048:
        return (lock_ms + 5120) // 32
    if lock_s < 4.096:
        return (lock_ms + 12288) // 64
    if lock_s < 8.192:
        return (lock_ms + 28672) // 128
    if lock_s < 16.384:
        return (lock_ms + 65536) // 256
    if lock_s < 32.768:
        return (lock_ms + 147456) // 512
    if lock_s < 65.536:
        return (lock_ms + 327680) // 1024
    if lock_s < 131.072:
        return (lock_ms + 720896) // 2048
    if lock_s < 262.144:
        return (lock_ms + 1572864) // 4096
    if lock_s < 524.288:
        return (lock_ms + 3407872) // 8192
    if lock_s < 1048.576:
        return (lock_ms + 7340032) // 16384
    if lock_s < 2097.152:
        return (lock_ms + 15728640) // 32768
    if lock_s < 4194.304:
        return (lock_ms + 33554432) // 65536
    if lock_s < 8388.608:
        return (lock_ms + 71303168) // 131072
    if lock_s < 16777.216:
        return (lock_ms + 150994944) // 262144
    if lock_s < 33554.432:
        return (lock_ms + 318767104) // 524288
    if lock_s < 67108.864:
        return (lock_ms + 671088640) // 1048576
    return 704


@dataclass(slots=True)
class _CellValue:
    sat_key: tuple[Constellation, int]
    signal_label: str
    pseudorange_m: float
    carrier_cycles: float
    doppler_hz: float
    cnr_dbhz: float
    half: int
    slip: bool
    lli: int
    glonass_fcn: int | None


class RtcmEncoder:
    def __init__(self, station_id: int) -> None:
        self._station_id = station_id
        self._msm_sequence = 0
        self._phase_offsets: dict[tuple[Constellation, int, str], float] = {}
        self._lock_starts: dict[tuple[Constellation, int, str], GNSSTime] = {}

    def encode(self, payload: ScheduledPayload) -> bytes:
        if isinstance(payload, MsmMessage):
            frames = self.encode_many(payload)
            if len(frames) != 1:
                raise UnsupportedMessageError("MSM payload split into multiple RTCM messages; use encode_many()")
            return frames[0]
        return self.encode_many(payload)[0]

    def current_msm_sequence(self) -> int:
        return self._msm_sequence

    def advance_msm_sequence(self) -> None:
        self._msm_sequence = (self._msm_sequence + 1) % 8

    def encode_many(
        self,
        payload: ScheduledPayload,
        *,
        final_observation_message: bool = True,
        sequence: int | None = None,
        advance_sequence: bool = True,
    ) -> list[bytes]:
        if isinstance(payload, StationMessage):
            return [self._frame(self._encode_station(payload))]
        if isinstance(payload, GlonassBiasMessage):
            return [self._frame(self._encode_1230(payload))]
        if isinstance(payload, EphemerisMessage):
            return [self._frame(self._encode_ephemeris(payload))]
        if isinstance(payload, MsmMessage):
            chunks = self._split_msm_payload(payload)
            msm_sequence = self._msm_sequence if sequence is None else sequence
            frames = []
            for index, chunk in enumerate(chunks):
                is_last_chunk = index == len(chunks) - 1
                more_observation_messages_follow = not (final_observation_message and is_last_chunk)
                frames.append(
                    self._frame(
                        self._encode_msm(
                            chunk,
                            multiple_message=more_observation_messages_follow,
                            sequence=msm_sequence,
                        )
                    )
                )
            if advance_sequence:
                self._msm_sequence = (msm_sequence + 1) % 8
            if len(chunks) > 1:
                LOGGER.info(
                    "split %s epoch %.3f into %d RTCM MSM messages to keep <= %d mask slots",
                    payload.system.value,
                    payload.epoch.time.gps_seconds,
                    len(chunks),
                    MSM_MAX_CELLS,
                )
            return frames
        raise UnsupportedMessageError(f"Unsupported payload type: {type(payload)!r}")

    def _split_msm_payload(self, payload: MsmMessage) -> list[MsmMessage]:
        ordered_satellites: list[tuple[SatelliteObservation, set[str]]] = []
        for satellite in sorted(payload.satellites, key=lambda item: item.prn):
            signal_labels = self._encodable_signal_labels(satellite)
            if not signal_labels:
                continue
            ordered_satellites.append((satellite, signal_labels))

        if not ordered_satellites:
            raise UnsupportedMessageError(f"No encodable observations for {payload.system}")

        chunks: list[list[SatelliteObservation]] = []
        current_chunk: list[SatelliteObservation] = []
        current_signals: set[str] = set()
        for satellite, signal_labels in ordered_satellites:
            next_signals = current_signals | signal_labels
            next_satellites = len(current_chunk) + 1
            if current_chunk and next_satellites * len(next_signals) > MSM_MAX_CELLS:
                chunks.append(current_chunk)
                current_chunk = []
                current_signals = set()
                next_signals = set(signal_labels)
                next_satellites = 1
            if next_satellites * len(next_signals) > MSM_MAX_CELLS:
                raise UnsupportedMessageError(
                    f"{payload.system.value} satellite {satellite.prn} exceeds MSM mask limit {MSM_MAX_CELLS}"
                )
            current_chunk.append(satellite)
            current_signals = next_signals
        if current_chunk:
            chunks.append(current_chunk)

        return [
            MsmMessage(
                message_type=payload.message_type,
                system=payload.system,
                msm_level=payload.msm_level,
                epoch=payload.epoch,
                satellites=chunk,
            )
            for chunk in chunks
        ]

    def _encodable_signal_labels(self, satellite: SatelliteObservation) -> set[str]:
        labels: set[str] = set()
        for signal in satellite.signals:
            if not _has_meaningful_observable(signal):
                continue
            label = _canonical_signal_label(satellite.system, signal.signal_label)
            try:
                signal_definition(satellite.system, label)
            except UnsupportedMessageError:
                continue
            labels.add(label)
        return labels

    def _encodable_cell_count(self, satellite: SatelliteObservation) -> int:
        return len(self._encodable_signal_labels(satellite))

    def _frame(self, body: BitBuffer) -> bytes:
        body.pad_to_byte()
        payload = body.to_bytes()
        length = len(payload)
        header = bytes([0xD3, (length >> 8) & 0x03, length & 0xFF])
        frame = header + payload
        crc = _crc24q(frame)
        return frame + bytes([(crc >> 16) & 0xFF, (crc >> 8) & 0xFF, crc & 0xFF])

    def _encode_station(self, payload: StationMessage) -> BitBuffer:
        station = payload.station
        body = BitBuffer()
        if payload.message_type == 1005:
            if station.ecef_xyz_m is None:
                raise UnsupportedMessageError("RTCM 1005 station message requires ECEF coordinates")
            body.append_unsigned(1005, 12)
            body.append_unsigned(station.station_id, 12)
            body.append_unsigned(0, 6)
            body.append_unsigned(1, 1)
            body.append_unsigned(1, 1)
            body.append_unsigned(1, 1)
            body.append_unsigned(0, 1)
            _append_signed_38(body, _round(station.ecef_xyz_m[0] / 0.0001))
            body.append_unsigned(1, 1)
            body.append_unsigned(0, 1)
            _append_signed_38(body, _round(station.ecef_xyz_m[1] / 0.0001))
            body.append_unsigned(0, 2)
            _append_signed_38(body, _round(station.ecef_xyz_m[2] / 0.0001))
            return body
        if payload.message_type == 1006:
            if station.ecef_xyz_m is None:
                raise UnsupportedMessageError("RTCM 1006 station message requires ECEF coordinates")
            height = _round(max(0.0, min(station.antenna_height_m, 6.5535)) / 0.0001)
            body.append_unsigned(1006, 12)
            body.append_unsigned(station.station_id, 12)
            body.append_unsigned(0, 6)
            body.append_unsigned(1, 1)
            body.append_unsigned(1, 1)
            body.append_unsigned(1, 1)
            body.append_unsigned(0, 1)
            _append_signed_38(body, _round(station.ecef_xyz_m[0] / 0.0001))
            body.append_unsigned(1, 1)
            body.append_unsigned(0, 1)
            _append_signed_38(body, _round(station.ecef_xyz_m[1] / 0.0001))
            body.append_unsigned(0, 2)
            _append_signed_38(body, _round(station.ecef_xyz_m[2] / 0.0001))
            body.append_unsigned(height, 16)
            return body
        if payload.message_type == 1007:
            body.append_unsigned(1007, 12)
            body.append_unsigned(station.station_id, 12)
            _append_string_field(body, _antenna_descriptor_text(station))
            body.append_unsigned(0, 8)
            return body
        if payload.message_type == 1008:
            body.append_unsigned(1008, 12)
            body.append_unsigned(station.station_id, 12)
            _append_string_field(body, _antenna_descriptor_text(station))
            body.append_unsigned(0, 8)
            _append_string_field(body, station.antenna_serial)
            return body
        if payload.message_type == 1033:
            body.append_unsigned(1033, 12)
            body.append_unsigned(station.station_id, 12)
            _append_string_field(body, _antenna_descriptor_text(station))
            body.append_unsigned(0, 8)
            _append_string_field(body, station.antenna_serial)
            _append_string_field(body, station.receiver_type)
            _append_string_field(body, station.receiver_version)
            _append_string_field(body, station.receiver_serial)
            return body
        raise UnsupportedMessageError(f"Unsupported station message: {payload.message_type}")

    def _encode_1230(self, payload: GlonassBiasMessage) -> BitBuffer:
        body = BitBuffer()
        body.append_unsigned(1230, 12)
        body.append_unsigned(self._station_id, 12)
        body.append_unsigned(0, 1)
        body.append_unsigned(0, 3)
        body.append_unsigned(0, 4)
        return body

    def _encode_ephemeris(self, payload: EphemerisMessage) -> BitBuffer:
        msg_type = payload.message_type
        eph = payload.ephemeris
        if msg_type == 1019 and isinstance(eph, KeplerEphemeris) and eph.system is Constellation.GPS:
            return self._encode_1019(eph)
        if msg_type == 1020 and isinstance(eph, GlonassEphemeris):
            return self._encode_1020(eph)
        if msg_type == 1042 and isinstance(eph, KeplerEphemeris) and eph.system is Constellation.BDS:
            return self._encode_1042(eph)
        if msg_type == 1044 and isinstance(eph, KeplerEphemeris) and eph.system is Constellation.QZS:
            return self._encode_1044(eph)
        if msg_type == 1045 and isinstance(eph, KeplerEphemeris) and eph.system is Constellation.GAL:
            return self._encode_1045(eph)
        if msg_type == 1046 and isinstance(eph, KeplerEphemeris) and eph.system is Constellation.GAL:
            return self._encode_1046(eph)
        if msg_type == 1041:
            raise UnsupportedMessageError("RTCM 1041 NavIC/IRNSS ephemeris is defined but not implemented yet")
        if msg_type == 1043:
            raise UnsupportedMessageError("RTCM 1043 is undefined in RTCM 3.3")
        raise UnsupportedMessageError(f"Unsupported ephemeris message: {msg_type}")

    def _encode_1019(self, eph: KeplerEphemeris) -> BitBuffer:
        week = eph.week % 1024
        toc = _round(eph.toc.gps_week_tow()[1] / 16.0)
        toe = _round(eph.toes / 16.0)
        body = BitBuffer()
        body.append_unsigned(1019, 12)
        body.append_unsigned(eph.prn, 6)
        body.append_unsigned(week, 10)
        body.append_unsigned(eph.sva, 4)
        body.append_unsigned(eph.code, 2)
        body.append_signed(_round(eph.idot / P2_43 / SC2RAD), 14)
        body.append_unsigned(eph.iode, 8)
        body.append_unsigned(toc, 16)
        body.append_signed(_round(eph.f2 / P2_55), 8)
        body.append_signed(_round(eph.f1 / P2_43), 16)
        body.append_signed(_round(eph.f0 / P2_31), 22)
        body.append_unsigned(eph.iodc, 10)
        body.append_signed(_round(eph.crs / P2_5), 16)
        body.append_signed(_round(eph.deln / P2_43 / SC2RAD), 16)
        body.append_signed(_round(eph.m0 / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.cuc / P2_29), 16)
        body.append_unsigned(_round_u(eph.e / P2_33), 32)
        body.append_signed(_round(eph.cus / P2_29), 16)
        body.append_unsigned(_round_u(eph.sqrt_a / P2_19), 32)
        body.append_unsigned(toe, 16)
        body.append_signed(_round(eph.cic / P2_29), 16)
        body.append_signed(_round(eph.omega0 / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.cis / P2_29), 16)
        body.append_signed(_round(eph.i0 / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.crc / P2_5), 16)
        body.append_signed(_round(eph.omega / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.omega_dot / P2_43 / SC2RAD), 24)
        body.append_signed(_round(eph.tgd[0] / P2_31), 8)
        body.append_unsigned(eph.svh, 6)
        body.append_unsigned(eph.flag, 1)
        body.append_unsigned(0 if eph.fit > 0.0 else 1, 1)
        return body

    def _encode_1020(self, eph: GlonassEphemeris) -> BitBuffer:
        tof_local = gpst_to_utc_datetime(eph.tof.gps_seconds) + timedelta(seconds=10800.0)
        toe_local = gpst_to_utc_datetime(eph.toe.gps_seconds) + timedelta(seconds=10800.0)
        tk_h = tof_local.hour
        tk_m = tof_local.minute
        tk_s = _round(tof_local.second / 30.0)
        tb = _round((toe_local.hour * 3600.0 + toe_local.minute * 60.0 + toe_local.second) / 900.0)
        nt = glonass_day_index(tof_local)
        pos = [_round(value / P2_11 / 1e3) for value in eph.position_m]
        vel = [_round(value / P2_20 / 1e3) for value in eph.velocity_mps]
        acc = [_round(value / P2_30 / 1e3) for value in eph.acceleration_mps2]
        body = BitBuffer()
        body.append_unsigned(1020, 12)
        body.append_unsigned(eph.prn, 6)
        body.append_unsigned(eph.frequency_channel + 7, 5)
        body.append_unsigned(0, 4)
        body.append_unsigned(tk_h, 5)
        body.append_unsigned(tk_m, 6)
        body.append_unsigned(tk_s, 1)
        body.append_unsigned(eph.svh, 1)
        body.append_unsigned(0, 1)
        body.append_unsigned(tb, 7)
        for index in range(3):
            body.append_sign_magnitude(vel[index], 24)
            body.append_sign_magnitude(pos[index], 27)
            body.append_sign_magnitude(acc[index], 5)
        body.append_unsigned(0, 1)
        body.append_sign_magnitude(_round(eph.gamn / P2_40), 11)
        body.append_unsigned(0, 3)
        body.append_sign_magnitude(_round(eph.taun / P2_30), 22)
        body.append_unsigned(_round(eph.dtaun / P2_30), 5)
        body.append_unsigned(eph.age, 5)
        body.append_unsigned(0, 1)
        body.append_unsigned(0, 4)
        body.append_unsigned(nt, 11)
        body.append_unsigned(0, 2)
        body.append_unsigned(0, 1)
        body.append_unsigned(0, 11)
        body.append_unsigned(0, 32)
        body.append_unsigned(0, 5)
        body.append_unsigned(0, 22)
        body.append_unsigned(0, 1)
        body.append_unsigned(0, 7)
        return body

    def _encode_1042(self, eph: KeplerEphemeris) -> BitBuffer:
        week = eph.week % 8192
        _, toc_bdt = eph.toc.bdt_week_tow()
        toe = _round(eph.toes / 8.0)
        toc = _round(toc_bdt / 8.0)
        body = BitBuffer()
        body.append_unsigned(1042, 12)
        body.append_unsigned(eph.prn, 6)
        body.append_unsigned(week, 13)
        body.append_unsigned(eph.sva, 4)
        body.append_signed(_round(eph.idot / P2_43 / SC2RAD), 14)
        body.append_unsigned(eph.iode, 5)
        body.append_unsigned(toc, 17)
        body.append_signed(_round(eph.f2 / P2_66), 11)
        body.append_signed(_round(eph.f1 / P2_50), 22)
        body.append_signed(_round(eph.f0 / P2_33), 24)
        body.append_unsigned(eph.iodc, 5)
        body.append_signed(_round(eph.crs / P2_6), 18)
        body.append_signed(_round(eph.deln / P2_43 / SC2RAD), 16)
        body.append_signed(_round(eph.m0 / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.cuc / P2_31), 18)
        body.append_unsigned(_round_u(eph.e / P2_33), 32)
        body.append_signed(_round(eph.cus / P2_31), 18)
        body.append_unsigned(_round_u(eph.sqrt_a / P2_19), 32)
        body.append_unsigned(toe, 17)
        body.append_signed(_round(eph.cic / P2_31), 18)
        body.append_signed(_round(eph.omega0 / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.cis / P2_31), 18)
        body.append_signed(_round(eph.i0 / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.crc / P2_6), 18)
        body.append_signed(_round(eph.omega / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.omega_dot / P2_43 / SC2RAD), 24)
        body.append_signed(_round(eph.tgd[0] / 1e-10), 10)
        body.append_signed(_round(eph.tgd[1] / 1e-10), 10)
        body.append_unsigned(eph.svh, 1)
        return body

    def _encode_1044(self, eph: KeplerEphemeris) -> BitBuffer:
        week = eph.week % 1024
        toc = _round(eph.toc.gps_week_tow()[1] / 16.0)
        toe = _round(eph.toes / 16.0)
        body = BitBuffer()
        body.append_unsigned(1044, 12)
        body.append_unsigned(_qzs_satellite_number(eph.prn), 4)
        body.append_unsigned(toc, 16)
        body.append_signed(_round(eph.f2 / P2_55), 8)
        body.append_signed(_round(eph.f1 / P2_43), 16)
        body.append_signed(_round(eph.f0 / P2_31), 22)
        body.append_unsigned(eph.iode, 8)
        body.append_signed(_round(eph.crs / P2_5), 16)
        body.append_signed(_round(eph.deln / P2_43 / SC2RAD), 16)
        body.append_signed(_round(eph.m0 / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.cuc / P2_29), 16)
        body.append_unsigned(_round_u(eph.e / P2_33), 32)
        body.append_signed(_round(eph.cus / P2_29), 16)
        body.append_unsigned(_round_u(eph.sqrt_a / P2_19), 32)
        body.append_unsigned(toe, 16)
        body.append_signed(_round(eph.cic / P2_29), 16)
        body.append_signed(_round(eph.omega0 / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.cis / P2_29), 16)
        body.append_signed(_round(eph.i0 / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.crc / P2_5), 16)
        body.append_signed(_round(eph.omega / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.omega_dot / P2_43 / SC2RAD), 24)
        body.append_signed(_round(eph.idot / P2_43 / SC2RAD), 14)
        body.append_unsigned(eph.code, 2)
        body.append_unsigned(week, 10)
        body.append_unsigned(eph.sva, 4)
        body.append_unsigned(eph.svh, 6)
        body.append_signed(_round(eph.tgd[0] / P2_31), 8)
        body.append_unsigned(eph.iodc, 10)
        body.append_unsigned(0 if eph.fit == 2.0 else 1, 1)
        return body

    def _encode_1045(self, eph: KeplerEphemeris) -> BitBuffer:
        week = (eph.week - 1024) % 4096
        toc = _round(eph.toc.gps_week_tow()[1] / 60.0)
        toe = _round(eph.toes / 60.0)
        body = BitBuffer()
        body.append_unsigned(1045, 12)
        body.append_unsigned(eph.prn, 6)
        body.append_unsigned(week, 12)
        body.append_unsigned(eph.iode, 10)
        body.append_unsigned(eph.sva, 8)
        body.append_signed(_round(eph.idot / P2_43 / SC2RAD), 14)
        body.append_unsigned(toc, 14)
        body.append_signed(_round(eph.f2 / P2_59), 6)
        body.append_signed(_round(eph.f1 / P2_46), 21)
        body.append_signed(_round(eph.f0 / P2_34), 31)
        body.append_signed(_round(eph.crs / P2_5), 16)
        body.append_signed(_round(eph.deln / P2_43 / SC2RAD), 16)
        body.append_signed(_round(eph.m0 / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.cuc / P2_29), 16)
        body.append_unsigned(_round_u(eph.e / P2_33), 32)
        body.append_signed(_round(eph.cus / P2_29), 16)
        body.append_unsigned(_round_u(eph.sqrt_a / P2_19), 32)
        body.append_unsigned(toe, 14)
        body.append_signed(_round(eph.cic / P2_29), 16)
        body.append_signed(_round(eph.omega0 / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.cis / P2_29), 16)
        body.append_signed(_round(eph.i0 / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.crc / P2_5), 16)
        body.append_signed(_round(eph.omega / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.omega_dot / P2_43 / SC2RAD), 24)
        body.append_signed(_round(eph.tgd[0] / P2_32), 10)
        body.append_unsigned((eph.svh >> 4) & 0x03, 2)
        body.append_unsigned((eph.svh >> 3) & 0x01, 1)
        body.append_unsigned(0, 7)
        return body

    def _encode_1046(self, eph: KeplerEphemeris) -> BitBuffer:
        week = (eph.week - 1024) % 4096
        toc = _round(eph.toc.gps_week_tow()[1] / 60.0)
        toe = _round(eph.toes / 60.0)
        body = BitBuffer()
        body.append_unsigned(1046, 12)
        body.append_unsigned(eph.prn, 6)
        body.append_unsigned(week, 12)
        body.append_unsigned(eph.iode, 10)
        body.append_unsigned(eph.sva, 8)
        body.append_signed(_round(eph.idot / P2_43 / SC2RAD), 14)
        body.append_unsigned(toc, 14)
        body.append_signed(_round(eph.f2 / P2_59), 6)
        body.append_signed(_round(eph.f1 / P2_46), 21)
        body.append_signed(_round(eph.f0 / P2_34), 31)
        body.append_signed(_round(eph.crs / P2_5), 16)
        body.append_signed(_round(eph.deln / P2_43 / SC2RAD), 16)
        body.append_signed(_round(eph.m0 / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.cuc / P2_29), 16)
        body.append_unsigned(_round_u(eph.e / P2_33), 32)
        body.append_signed(_round(eph.cus / P2_29), 16)
        body.append_unsigned(_round_u(eph.sqrt_a / P2_19), 32)
        body.append_unsigned(toe, 14)
        body.append_signed(_round(eph.cic / P2_29), 16)
        body.append_signed(_round(eph.omega0 / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.cis / P2_29), 16)
        body.append_signed(_round(eph.i0 / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.crc / P2_5), 16)
        body.append_signed(_round(eph.omega / P2_31 / SC2RAD), 32)
        body.append_signed(_round(eph.omega_dot / P2_43 / SC2RAD), 24)
        body.append_signed(_round(eph.tgd[0] / P2_32), 10)
        body.append_signed(_round(eph.tgd[1] / P2_32), 10)
        body.append_unsigned((eph.svh >> 7) & 0x03, 2)
        body.append_unsigned((eph.svh >> 6) & 0x01, 1)
        body.append_unsigned((eph.svh >> 1) & 0x03, 2)
        body.append_unsigned(eph.svh & 0x01, 1)
        return body

    def _encode_msm(
        self,
        payload: MsmMessage,
        *,
        multiple_message: bool = False,
        sequence: int | None = None,
    ) -> BitBuffer:
        sat_entries: list[tuple[int, dict[str, _CellValue], int | None]] = []
        candidate_cells: list[tuple[int, str, _CellValue]] = []
        for satellite in sorted(payload.satellites, key=lambda item: item.prn):
            sig_map: dict[str, _CellValue] = {}
            for signal in satellite.signals:
                if not _has_meaningful_observable(signal):
                    continue
                label = _canonical_signal_label(satellite.system, signal.signal_label)
                try:
                    signal_definition(satellite.system, label)
                except UnsupportedMessageError:
                    continue
                cell = _CellValue(
                    sat_key=(satellite.system, satellite.prn),
                    signal_label=label,
                    pseudorange_m=signal.pseudorange_m,
                    carrier_cycles=signal.carrier_cycles,
                    doppler_hz=signal.doppler_hz,
                    cnr_dbhz=signal.cnr_dbhz,
                    half=1 if signal.half_cycle_ambiguity or (signal.lli & 0x02) else 0,
                    slip=signal.slip_detected or bool(signal.lli & 0x01),
                    lli=signal.lli,
                    glonass_fcn=satellite.glonass_fcn,
                )
                sig_map[label] = cell
                candidate_cells.append((_msm_satellite_number(satellite.system, satellite.prn), label, cell))
            if sig_map:
                sat_entries.append((satellite.prn, sig_map, satellite.glonass_fcn))

        if not sat_entries:
            raise UnsupportedMessageError(f"No encodable observations for {payload.system}")

        candidate_cells.sort(key=lambda item: (item[0], signal_definition(payload.system, item[1]).msm_id))
        sat_ids = sorted({sat for sat, _, _ in candidate_cells})
        sig_labels = sorted(
            {label for _, label, _ in candidate_cells},
            key=lambda label: signal_definition(payload.system, label).msm_id,
        )
        if len(sat_ids) * len(sig_labels) > MSM_MAX_CELLS:
            raise UnsupportedMessageError(
                f"{payload.system.value} MSM payload requires {len(sat_ids)}x{len(sig_labels)} mask slots, exceeds {MSM_MAX_CELLS}"
            )
        sat_index = {sat_id: index for index, sat_id in enumerate(sat_ids)}
        sig_index = {label: index for index, label in enumerate(sig_labels)}
        cell_mask = [[False for _ in sig_labels] for _ in sat_ids]
        by_satellite: dict[int, dict[str, _CellValue]] = {sat_id: {} for sat_id in sat_ids}
        glonass_fcn_by_sat: dict[int, int | None] = {}
        for sat_id, label, cell in candidate_cells:
            cell_mask[sat_index[sat_id]][sig_index[label]] = True
            by_satellite[sat_id][label] = cell
            glonass_fcn_by_sat[sat_id] = cell.glonass_fcn

        rrng = [0.0 for _ in sat_ids]
        info = [0 for _ in sat_ids]
        rrate = [0.0 for _ in sat_ids]
        for sat_id in sat_ids:
            cells = list(by_satellite[sat_id].values())
            if not cells:
                continue
            reference = next((cell for cell in cells if cell.pseudorange_m != 0.0), cells[0])
            index = sat_index[sat_id]
            rrng[index] = _round(reference.pseudorange_m / RANGE_MS / P2_10) * RANGE_MS * P2_10
            if reference.doppler_hz != 0.0:
                lam = wavelength_m(payload.system, reference.signal_label, reference.glonass_fcn)
                rrate[index] = _round(-reference.doppler_hz * lam) * 1.0
            if payload.system is Constellation.GLO:
                fcn = glonass_fcn_by_sat.get(sat_id)
                info[index] = 15 if fcn is None else fcn + 7

        ncell = sum(1 for row in cell_mask for value in row if value)
        psrng = [0.0 for _ in range(ncell)]
        phrng = [0.0 for _ in range(ncell)]
        rate = [0.0 for _ in range(ncell)]
        lock = [0.0 for _ in range(ncell)]
        half = [0 for _ in range(ncell)]
        cnr = [0.0 for _ in range(ncell)]

        cell_counter = 0
        for sat_id in sat_ids:
            sat_cells = by_satellite[sat_id]
            for label in sig_labels:
                if label not in sat_cells:
                    continue
                cell = sat_cells[label]
                sat_idx = sat_index[sat_id]
                lam = wavelength_m(payload.system, label, cell.glonass_fcn)
                psrng_value = cell.pseudorange_m - rrng[sat_idx] if cell.pseudorange_m else 0.0
                phrng_raw = cell.carrier_cycles * lam - rrng[sat_idx] if cell.carrier_cycles and lam > 0.0 else 0.0
                rate_value = -cell.doppler_hz * lam - rrate[sat_idx] if cell.doppler_hz and lam > 0.0 else 0.0
                phase_key = (payload.system, sat_id, label)
                if cell.slip or phase_key not in self._phase_offsets or abs(phrng_raw - self._phase_offsets[phase_key]) > 1171.0:
                    self._phase_offsets[phase_key] = _round(phrng_raw / lam) * lam if lam > 0.0 else 0.0
                phrng_value = phrng_raw - self._phase_offsets.get(phase_key, 0.0)
                if cell.slip or phase_key not in self._lock_starts:
                    self._lock_starts[phase_key] = payload.epoch.time
                lock[cell_counter] = payload.epoch.time - self._lock_starts[phase_key]
                psrng[cell_counter] = psrng_value
                phrng[cell_counter] = phrng_value
                rate[cell_counter] = rate_value
                half[cell_counter] = cell.half
                cnr[cell_counter] = cell.cnr_dbhz
                cell_counter += 1

        epoch = self._msm_epoch(payload.epoch.time, payload.system)
        body = BitBuffer()
        body.append_unsigned(payload.message_type, 12)
        body.append_unsigned(self._station_id, 12)
        body.append_unsigned(epoch, 30)
        body.append_unsigned(1 if multiple_message else 0, 1)
        body.append_unsigned(self._msm_sequence if sequence is None else sequence, 3)
        body.append_unsigned(0, 7)
        body.append_unsigned(0, 2)
        body.append_unsigned(0, 2)
        body.append_unsigned(0, 1)
        body.append_unsigned(0, 3)
        for sat_id in range(1, 65):
            body.append_unsigned(1 if sat_id in sat_ids else 0, 1)
        signal_msm_ids = {signal_definition(payload.system, label).msm_id: label for label in sig_labels}
        for msm_id in range(1, 33):
            body.append_unsigned(1 if msm_id in signal_msm_ids else 0, 1)
        for sat_id in sat_ids:
            sat_idx = sat_index[sat_id]
            for label in sig_labels:
                body.append_unsigned(1 if cell_mask[sat_idx][sig_index[label]] else 0, 1)

        for value in rrng:
            int_ms = 255 if value > RANGE_MS * 255.0 else (_round_u(value / RANGE_MS / P2_10) >> 10)
            body.append_unsigned(int_ms, 8)
        if payload.msm_level in {5, 7}:
            for value in info:
                body.append_unsigned(value, 4)
        for value in rrng:
            mod_ms = 0 if value > RANGE_MS * 255.0 else (_round_u(value / RANGE_MS / P2_10) & 0x3FF)
            body.append_unsigned(mod_ms, 10)
        if payload.msm_level in {5, 7}:
            for value in rrate:
                rrate_val = -8192 if abs(value) > 8191.0 else _round(value)
                body.append_signed(rrate_val, 14)

        if payload.msm_level in {4, 5}:
            for value in psrng:
                psrng_val = -16384 if abs(value) > 292.7 else _round(value / RANGE_MS / P2_24)
                body.append_signed(psrng_val, 15)
            for value in phrng:
                phrng_val = -2097152 if abs(value) > 1171.0 else _round(value / RANGE_MS / P2_29)
                body.append_signed(phrng_val, 22)
            for value in lock:
                body.append_unsigned(_msm_lock(value), 4)
            for value in half:
                body.append_unsigned(value, 1)
            for value in cnr:
                body.append_unsigned(max(0, min(63, _round(value / 1.0))), 6)
            if payload.msm_level == 5:
                for value in rate:
                    rate_val = -16384 if abs(value) > 1.6384 else _round(value / 0.0001)
                    body.append_signed(rate_val, 15)
        elif payload.msm_level in {6, 7}:
            for value in psrng:
                psrng_val = -524288 if abs(value) > 292.7 else _round(value / RANGE_MS / P2_29)
                body.append_signed(psrng_val, 20)
            for value in phrng:
                phrng_val = -8388608 if abs(value) > 1171.0 else _round(value / RANGE_MS / P2_31)
                body.append_signed(phrng_val, 24)
            for value in lock:
                body.append_unsigned(_msm_lock_ex(value), 10)
            for value in half:
                body.append_unsigned(value, 1)
            for value in cnr:
                body.append_unsigned(max(0, min(1023, _round(value / 0.0625))), 10)
            if payload.msm_level == 7:
                for value in rate:
                    rate_val = -16384 if abs(value) > 1.6384 else _round(value / 0.0001)
                    body.append_signed(rate_val, 15)
        else:
            raise UnsupportedMessageError(f"Unsupported MSM level: {payload.msm_level}")
        return body

    def _msm_epoch(self, time: GNSSTime, system: Constellation) -> int:
        if system is Constellation.GLO:
            tow_utc = (gpst_to_utc_datetime(time.gps_seconds) + timedelta(seconds=10800.0))
            day_seconds = tow_utc.hour * 3600.0 + tow_utc.minute * 60.0 + tow_utc.second + tow_utc.microsecond * 1e-6
            dow = tow_utc.weekday() + 1
            if dow == 7:
                dow = 0
            return (dow << 27) + _round_u(day_seconds * 1e3)
        if system is Constellation.BDS:
            return _round_u(time.bdt_week_tow()[1] * 1e3)
        return _round_u(time.gps_week_tow()[1] * 1e3)
