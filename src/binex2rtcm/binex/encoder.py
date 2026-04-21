"""BINEX subset encoder.

The encoder intentionally targets the same practical subset the decoder already
supports:

- 0x00 site metadata
- 0x01-01..06 decoded navigation messages
- 0x7F-05 Trimble-style mixed observation epochs
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
import math
import struct

from ..errors import UnsupportedRecordError
from ..gnss_time import GPS_EPOCH, GNSSTime, glonass_day_index, gpst_to_utc_datetime
from ..model.ephemeris import GlonassEphemeris, KeplerEphemeris, SbasEphemeris
from ..model.observation import EpochObservations, SatelliteObservation, SignalObservation
from ..model.signals import Constellation, wavelength_m
from ..model.station import StationInfo
from ..stream_logging import current_gpst_calendar_datetime
from .framer import crc16_ccitt_zero, xor_checksum

GPS_URA_TABLE = [
    2.4,
    3.4,
    4.85,
    6.85,
    9.65,
    13.65,
    24.0,
    48.0,
    96.0,
    192.0,
    384.0,
    768.0,
    1536.0,
    3072.0,
    6144.0,
    0.0,
]
BINEX_SYSTEM_CODE = {
    Constellation.GPS: 0,
    Constellation.GLO: 1,
    Constellation.SBS: 2,
    Constellation.GAL: 3,
    Constellation.BDS: 4,
    Constellation.QZS: 5,
}
STRING_FIELD_IDS = {
    "metadata_format": 0x00,
    "site_name": 0x04,
    "site_identifier": 0x09,
    "marker_name": 0x0F,
    "antenna_descriptor": 0x17,
    "antenna_serial": 0x18,
    "receiver_type": 0x19,
    "receiver_serial": 0x1A,
    "receiver_version": 0x1B,
    "antenna_radome": 0x20,
}
# Use the official BINEX 0x7f-05 observation code IDs published by UNAVCO/GAGE.
# The decoder keeps a more permissive compatibility table, but encoding should
# emit the canonical IDs so downstream tools such as ringo interpret signals
# consistently.
OBS_CODE_INDEX: dict[Constellation, dict[str, int]] = {
    Constellation.GPS: {
        "1C": 1,
        "1P": 2,
        "1W": 3,
        "1Y": 4,
        "1M": 5,
        "1L": 6,
        "1N": 7,
        "1S": 8,
        "1X": 9,
        "2C": 11,
        "2D": 12,
        "2S": 13,
        "2L": 14,
        "2X": 15,
        "2P": 16,
        "2W": 17,
        "2Y": 18,
        "2M": 19,
        "2N": 20,
        "5I": 24,
        "5Q": 25,
        "5X": 26,
    },
    Constellation.GLO: {
        "1C": 1,
        "1P": 2,
        "3I": 14,
        "3Q": 15,
        "3X": 16,
        "2C": 11,
        "2P": 12,
    },
    Constellation.GAL: {
        "1A": 1,
        "1B": 2,
        "1C": 3,
        "1X": 4,
        "1Z": 5,
        "5I": 7,
        "5Q": 8,
        "5X": 9,
        "7I": 11,
        "7Q": 12,
        "7X": 13,
        "8I": 15,
        "8Q": 16,
        "8X": 17,
        "6A": 19,
        "6B": 20,
        "6C": 21,
        "6X": 22,
        "6Z": 23,
    },
    Constellation.SBS: {
        "1C": 1,
        "5I": 7,
        "5Q": 8,
        "5X": 9,
    },
    Constellation.BDS: {
        "2I": 1,
        "2Q": 2,
        "2X": 3,
        "7I": 5,
        "7Q": 6,
        "7X": 7,
        "6I": 9,
        "6Q": 10,
        "6X": 11,
        "1D": 13,
        "1P": 14,
        "1X": 15,
        "5D": 17,
        "5P": 18,
        "5X": 19,
        "7D": 23,
    },
    Constellation.QZS: {
        "1C": 1,
        "1S": 2,
        "1L": 3,
        "1X": 4,
        "2S": 8,
        "2L": 9,
        "2X": 10,
        "5I": 14,
        "5Q": 15,
        "5X": 16,
        "6S": 20,
        "6L": 21,
        "6X": 22,
        "1Z": 30,
    },
}


def _ubnxi(value: int) -> bytes:
    if value < 0:
        raise ValueError(f"ubinxi cannot encode negative value {value}")
    if value < 0x80:
        return bytes([value])
    if value < 0x4000:
        return bytes([0x80 | (value >> 7), value & 0x7F])
    if value < 0x200000:
        return bytes([0x80 | (value >> 14), 0x80 | ((value >> 7) & 0x7F), value & 0x7F])
    return bytes(
        [
            0x80 | ((value >> 22) & 0x7F),
            0x80 | ((value >> 15) & 0x7F),
            0x80 | ((value >> 8) & 0x7F),
            value & 0xFF,
        ]
    )


def _to_twos(value: int, bits: int) -> int:
    limit = 1 << bits
    if value < 0:
        value += limit
    return value & (limit - 1)


def _pack_sbits(value: int, bits: int) -> bytes:
    encoded = _to_twos(value, bits)
    size = (bits + 7) // 8
    return encoded.to_bytes(size, "big")


def _round_i(value: float) -> int:
    return int(round(value))


def _gps_seconds_from_datetime(when: datetime) -> float:
    return (when - GPS_EPOCH).total_seconds()


def _gpst_time_tag(when: datetime | None = None) -> tuple[int, int]:
    seconds = _gps_seconds_from_datetime(when or current_gpst_calendar_datetime())
    minutes = int(math.floor(seconds / 60.0))
    quarter_seconds = int(round((seconds - minutes * 60.0) * 4.0))
    if quarter_seconds >= 240:
        minutes += 1
        quarter_seconds = 0
    return minutes, quarter_seconds


def _epoch_tag(time: GNSSTime) -> tuple[int, int]:
    minutes = int(math.floor(time.gps_seconds / 60.0))
    milliseconds = int(round((time.gps_seconds - minutes * 60.0) * 1000.0))
    if milliseconds >= 60000:
        minutes += 1
        milliseconds = 0
    return minutes, milliseconds


def _pack_string(value: str) -> bytes:
    encoded = value.encode("ascii", errors="ignore")
    return _ubnxi(len(encoded)) + encoded


def _frame(record_id: int, payload: bytes) -> bytes:
    length = _ubnxi(len(payload))
    checksum_input = bytes([record_id]) + length + payload
    if len(checksum_input) < 128:
        checksum = bytes([xor_checksum(checksum_input)])
    else:
        checksum = crc16_ccitt_zero(checksum_input).to_bytes(2, "big")
    return bytes([0xE2, record_id]) + length + payload + checksum


def _ura_value(index: int) -> float:
    if 0 <= index < len(GPS_URA_TABLE):
        return GPS_URA_TABLE[index]
    return 0.0


def _bds_tgd_bits(value: float) -> int:
    scaled = _round_i(value / 1e-10)
    return _to_twos(scaled, 10)


def _obs_code_index(system: Constellation, label: str) -> int | None:
    return OBS_CODE_INDEX.get(system, {}).get(label)


def _signal_sort_key(system: Constellation, label: str) -> int:
    code_index = _obs_code_index(system, label)
    return 255 if code_index is None else code_index


def _glonass_flag_payload(fcn: int) -> int:
    return ((_to_twos(fcn, 4) & 0x0F) << 2) | 0x02


def _emit_extensions(flags0: int, glonass_fcn: int | None) -> bytes:
    payloads: list[int] = []
    if flags0:
        payloads.append(flags0)
    if glonass_fcn is not None:
        payloads.append(_glonass_flag_payload(glonass_fcn))
    if not payloads:
        return b""
    encoded = bytearray()
    for index, payload in enumerate(payloads):
        chain = 0x80 if index < len(payloads) - 1 else 0x00
        encoded.append(chain | (payload & 0x7F))
    return bytes(encoded)


def _quantized_cnr(signal: SignalObservation) -> int:
    if not math.isfinite(signal.cnr_dbhz):
        return 0
    return max(0, min(255, _round_i(signal.cnr_dbhz / 0.4)))


def _encode_first_range(range_m: float) -> bytes:
    coarse = int(math.floor(range_m / 0.064))
    fine = _round_i((range_m - coarse * 0.064) / 0.001)
    if fine >= 64:
        coarse += 1
        fine = 0
    value = ((coarse & 0xFFFFFFFF) << 6) | (fine & 0x3F)
    return value.to_bytes(5, "big")


def _encode_delta_range(delta_m: float, extended: bool) -> bytes:
    if extended:
        delta_mm = _round_i(delta_m / 0.001)
        return _pack_sbits(delta_mm, 20).rjust(3, b"\x00")
    delta_mm = _round_i(delta_m / 0.001)
    return struct.pack(">h", max(-32768, min(32767, delta_mm)))


def _encode_phase_delta(delta_m: float, acc: float, extended: bool) -> bytes:
    raw = _round_i(delta_m / acc)
    bits = 24 if extended else 22
    encoded = _pack_sbits(raw, bits)
    return encoded if extended else (int.from_bytes(encoded, "big") & 0x3FFFFF).to_bytes(3, "big")


def _encode_doppler(doppler_hz: float) -> bytes:
    raw = _round_i(doppler_hz * 256.0)
    return _pack_sbits(raw, 24)


def _encode_observation_signal(
    system: Constellation,
    signal: SignalObservation,
    range0_m: float,
    is_first: bool,
    glonass_fcn: int | None,
) -> bytes | None:
    code_index = _obs_code_index(system, signal.signal_label)
    if code_index is None:
        return None
    if not math.isfinite(signal.pseudorange_m):
        return None

    try:
        lam = wavelength_m(system, signal.signal_label, glonass_fcn)
    except Exception:
        return None

    range_m = signal.pseudorange_m
    phase_m = signal.carrier_cycles * lam if lam > 0.0 and math.isfinite(signal.carrier_cycles) else range_m
    range_delta = range_m - range0_m
    phase_delta = phase_m - range_m
    doppler = signal.doppler_hz if math.isfinite(signal.doppler_hz) else 0.0

    extended = (not is_first and abs(range_delta) > 32.767) or abs(phase_delta) > 41.94302
    if not is_first and abs(range_delta) > 524.287:
        return None
    if abs(phase_delta) > 167.77214:
        return None

    flags0 = 0x40 if extended else 0
    if abs(doppler) > 0.0:
        flags0 |= 0x04
    first = (0x80 if (flags0 or glonass_fcn is not None) else 0x00) | ((1 if signal.slip_detected or (signal.lli & 0x01) else 0) << 5) | code_index

    payload = bytearray([first])
    payload.extend(_emit_extensions(flags0, glonass_fcn))
    payload.append(_quantized_cnr(signal))

    if is_first:
        payload.extend(_encode_first_range(range_m))
    else:
        payload.extend(_encode_delta_range(range_delta, extended))

    payload.extend(_encode_phase_delta(phase_delta, 0.00002, extended))

    if flags0 & 0x04:
        payload.extend(_encode_doppler(doppler))

    return bytes(payload)


def _encode_satellite_observations(satellite: SatelliteObservation) -> bytes | None:
    system_code = BINEX_SYSTEM_CODE.get(satellite.system)
    if system_code is None:
        return None

    encodable = [
        signal
        for signal in satellite.signals
        if _obs_code_index(satellite.system, signal.signal_label) is not None and math.isfinite(signal.pseudorange_m)
    ]
    if satellite.system is Constellation.GLO:
        fallback_doppler = next(
            (
                signal.doppler_hz
                for signal in encodable
                if math.isfinite(signal.doppler_hz) and abs(signal.doppler_hz) > 1e-12
            ),
            None,
        )
        if fallback_doppler is not None:
            # Preserve a usable GLONASS Doppler when one signal is missing rate information.
            encodable = [
                signal
                if math.isfinite(signal.doppler_hz) and abs(signal.doppler_hz) > 1e-12
                else replace(signal, doppler_hz=fallback_doppler)
                for signal in encodable
            ]
    encodable.sort(key=lambda item: _signal_sort_key(satellite.system, item.signal_label))
    encodable = encodable[:7]
    if not encodable:
        return None

    range0 = encodable[0].pseudorange_m
    chunks: list[bytes] = []

    for index, signal in enumerate(encodable):
        chunk = _encode_observation_signal(
            satellite.system,
            signal,
            range0,
            is_first=index == 0,
            glonass_fcn=satellite.glonass_fcn,
        )
        if chunk is None:
            if index == 0:
                return None
            continue
        chunks.append(chunk)
    if not chunks:
        return None
    body = bytearray([satellite.prn, (len(chunks) << 4) | system_code])
    for chunk in chunks:
        body.extend(chunk)
    return bytes(body)


class BinexEncoder:
    """Encode normalized items into the project-supported BINEX subset."""

    def encode(self, item: object, logical_time: datetime | None = None) -> bytes:
        if isinstance(item, StationInfo):
            return _frame(0x00, self._encode_station(item, logical_time))
        if isinstance(item, KeplerEphemeris):
            return _frame(0x01, self._encode_kepler_ephemeris(item))
        if isinstance(item, GlonassEphemeris):
            return _frame(0x01, self._encode_glonass_ephemeris(item))
        if isinstance(item, SbasEphemeris):
            return _frame(0x01, self._encode_sbas_ephemeris(item))
        if isinstance(item, EpochObservations):
            return _frame(0x7F, self._encode_observations(item))
        raise UnsupportedRecordError(f"Unsupported BINEX emission item: {type(item)!r}")

    def _encode_station(self, station: StationInfo, logical_time: datetime | None) -> bytes:
        minutes, quarter_seconds = _gpst_time_tag(logical_time)
        payload = bytearray()
        payload.extend(struct.pack(">I", minutes))
        payload.append(quarter_seconds & 0xFF)
        payload.append(0)

        def add_string(field_name: str, value: str) -> None:
            text = value.strip()
            if not text:
                return
            payload.extend(_ubnxi(STRING_FIELD_IDS[field_name]))
            payload.extend(_pack_string(text))

        add_string("metadata_format", station.metadata_format)
        add_string("site_name", station.site_name)
        add_string("site_identifier", station.site_identifier)
        add_string("marker_name", station.marker_name)
        add_string("antenna_descriptor", station.antenna_descriptor)
        add_string("antenna_serial", station.antenna_serial)
        add_string("receiver_type", station.receiver_type)
        add_string("receiver_serial", station.receiver_serial)
        add_string("receiver_version", station.receiver_version)
        add_string("antenna_radome", station.antenna_radome)

        if station.ecef_xyz_m is not None:
            payload.extend(_ubnxi(0x1D))
            payload.extend(_ubnxi(0))
            payload.extend(struct.pack(">ddd", *station.ecef_xyz_m))

        if station.ecef_xyz_m is not None or abs(station.antenna_height_m) > 0.0:
            payload.extend(_ubnxi(0x1F))
            payload.extend(struct.pack(">ddd", station.antenna_height_m, 0.0, 0.0))
        return bytes(payload)

    def _encode_kepler_ephemeris(self, eph: KeplerEphemeris) -> bytes:
        if eph.system is Constellation.GPS:
            week, tow = eph.ttr.gps_week_tow()
            payload = bytearray([0x01, eph.prn - 1])
            payload.extend(struct.pack(">H", week))
            payload.extend(struct.pack(">i", _round_i(tow)))
            payload.extend(struct.pack(">i", _round_i(eph.toes)))
            payload.extend(struct.pack(">f", eph.tgd[0]))
            payload.extend(struct.pack(">i", eph.iodc))
            payload.extend(struct.pack(">f", eph.f2))
            payload.extend(struct.pack(">f", eph.f1))
            payload.extend(struct.pack(">f", eph.f0))
            payload.extend(struct.pack(">i", eph.iode))
            payload.extend(struct.pack(">f", eph.deln / math.pi))
            payload.extend(struct.pack(">d", eph.m0))
            payload.extend(struct.pack(">d", eph.e))
            payload.extend(struct.pack(">d", eph.sqrt_a))
            payload.extend(struct.pack(">f", eph.cic))
            payload.extend(struct.pack(">f", eph.crc))
            payload.extend(struct.pack(">f", eph.cis))
            payload.extend(struct.pack(">f", eph.crs))
            payload.extend(struct.pack(">f", eph.cuc))
            payload.extend(struct.pack(">f", eph.cus))
            payload.extend(struct.pack(">d", eph.omega0))
            payload.extend(struct.pack(">d", eph.omega))
            payload.extend(struct.pack(">d", eph.i0))
            payload.extend(struct.pack(">f", eph.omega_dot / math.pi))
            payload.extend(struct.pack(">f", eph.idot / math.pi))
            payload.extend(struct.pack(">f", _ura_value(eph.sva)))
            payload.extend(struct.pack(">H", eph.svh))
            flag = (int(eph.fit) & 0xFF) | ((eph.flag & 0x01) << 8) | ((eph.code & 0x03) << 9)
            payload.extend(struct.pack(">H", flag))
            return bytes(payload)

        if eph.system is Constellation.GAL:
            week, tow = eph.ttr.gps_week_tow()
            payload = bytearray([0x04, eph.prn - 1])
            payload.extend(struct.pack(">H", week))
            payload.extend(struct.pack(">i", _round_i(tow)))
            payload.extend(struct.pack(">i", _round_i(eph.toes)))
            payload.extend(struct.pack(">f", eph.tgd[0]))
            payload.extend(struct.pack(">f", eph.tgd[1]))
            payload.extend(struct.pack(">i", eph.iode))
            payload.extend(struct.pack(">f", eph.f2))
            payload.extend(struct.pack(">f", eph.f1))
            payload.extend(struct.pack(">f", eph.f0))
            payload.extend(struct.pack(">f", eph.deln / math.pi))
            payload.extend(struct.pack(">d", eph.m0))
            payload.extend(struct.pack(">d", eph.e))
            payload.extend(struct.pack(">d", eph.sqrt_a))
            payload.extend(struct.pack(">f", eph.cic))
            payload.extend(struct.pack(">f", eph.crc))
            payload.extend(struct.pack(">f", eph.cis))
            payload.extend(struct.pack(">f", eph.crs))
            payload.extend(struct.pack(">f", eph.cuc))
            payload.extend(struct.pack(">f", eph.cus))
            payload.extend(struct.pack(">d", eph.omega0))
            payload.extend(struct.pack(">d", eph.omega))
            payload.extend(struct.pack(">d", eph.i0))
            payload.extend(struct.pack(">f", eph.omega_dot / math.pi))
            payload.extend(struct.pack(">f", eph.idot / math.pi))
            payload.extend(struct.pack(">f", _ura_value(eph.sva)))
            payload.extend(struct.pack(">H", eph.svh))
            payload.extend(struct.pack(">H", eph.code))
            return bytes(payload)

        if eph.system is Constellation.BDS:
            week, tow = eph.ttr.bdt_week_tow()
            _, toc = eph.toc.bdt_week_tow()
            bdt_week, toes = eph.toe.bdt_week_tow()
            payload = bytearray([0x05, eph.prn])
            payload.extend(struct.pack(">H", bdt_week))
            payload.extend(struct.pack(">i", _round_i(tow)))
            payload.extend(struct.pack(">i", _round_i(toc)))
            payload.extend(struct.pack(">i", _round_i(toes)))
            payload.extend(struct.pack(">f", eph.f2))
            payload.extend(struct.pack(">f", eph.f1))
            payload.extend(struct.pack(">f", eph.f0))
            payload.extend(struct.pack(">f", eph.deln / math.pi))
            payload.extend(struct.pack(">d", eph.m0))
            payload.extend(struct.pack(">d", eph.e))
            payload.extend(struct.pack(">d", eph.sqrt_a))
            payload.extend(struct.pack(">f", eph.cic))
            payload.extend(struct.pack(">f", eph.crc))
            payload.extend(struct.pack(">f", eph.cis))
            payload.extend(struct.pack(">f", eph.crs))
            payload.extend(struct.pack(">f", eph.cuc))
            payload.extend(struct.pack(">f", eph.cus))
            payload.extend(struct.pack(">d", eph.omega0))
            payload.extend(struct.pack(">d", eph.omega))
            payload.extend(struct.pack(">d", eph.i0))
            payload.extend(struct.pack(">f", eph.omega_dot / math.pi))
            payload.extend(struct.pack(">f", eph.idot / math.pi))
            flag1 = ((eph.flag & 0x07) << 11) | ((eph.iode & 0x1F) << 6) | ((eph.iodc & 0x1F) << 1) | (eph.svh & 0x01)
            flag2 = (
                (eph.sva & 0x0F)
                | (_bds_tgd_bits(eph.tgd[0]) << 4)
                | (_bds_tgd_bits(eph.tgd[1]) << 14)
                | ((eph.code & 0x7F) << 25)
            )
            payload.extend(struct.pack(">H", flag1))
            payload.extend(struct.pack(">I", flag2))
            return bytes(payload)

        if eph.system is Constellation.QZS:
            week, tow = eph.ttr.gps_week_tow()
            payload = bytearray([0x06, eph.prn])
            payload.extend(struct.pack(">H", week))
            payload.extend(struct.pack(">i", _round_i(tow)))
            payload.extend(struct.pack(">i", _round_i(eph.toes)))
            payload.extend(struct.pack(">f", eph.tgd[0]))
            payload.extend(struct.pack(">i", eph.iodc))
            payload.extend(struct.pack(">f", eph.f2))
            payload.extend(struct.pack(">f", eph.f1))
            payload.extend(struct.pack(">f", eph.f0))
            payload.extend(struct.pack(">i", eph.iode))
            payload.extend(struct.pack(">f", eph.deln / math.pi))
            payload.extend(struct.pack(">d", eph.m0))
            payload.extend(struct.pack(">d", eph.e))
            payload.extend(struct.pack(">d", eph.sqrt_a))
            payload.extend(struct.pack(">f", eph.cic))
            payload.extend(struct.pack(">f", eph.crc))
            payload.extend(struct.pack(">f", eph.cis))
            payload.extend(struct.pack(">f", eph.crs))
            payload.extend(struct.pack(">f", eph.cuc))
            payload.extend(struct.pack(">f", eph.cus))
            payload.extend(struct.pack(">d", eph.omega0))
            payload.extend(struct.pack(">d", eph.omega))
            payload.extend(struct.pack(">d", eph.i0))
            payload.extend(struct.pack(">f", eph.omega_dot / math.pi))
            payload.extend(struct.pack(">f", eph.idot / math.pi))
            payload.extend(struct.pack(">f", _ura_value(eph.sva)))
            payload.extend(struct.pack(">H", eph.svh))
            payload.extend(struct.pack(">H", 1 if eph.fit == 0.0 else 0))
            return bytes(payload)

        raise UnsupportedRecordError(f"Unsupported BINEX kepler ephemeris: {eph.system}")

    def _encode_glonass_ephemeris(self, eph: GlonassEphemeris) -> bytes:
        toe_local = gpst_to_utc_datetime(eph.toe.gps_seconds)
        tof_local = gpst_to_utc_datetime(eph.tof.gps_seconds)
        # The decoded subset stores GLONASS times in local UTC+3 seconds of day.
        toe_moscow = toe_local + timedelta(seconds=10800.0)
        tof_moscow = tof_local + timedelta(seconds=10800.0)
        toe_sod = toe_moscow.hour * 3600.0 + toe_moscow.minute * 60.0 + toe_moscow.second + toe_moscow.microsecond * 1e-6
        tof_sod = tof_moscow.hour * 3600.0 + tof_moscow.minute * 60.0 + tof_moscow.second + tof_moscow.microsecond * 1e-6
        payload = bytearray([0x02, eph.prn - 1])
        payload.extend(struct.pack(">H", glonass_day_index(toe_moscow)))
        payload.extend(struct.pack(">I", _round_i(toe_sod)))
        payload.extend(struct.pack(">d", -eph.taun))
        payload.extend(struct.pack(">d", eph.gamn))
        payload.extend(struct.pack(">I", _round_i(tof_sod)))
        for value in eph.position_m:
            payload.extend(struct.pack(">d", value * 1e-3))
        for value in eph.velocity_mps:
            payload.extend(struct.pack(">d", value * 1e-3))
        for value in eph.acceleration_mps2:
            payload.extend(struct.pack(">d", value * 1e-3))
        payload.append(eph.svh & 0x01)
        payload.extend(struct.pack(">b", eph.frequency_channel))
        payload.append(eph.age & 0xFF)
        payload.append(18)
        payload.extend(struct.pack(">d", 0.0))
        payload.extend(struct.pack(">d", eph.dtaun))
        return bytes(payload)

    def _encode_sbas_ephemeris(self, eph: SbasEphemeris) -> bytes:
        week, tow = eph.toe.gps_week_tow()
        _, tof = eph.tof.gps_week_tow()
        payload = bytearray([0x03, eph.prn])
        payload.extend(struct.pack(">H", week))
        payload.extend(struct.pack(">I", _round_i(tow)))
        payload.extend(struct.pack(">d", eph.af0))
        payload.extend(struct.pack(">f", 0.0))
        payload.extend(struct.pack(">I", _round_i(tof)))
        for values in (eph.position_m, eph.velocity_mps, eph.acceleration_mps2):
            for value in values:
                payload.extend(struct.pack(">d", value * 1e-3))
        payload.append(eph.svh & 0xFF)
        payload.append(eph.sva & 0xFF)
        payload.append(0)
        return bytes(payload)

    def _encode_observations(self, epoch: EpochObservations) -> bytes:
        satellites = []
        for satellite in epoch.satellites:
            encoded = _encode_satellite_observations(satellite)
            if encoded is not None:
                satellites.append(encoded)
        if not satellites:
            raise UnsupportedRecordError("No encodable BINEX observations in epoch")

        minutes, milliseconds = _epoch_tag(epoch.time)
        payload = bytearray([0x05])
        payload.extend(struct.pack(">I", minutes))
        payload.extend(struct.pack(">H", milliseconds))
        payload.append((len(satellites) - 1) & 0x3F)
        for satellite in satellites:
            payload.extend(satellite)
        return bytes(payload)
