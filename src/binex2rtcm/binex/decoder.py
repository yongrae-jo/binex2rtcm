"""BINEX subset decoder.

The supported subset follows RTKLIB's BINEX support profile:
0x00 site metadata, 0x01-01..06 ephemerides, 0x7D-00 receiver state,
and 0x7F-05 Trimble NetR8 observations.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
import struct

from ..errors import ProtocolError, UnsupportedRecordError
from ..gnss_time import BDS_TO_GPS_SECONDS_OFFSET, GNSSTime, adjust_day, adjust_week, gps_from_week_tow
from ..model.ephemeris import GlonassEphemeris, KeplerEphemeris, SbasEphemeris
from ..model.observation import EpochObservations, SatelliteObservation, SignalObservation
from ..model.signals import Constellation, signal_definition, wavelength_m
from ..model.station import StationInfo
from .framer import BinexFrame, parse_binex_uint

SC2RAD = math.pi
WGS84_A = 6378137.0
WGS84_F = 1.0 / 298.257223563
WGS84_E2 = WGS84_F * (2.0 - WGS84_F)
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

def _official_obs_code_table(entries: dict[int, str]) -> list[str | None]:
    table: list[str | None] = [None] * 32
    for index, label in entries.items():
        table[index] = label
    return table


OBS_CODE_TABLE: dict[Constellation, list[str | None]] = {
    Constellation.GPS: _official_obs_code_table(
        {
            1: "1C",
            2: "1P",
            3: "1W",
            4: "1Y",
            5: "1M",
            6: "1L",
            7: "1N",
            8: "1S",
            9: "1X",
            11: "2C",
            12: "2D",
            13: "2S",
            14: "2L",
            15: "2X",
            16: "2P",
            17: "2W",
            18: "2Y",
            19: "2M",
            20: "2N",
            24: "5I",
            25: "5Q",
            26: "5X",
        }
    ),
    Constellation.GLO: _official_obs_code_table(
        {
            1: "1C",
            2: "1P",
            5: "4A",
            6: "4B",
            7: "4X",
            11: "2C",
            12: "2P",
            14: "3I",
            15: "3Q",
            16: "3X",
            20: "6A",
            21: "6B",
            22: "6X",
        }
    ),
    Constellation.GAL: _official_obs_code_table(
        {
            1: "1A",
            2: "1B",
            3: "1C",
            4: "1X",
            5: "1Z",
            7: "5I",
            8: "5Q",
            9: "5X",
            11: "7I",
            12: "7Q",
            13: "7X",
            15: "8I",
            16: "8Q",
            17: "8X",
            19: "6A",
            20: "6B",
            21: "6C",
            22: "6X",
            23: "6Z",
        }
    ),
    Constellation.SBS: _official_obs_code_table(
        {
            1: "1C",
            7: "5I",
            8: "5Q",
            9: "5X",
        }
    ),
    Constellation.BDS: _official_obs_code_table(
        {
            1: "2I",
            2: "2Q",
            3: "2X",
            5: "7I",
            6: "7Q",
            7: "7X",
            9: "6I",
            10: "6Q",
            11: "6X",
            13: "1D",
            14: "1P",
            15: "1X",
            17: "5D",
            18: "5P",
            19: "5X",
            20: "1S",
            21: "1L",
            22: "1Z",
            23: "7D",
            24: "7P",
            25: "7Z",
            26: "8D",
            27: "8P",
            28: "8X",
            29: "6D",
            30: "6P",
            31: "6Z",
        }
    ),
    Constellation.QZS: _official_obs_code_table(
        {
            1: "1C",
            2: "1S",
            3: "1L",
            4: "1X",
            5: "1E",
            8: "2S",
            9: "2L",
            10: "2X",
            14: "5I",
            15: "5Q",
            16: "5X",
            20: "6S",
            21: "6L",
            22: "6X",
            23: "6E",
            24: "6Z",
            30: "1Z",
            31: "1B",
        }
    ),
}


def getbitu(data: bytes, pos: int, length: int) -> int:
    value = 0
    for index in range(pos, pos + length):
        value = (value << 1) | ((data[index // 8] >> (7 - index % 8)) & 1)
    return value


def getbits(data: bytes, pos: int, length: int) -> int:
    value = getbitu(data, pos, length)
    if length <= 0 or not (value & (1 << (length - 1))):
        return value
    return value - (1 << length)


def ura_index(value: float) -> int:
    for index, candidate in enumerate(GPS_URA_TABLE[:-1]):
        if candidate >= value:
            return index
    return 15


def bds_tgd(raw_value: int) -> float:
    raw_value &= 0x3FF
    return -1e-10 * ((~raw_value) & 0x1FF) if raw_value & 0x200 else 1e-10 * (raw_value & 0x1FF)


def finite_or_zero(value: float) -> float:
    return value if math.isfinite(value) else 0.0


# Trimble live streams expose a few auxiliary strings around the standard
# site/receiver fields. Preserve them even when they do not map into RTCM.
SITE_METADATA_STRING_FIELDS = {
    0x00: "metadata_format",
    0x04: "site_name",
    0x08: "site_name",
    0x09: "site_identifier",
    0x0F: "marker_name",
    0x17: "antenna_descriptor",
    0x18: "antenna_serial",
    0x19: "receiver_type",
    0x1A: "receiver_serial",
    0x1B: "receiver_version",
    0x20: "antenna_radome",
}
SITE_METADATA_IGNORED_STRING_FIELDS = {
    0x01,  # software descriptor (string form)
}


@dataclass(slots=True)
class _Reader:
    data: bytes
    offset: int = 0

    def remaining(self) -> int:
        return len(self.data) - self.offset

    def u1(self) -> int:
        value = self.data[self.offset]
        self.offset += 1
        return value

    def i1(self) -> int:
        value = struct.unpack(">b", self.data[self.offset : self.offset + 1])[0]
        self.offset += 1
        return value

    def u2(self) -> int:
        value = struct.unpack(">H", self.data[self.offset : self.offset + 2])[0]
        self.offset += 2
        return value

    def u4(self) -> int:
        value = struct.unpack(">I", self.data[self.offset : self.offset + 4])[0]
        self.offset += 4
        return value

    def i4(self) -> int:
        value = struct.unpack(">i", self.data[self.offset : self.offset + 4])[0]
        self.offset += 4
        return value

    def r4(self) -> float:
        value = struct.unpack(">f", self.data[self.offset : self.offset + 4])[0]
        self.offset += 4
        return value

    def r8(self) -> float:
        value = struct.unpack(">d", self.data[self.offset : self.offset + 8])[0]
        self.offset += 8
        return value

    def take(self, count: int) -> bytes:
        value = self.data[self.offset : self.offset + count]
        self.offset += count
        return value


@dataclass(slots=True)
class _Raw7f05Observables:
    code_indexes: list[int]
    ranges: list[float]
    phases: list[float]
    cnrs: list[float]
    dopplers: list[float]
    slips: list[bool]
    glonass_fcn: int | None = None


class BinexDecoder:
    def __init__(self, station_id: int = 0) -> None:
        self._last_epoch_time: GNSSTime | None = None
        self._station_id = station_id
        self._station_meta: dict[str, object] = {}

    def decode(self, frame: BinexFrame) -> list[object]:
        if frame.record_id == 0x00:
            item = self._decode_site_metadata(frame.payload)
            return [item] if item is not None else []
        if frame.record_id == 0x01:
            item = self._decode_navigation(frame.payload)
            return [item] if item is not None else []
        if frame.record_id == 0x7D:
            self._decode_receiver_state(frame.payload)
            return []
        if frame.record_id == 0x7F:
            item = self._decode_prototyping(frame.payload)
            return [item] if item is not None else []
        raise UnsupportedRecordError(f"Unsupported BINEX record 0x{frame.record_id:02X}")

    def _read_ubnxi(self, reader: _Reader) -> int:
        value, size = parse_binex_uint(reader.data, reader.offset)
        reader.offset += size
        return value

    def _read_binex_string(self, reader: _Reader) -> str:
        length = self._read_ubnxi(reader)
        if length == 0:
            return ""
        return reader.take(length).decode("ascii", errors="replace").rstrip()

    def _llh_to_ecef(self, lon_deg: float, lat_deg: float, height_m: float) -> tuple[float, float, float]:
        lat = math.radians(lat_deg)
        lon = math.radians(lon_deg)
        sin_lat = math.sin(lat)
        cos_lat = math.cos(lat)
        sin_lon = math.sin(lon)
        cos_lon = math.cos(lon)
        n = WGS84_A / math.sqrt(1.0 - WGS84_E2 * sin_lat * sin_lat)
        x = (n + height_m) * cos_lat * cos_lon
        y = (n + height_m) * cos_lat * sin_lon
        z = (n * (1.0 - WGS84_E2) + height_m) * sin_lat
        return x, y, z

    def _build_station_info(self) -> StationInfo | None:
        ecef_xyz_m = self._station_meta.get("ecef_xyz_m")
        if not isinstance(ecef_xyz_m, tuple):
            return None
        return StationInfo(
            station_id=self._station_id,
            ecef_xyz_m=ecef_xyz_m,
            antenna_height_m=float(self._station_meta.get("antenna_height_m", 0.0)),
            antenna_descriptor=str(self._station_meta.get("antenna_descriptor", "")),
            antenna_radome=str(self._station_meta.get("antenna_radome", "")),
            antenna_serial=str(self._station_meta.get("antenna_serial", "")),
            receiver_type=str(self._station_meta.get("receiver_type", "")),
            receiver_version=str(self._station_meta.get("receiver_version", "")),
            receiver_serial=str(self._station_meta.get("receiver_serial", "")),
            marker_name=str(
                self._station_meta.get(
                    "marker_name",
                    self._station_meta.get("site_identifier", self._station_meta.get("site_name", "")),
                )
            ),
            site_name=str(self._station_meta.get("site_name", "")),
            site_identifier=str(self._station_meta.get("site_identifier", "")),
            metadata_format=str(self._station_meta.get("metadata_format", "")),
        )

    def _decode_site_metadata(self, payload: bytes) -> StationInfo | None:
        if len(payload) < 6:
            raise ProtocolError(f"BINEX 0x00 length error: {len(payload)}")
        reader = _Reader(payload)
        reader.u4()  # minutes since 1980-01-06
        quarter_seconds = reader.u1()
        if quarter_seconds >= 0xF0:
            raise ProtocolError(f"BINEX 0x00 invalid quarter-second tag: {quarter_seconds:#x}")
        reader.u1()  # metadata source id

        updates: dict[str, object] = {}
        while reader.remaining() > 0:
            field_id = self._read_ubnxi(reader)
            if field_id in SITE_METADATA_STRING_FIELDS:
                value = self._read_binex_string(reader)
                if value:
                    updates[SITE_METADATA_STRING_FIELDS[field_id]] = value
                continue
            if field_id in SITE_METADATA_IGNORED_STRING_FIELDS:
                self._read_binex_string(reader)
                continue
            if field_id == 0x1D:
                model_len = self._read_ubnxi(reader)
                if model_len:
                    reader.take(model_len)
                updates["ecef_xyz_m"] = (reader.r8(), reader.r8(), reader.r8())
                continue
            if field_id == 0x1E:
                model_len = self._read_ubnxi(reader)
                if model_len:
                    reader.take(model_len)
                lon_deg = reader.r8()
                lat_deg = reader.r8()
                height_m = reader.r8()
                updates["ecef_xyz_m"] = self._llh_to_ecef(lon_deg, lat_deg, height_m)
                continue
            if field_id == 0x1F:
                updates["antenna_height_m"] = reader.r8()
                reader.r8()  # east offset
                reader.r8()  # north offset
                continue
            # Vendor extensions do not always expose a typed length, so the
            # safest fallback is to keep already parsed metadata and ignore the
            # remainder of this record instead of dropping the whole update.
            break

        self._station_meta.update(updates)
        return self._build_station_info()

    def _decode_receiver_state(self, payload: bytes) -> None:
        if len(payload) < 8:
            raise ProtocolError(f"BINEX 0x7D length error: {len(payload)}")
        reader = _Reader(payload)
        subrecord = reader.u1()
        if subrecord != 0x00:
            raise UnsupportedRecordError(f"Unsupported BINEX receiver state subrecord 0x{subrecord:02X}")
        reader.take(6)  # time tag
        flags: list[int] = []
        while True:
            flag = reader.u1()
            flags.append(flag)
            if not (flag & 0x80):
                break
        bitmask = flags[0] & 0x7F
        if bitmask & 0x01:
            reader.i1()
        for mask in (0x02, 0x04, 0x08, 0x10):
            if bitmask & mask:
                reader.u2()

    def _decode_navigation(self, payload: bytes) -> object | None:
        subrecord = payload[0]
        body = payload[1:]
        if subrecord == 0x01:
            return self._decode_01_01(body)
        if subrecord == 0x02:
            return self._decode_01_02(body)
        if subrecord == 0x03:
            return self._decode_01_03(body)
        if subrecord == 0x04:
            return self._decode_01_04(body)
        if subrecord == 0x05:
            return self._decode_01_05(body)
        if subrecord == 0x06:
            return self._decode_01_06(body)
        if subrecord == 0x14:
            # Live Trimble BINEX streams can emit the upgraded Galileo record.
            # The project currently consumes the common decoded field layout.
            return self._decode_01_04(body)
        raise UnsupportedRecordError(f"Unsupported BINEX navigation subrecord 0x{subrecord:02X}")

    def _decode_prototyping(self, payload: bytes) -> object | None:
        reader = _Reader(payload)
        subrecord = reader.u1()
        gps_seconds = reader.u4() * 60.0 + reader.u2() * 0.001
        epoch_time = GNSSTime(gps_seconds)
        self._last_epoch_time = epoch_time
        if subrecord != 0x05:
            raise UnsupportedRecordError(f"Unsupported BINEX prototyping subrecord 0x{subrecord:02X}")
        return self._decode_7f_05(reader.take(reader.remaining()), epoch_time)

    def _decode_01_01(self, payload: bytes) -> KeplerEphemeris:
        if len(payload) < 127:
            raise ProtocolError(f"BINEX 0x01-01 length error: {len(payload)}")
        reader = _Reader(payload)
        prn = reader.u1() + 1
        week = reader.u2()
        tow = reader.i4()
        toes = float(reader.i4())
        tgd0 = reader.r4()
        iodc = reader.i4()
        f2 = reader.r4()
        f1 = reader.r4()
        f0 = reader.r4()
        iode = reader.i4()
        deln = reader.r4() * SC2RAD
        m0 = reader.r8()
        e = reader.r8()
        sqrt_a = reader.r8()
        cic = reader.r4()
        crc = reader.r4()
        cis = reader.r4()
        crs = reader.r4()
        cuc = reader.r4()
        cus = reader.r4()
        omega0 = reader.r8()
        omega = reader.r8()
        i0 = reader.r8()
        omega_dot = reader.r4() * SC2RAD
        idot = reader.r4() * SC2RAD
        ura = reader.r4() * 0.1
        svh = reader.u2()
        flag = reader.u2()
        toe = GNSSTime.from_gps_week_tow(week, toes)
        return KeplerEphemeris(
            system=Constellation.GPS,
            prn=prn,
            week=week,
            toes=toes,
            toe=toe,
            toc=toe,
            ttr=GNSSTime(adjust_week(toe.gps_seconds, tow)),
            iode=iode,
            iodc=iodc,
            f0=f0,
            f1=f1,
            f2=f2,
            deln=deln,
            m0=m0,
            e=e,
            sqrt_a=sqrt_a,
            cuc=cuc,
            cus=cus,
            crc=crc,
            crs=crs,
            cic=cic,
            cis=cis,
            omega0=omega0,
            omega=omega,
            i0=i0,
            omega_dot=omega_dot,
            idot=idot,
            sva=ura_index(ura),
            svh=svh,
            tgd=(tgd0, 0.0),
            code=(flag >> 9) & 0x03,
            flag=(flag >> 8) & 0x01,
            fit=float(flag & 0xFF),
        )

    def _decode_01_02(self, payload: bytes) -> GlonassEphemeris:
        if len(payload) < 119:
            raise ProtocolError(f"BINEX 0x01-02 length error: {len(payload)}")
        reader = _Reader(payload)
        prn = reader.u1() + 1
        _day = reader.u2()
        tod = float(reader.u4())
        taun = -reader.r8()
        gamn = reader.r8()
        tof = float(reader.u4())
        position = (reader.r8() * 1e3, reader.r8() * 1e3, reader.r8() * 1e3)
        velocity = (reader.r8() * 1e3, reader.r8() * 1e3, reader.r8() * 1e3)
        acceleration = (reader.r8() * 1e3, reader.r8() * 1e3, reader.r8() * 1e3)
        svh = reader.u1() & 0x01
        frq = reader.i1()
        age = reader.u1()
        _leap = reader.u1()
        _tau_gps = reader.r8()
        dtaun = reader.r8()
        # RTKLIB uses the latest stream time to resolve the daily rollover.
        anchor = self._last_epoch_time.gps_seconds if self._last_epoch_time is not None else gps_from_week_tow(0, 0.0)
        toe_gps = adjust_day(anchor, tod - 10800.0) + 18.0
        tof_gps = adjust_day(anchor, tof - 10800.0) + 18.0
        return GlonassEphemeris(
            system=Constellation.GLO,
            prn=prn,
            toe=GNSSTime(toe_gps),
            tof=GNSSTime(tof_gps),
            taun=taun,
            gamn=gamn,
            dtaun=dtaun,
            position_m=position,
            velocity_mps=velocity,
            acceleration_mps2=acceleration,
            svh=svh,
            frequency_channel=frq,
            age=age,
            iode=int((math.fmod(tod, 86400.0) / 900.0) + 0.5),
        )

    def _decode_01_03(self, payload: bytes) -> SbasEphemeris:
        if len(payload) < 98:
            raise ProtocolError(f"BINEX 0x01-03 length error: {len(payload)}")
        reader = _Reader(payload)
        prn = reader.u1()
        week = reader.u2()
        tow = float(reader.u4())
        af0 = reader.r8()
        _tod = reader.r4()
        tof = float(reader.u4())
        position = (reader.r8() * 1e3, reader.r8() * 1e3, reader.r8() * 1e3)
        velocity = (reader.r8() * 1e3, reader.r8() * 1e3, reader.r8() * 1e3)
        acceleration = (reader.r8() * 1e3, reader.r8() * 1e3, reader.r8() * 1e3)
        svh = reader.u1()
        sva = reader.u1()
        _iodn = reader.u1()
        t0 = GNSSTime.from_gps_week_tow(week, tow)
        return SbasEphemeris(
            system=Constellation.SBS,
            prn=prn,
            toe=t0,
            tof=GNSSTime(adjust_week(t0.gps_seconds, tof)),
            af0=af0,
            position_m=position,
            velocity_mps=velocity,
            acceleration_mps2=acceleration,
            svh=svh,
            sva=sva,
        )

    def _decode_01_04(self, payload: bytes) -> KeplerEphemeris:
        if len(payload) < 127:
            raise ProtocolError(f"BINEX 0x01-04 length error: {len(payload)}")
        reader = _Reader(payload)
        prn = reader.u1() + 1
        week = reader.u2()
        tow = reader.i4()
        toes = float(reader.i4())
        # Some live 0x14/0x04 Galileo records surface unavailable float fields
        # as NaN. Normalize them here so downstream RTCM/BINEX encoders do not
        # fail on otherwise usable ephemerides.
        bgd1 = finite_or_zero(reader.r4())
        bgd2 = finite_or_zero(reader.r4())
        iode = reader.i4()
        f2 = finite_or_zero(reader.r4())
        f1 = finite_or_zero(reader.r4())
        f0 = finite_or_zero(reader.r4())
        deln = finite_or_zero(reader.r4()) * SC2RAD
        m0 = finite_or_zero(reader.r8())
        e = finite_or_zero(reader.r8())
        sqrt_a = finite_or_zero(reader.r8())
        cic = finite_or_zero(reader.r4())
        crc = finite_or_zero(reader.r4())
        cis = finite_or_zero(reader.r4())
        crs = finite_or_zero(reader.r4())
        cuc = finite_or_zero(reader.r4())
        cus = finite_or_zero(reader.r4())
        omega0 = finite_or_zero(reader.r8())
        omega = finite_or_zero(reader.r8())
        i0 = finite_or_zero(reader.r8())
        omega_dot = finite_or_zero(reader.r4()) * SC2RAD
        idot = finite_or_zero(reader.r4()) * SC2RAD
        ura = finite_or_zero(reader.r4()) * 0.1
        svh = reader.u2()
        code = reader.u2()
        toe = GNSSTime.from_gps_week_tow(week, toes)
        return KeplerEphemeris(
            system=Constellation.GAL,
            prn=prn,
            week=week,
            toes=toes,
            toe=toe,
            toc=toe,
            ttr=GNSSTime(adjust_week(toe.gps_seconds, tow)),
            iode=iode,
            iodc=iode,
            f0=f0,
            f1=f1,
            f2=f2,
            deln=deln,
            m0=m0,
            e=e,
            sqrt_a=sqrt_a,
            cuc=cuc,
            cus=cus,
            crc=crc,
            crs=crs,
            cic=cic,
            cis=cis,
            omega0=omega0,
            omega=omega,
            i0=i0,
            omega_dot=omega_dot,
            idot=idot,
            sva=ura_index(ura),
            svh=svh,
            tgd=(bgd1, bgd2),
            code=code,
        )

    def _decode_01_05(self, payload: bytes) -> KeplerEphemeris:
        if len(payload) < 117:
            raise ProtocolError(f"BINEX 0x01-05 length error: {len(payload)}")
        reader = _Reader(payload)
        prn = reader.u1()
        week = reader.u2()
        tow = reader.i4()
        toc = float(reader.i4())
        toes = float(reader.i4())
        f2 = reader.r4()
        f1 = reader.r4()
        f0 = reader.r4()
        deln = reader.r4() * SC2RAD
        m0 = reader.r8()
        e = reader.r8()
        sqrt_a = reader.r8()
        cic = reader.r4()
        crc = reader.r4()
        cis = reader.r4()
        crs = reader.r4()
        cuc = reader.r4()
        cus = reader.r4()
        omega0 = reader.r8()
        omega = reader.r8()
        i0 = reader.r8()
        omega_dot = reader.r4() * SC2RAD
        idot = reader.r4() * SC2RAD
        flag1 = reader.u2()
        flag2 = reader.u4()
        toe_time = GNSSTime.from_bdt_week_tow(week, toes)
        toc_time = GNSSTime.from_bdt_week_tow(week, toc)
        return KeplerEphemeris(
            system=Constellation.BDS,
            prn=prn,
            week=week,
            toes=toes,
            toe=toe_time,
            toc=toc_time,
            ttr=GNSSTime(adjust_week(toe_time.gps_seconds, tow + BDS_TO_GPS_SECONDS_OFFSET)),
            iode=(flag1 >> 6) & 0x1F,
            iodc=(flag1 >> 1) & 0x1F,
            f0=f0,
            f1=f1,
            f2=f2,
            deln=deln,
            m0=m0,
            e=e,
            sqrt_a=sqrt_a,
            cuc=cuc,
            cus=cus,
            crc=crc,
            crs=crs,
            cic=cic,
            cis=cis,
            omega0=omega0,
            omega=omega,
            i0=i0,
            omega_dot=omega_dot,
            idot=idot,
            sva=flag2 & 0x0F,
            svh=flag1 & 0x01,
            tgd=(bds_tgd(flag2 >> 4), bds_tgd(flag2 >> 14)),
            code=(flag2 >> 25) & 0x7F,
            flag=(flag1 >> 11) & 0x07,
        )

    def _decode_01_06(self, payload: bytes) -> KeplerEphemeris:
        if len(payload) < 127:
            raise ProtocolError(f"BINEX 0x01-06 length error: {len(payload)}")
        reader = _Reader(payload)
        prn = reader.u1()
        week = reader.u2()
        tow = reader.i4()
        toes = float(reader.i4())
        tgd0 = reader.r4()
        iodc = reader.i4()
        f2 = reader.r4()
        f1 = reader.r4()
        f0 = reader.r4()
        iode = reader.i4()
        deln = reader.r4() * SC2RAD
        m0 = reader.r8()
        e = reader.r8()
        sqrt_a = reader.r8()
        cic = reader.r4()
        crc = reader.r4()
        cis = reader.r4()
        crs = reader.r4()
        cuc = reader.r4()
        cus = reader.r4()
        omega0 = reader.r8()
        omega = reader.r8()
        i0 = reader.r8()
        omega_dot = reader.r4() * SC2RAD
        idot = reader.r4() * SC2RAD
        ura = reader.r4() * 0.1
        svh = reader.u2()
        flag = reader.u2()
        toe = GNSSTime.from_gps_week_tow(week, toes)
        return KeplerEphemeris(
            system=Constellation.QZS,
            prn=prn,
            week=week,
            toes=toes,
            toe=toe,
            toc=toe,
            ttr=GNSSTime(adjust_week(toe.gps_seconds, tow)),
            iode=iode,
            iodc=iodc,
            f0=f0,
            f1=f1,
            f2=f2,
            deln=deln,
            m0=m0,
            e=e,
            sqrt_a=sqrt_a,
            cuc=cuc,
            cus=cus,
            crc=crc,
            crs=crs,
            cic=cic,
            cis=cis,
            omega0=omega0,
            omega=omega,
            i0=i0,
            omega_dot=omega_dot,
            idot=idot,
            sva=ura_index(ura),
            svh=svh,
            tgd=(tgd0, 0.0),
            code=2,
            fit=0.0 if (flag & 0x01) else 2.0,
        )

    def _decode_7f_05(self, payload: bytes, epoch_time: GNSSTime) -> EpochObservations:
        reader = _Reader(payload)
        flag = reader.u1()
        nsat = (flag & 0x3F) + 1
        receiver_clock_offset_s = None
        if flag & 0x80:
            chunk = reader.take(3)
            receiver_clock_offset_s = getbits(chunk, 2, 22) * 1e-9
        if flag & 0x40:
            systems = getbitu(bytes([reader.data[reader.offset]]), 0, 4)
            reader.offset += 1
            for _ in range(systems):
                reader.offset += 4
        satellites: list[SatelliteObservation] = []
        for _ in range(nsat):
            prn = reader.u1()
            descriptor = reader.u1()
            nobs = (descriptor >> 4) & 0x07
            sys_code = descriptor & 0x0F
            raw = self._read_7f_05_observables(reader, nobs)
            system = {
                0: Constellation.GPS,
                1: Constellation.GLO,
                2: Constellation.SBS,
                3: Constellation.GAL,
                4: Constellation.BDS,
                5: Constellation.QZS,
            }.get(sys_code)
            if system is None:
                continue
            satellite = self._decode_7f_05_satellite(system, prn, raw)
            if satellite is not None and satellite.signals:
                satellites.append(satellite)
        return EpochObservations(time=epoch_time, satellites=satellites, receiver_clock_offset_s=receiver_clock_offset_s)

    def _read_7f_05_observables(self, reader: _Reader, nobs: int) -> _Raw7f05Observables:
        raw_ranges: list[float] = []
        raw_phases: list[float] = []
        raw_cnrs: list[float] = []
        raw_dopplers: list[float] = []
        raw_code_indexes: list[int] = []
        raw_slips: list[bool] = []
        glonass_fcn: int | None = None
        range0 = 0.0

        for index in range(nobs):
            first = reader.u1()
            flag_chain = (first >> 7) & 0x01
            slip = (first >> 5) & 0x01
            code_index = first & 0x1F
            flags = [0, 0, 0, 0]
            while flag_chain:
                extension = reader.u1()
                flags[extension & 0x03] = extension & 0x7F
                flag_chain = 1 if extension & 0x80 else 0
            if flags[2]:
                glonass_fcn = getbits(bytes([flags[2]]), 2, 4)
            acc = 0.0001 if (flags[0] & 0x20) else 0.00002
            cnr = reader.u1() * 0.4

            if index == 0:
                block = reader.take(5)
                cnr += getbits(block, 0, 2) * 0.1
                range_value = getbitu(block, 2, 32) * 0.064 + getbitu(block, 34, 6) * 0.001
                range0 = range_value
            elif flags[0] & 0x40:
                block = reader.take(3)
                cnr += getbits(block, 0, 2) * 0.1
                range_value = range0 + getbits(block, 4, 20) * 0.001
            else:
                block = reader.take(2)
                range_value = range0 + getbits(block, 0, 16) * 0.001

            if flags[0] & 0x40:
                block = reader.take(3)
                phase_value = range_value + getbits(block, 0, 24) * acc
            else:
                block = reader.take(3)
                cnr += getbits(block, 0, 2) * 0.1
                phase_value = range_value + getbits(block, 2, 22) * acc

            doppler = 0.0
            if flags[0] & 0x04:
                doppler = getbits(reader.take(3), 0, 24) / 256.0
            if flags[0] & 0x08:
                if flags[0] & 0x10:
                    reader.u2()
                else:
                    reader.u1()

            raw_ranges.append(range_value)
            raw_phases.append(phase_value)
            raw_cnrs.append(cnr)
            raw_dopplers.append(doppler)
            raw_code_indexes.append(code_index)
            raw_slips.append(bool(slip))

        return _Raw7f05Observables(
            code_indexes=raw_code_indexes,
            ranges=raw_ranges,
            phases=raw_phases,
            cnrs=raw_cnrs,
            dopplers=raw_dopplers,
            slips=raw_slips,
            glonass_fcn=glonass_fcn,
        )

    def _decode_7f_05_satellite(
        self,
        system: Constellation,
        prn: int,
        raw: _Raw7f05Observables,
    ) -> SatelliteObservation | None:
        code_table = OBS_CODE_TABLE.get(system)
        if code_table is None:
            return None
        signals: list[SignalObservation] = []
        for index, code_index in enumerate(raw.code_indexes):
            label = code_table[code_index] if code_index < len(code_table) else None
            if label is None:
                continue
            try:
                definition = signal_definition(system, label)
            except Exception:
                continue
            try:
                lam = wavelength_m(system, definition.label, raw.glonass_fcn)
            except Exception:
                continue
            carrier_cycles = raw.phases[index] / lam if lam > 0.0 else 0.0
            signals.append(
                SignalObservation(
                    signal_label=definition.label,
                    pseudorange_m=raw.ranges[index],
                    carrier_cycles=carrier_cycles,
                    doppler_hz=raw.dopplers[index],
                    cnr_dbhz=raw.cnrs[index],
                    frequency_slot=definition.slot,
                    lock_time_s=0.0,
                    half_cycle_ambiguity=False,
                    slip_detected=raw.slips[index],
                    lli=1 if raw.slips[index] else 0,
                )
            )
        return SatelliteObservation(system=system, prn=prn, signals=signals, glonass_fcn=raw.glonass_fcn)
