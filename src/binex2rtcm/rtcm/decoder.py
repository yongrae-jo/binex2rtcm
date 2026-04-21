"""Internal RTCM decoder for project-generated messages."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
import math

from ..errors import ProtocolError, UnsupportedMessageError
from ..gnss_time import (
    BDS_TO_GPS_SECONDS_OFFSET,
    GPS_EPOCH,
    GNSSTime,
    SECONDS_PER_WEEK,
    adjust_week,
    utc_to_gpst_seconds,
)
from ..model.ephemeris import (
    Ephemeris,
    GALILEO_FNAV_DATA_SOURCE,
    GALILEO_INAV_DATA_SOURCE,
    GlonassEphemeris,
    KeplerEphemeris,
)
from ..model.observation import EpochObservations, SatelliteObservation, SignalObservation
from ..model.signals import CLIGHT, Constellation, SIGNAL_MAP, signal_definition, wavelength_m
from ..model.station import StationInfo

SC2RAD = math.pi
RANGE_MS = CLIGHT * 0.001
RANGE_MS_GLO = CLIGHT * 0.002
LEGACY_SIGNAL_PRIORITY = 1
MSM_SIGNAL_PRIORITY = 2
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

MSM_SYSTEM_BY_BASE = {
    1070: Constellation.GPS,
    1080: Constellation.GLO,
    1090: Constellation.GAL,
    1100: Constellation.SBS,
    1110: Constellation.QZS,
    1120: Constellation.BDS,
    1130: Constellation.IRN,
}
MSM_LEVELS = {4, 5, 6, 7}


@dataclass(slots=True)
class _BitReader:
    data: bytes
    bit_offset: int = 0

    def unsigned(self, bits: int) -> int:
        value = 0
        for _ in range(bits):
            byte_index = self.bit_offset // 8
            bit_index = 7 - (self.bit_offset % 8)
            value = (value << 1) | ((self.data[byte_index] >> bit_index) & 0x01)
            self.bit_offset += 1
        return value

    def signed(self, bits: int) -> int:
        value = self.unsigned(bits)
        if value & (1 << (bits - 1)):
            value -= 1 << bits
        return value

    def sign_magnitude(self, bits: int) -> int:
        sign = self.unsigned(1)
        magnitude = self.unsigned(bits - 1)
        return -magnitude if sign else magnitude

    def ascii(self, count: int) -> str:
        return "".join(chr(self.unsigned(8)) for _ in range(count))


def _reference_time() -> GNSSTime:
    return GNSSTime(utc_to_gpst_seconds(datetime.now(UTC)))


def _expand_week(raw_week: int, modulo: int, reference_week: int | None) -> int:
    if reference_week is None:
        return raw_week
    week = reference_week - (reference_week % modulo) + raw_week
    if week - reference_week > modulo // 2:
        week -= modulo
    elif week - reference_week < -(modulo // 2):
        week += modulo
    return week


def _split_antenna_descriptor(text: str) -> tuple[str, str]:
    if len(text) >= 20:
        return text[:16].rstrip(), text[16:20].strip()
    return text.rstrip(), ""


def _system_from_msm_message(message_type: int) -> tuple[Constellation, int]:
    base = (message_type // 10) * 10
    level = message_type % 10
    if level not in MSM_LEVELS or base not in MSM_SYSTEM_BY_BASE:
        raise UnsupportedMessageError(f"Unsupported MSM message: {message_type}")
    return MSM_SYSTEM_BY_BASE[base], level


def _msm_id_to_label(system: Constellation) -> dict[int, str]:
    mapping: dict[int, str] = {}
    for label, definition in SIGNAL_MAP[system].items():
        mapping[definition.msm_id] = label
    return mapping


class RtcmDecoder:
    def __init__(self, reference_time: GNSSTime | None = None, station_id: int = 0) -> None:
        self._reference_time = reference_time or _reference_time()
        self._station_id = station_id
        self._station_meta: dict[str, object] = {}
        self._lock_values: dict[tuple[str, Constellation, int, str], int] = {}
        self._carrier_phases: dict[tuple[str, Constellation, int, str], float] = {}
        self._last_epoch_time: dict[Constellation, GNSSTime] = {}

    def decode(self, frame: bytes) -> list[object]:
        if len(frame) < 6 or frame[0] != 0xD3:
            raise ProtocolError("RTCM frame header error")
        length = ((frame[1] & 0x03) << 8) | frame[2]
        payload = frame[3 : 3 + length]
        reader = _BitReader(payload)
        message_type = reader.unsigned(12)
        if message_type == 1005:
            item = self._decode_1005(reader)
            return [item] if item is not None else []
        if message_type == 1006:
            item = self._decode_1006(reader)
            return [item] if item is not None else []
        if message_type == 1007:
            item = self._decode_1007(reader)
            return [item] if item is not None else []
        if message_type == 1008:
            item = self._decode_1008(reader)
            return [item] if item is not None else []
        if message_type == 1033:
            item = self._decode_1033(reader)
            return [item] if item is not None else []
        if message_type == 1019:
            return [self._decode_1019(reader)]
        if message_type == 1020:
            return [self._decode_1020(reader)]
        if message_type == 1042:
            return [self._decode_1042(reader)]
        if message_type == 1044:
            return [self._decode_1044(reader)]
        if message_type == 1045:
            return [self._decode_1045(reader)]
        if message_type == 1046:
            return [self._decode_1046(reader)]
        if message_type == 1004:
            return [self._decode_1004(reader)]
        if message_type == 1012:
            return [self._decode_1012(reader)]
        if message_type == 1230:
            return []
        if 1071 <= message_type <= 1137:
            return [self._decode_msm(message_type, reader)]
        raise UnsupportedMessageError(f"Unsupported RTCM message {message_type}")

    def _build_station_info(self) -> StationInfo | None:
        ecef_xyz_m = self._station_meta.get("ecef_xyz_m")
        if not isinstance(ecef_xyz_m, tuple):
            ecef_xyz_m = None
        station_id = int(self._station_meta.get("station_id", self._station_id))
        site_identifier = str(self._station_meta.get("site_identifier", ""))
        if not site_identifier and station_id > 0:
            site_identifier = f"{station_id:04d}"
        station = StationInfo(
            station_id=station_id,
            ecef_xyz_m=ecef_xyz_m,
            antenna_height_m=float(self._station_meta.get("antenna_height_m", 0.0)),
            antenna_descriptor=str(self._station_meta.get("antenna_descriptor", "")),
            antenna_radome=str(self._station_meta.get("antenna_radome", "")),
            antenna_serial=str(self._station_meta.get("antenna_serial", "")),
            receiver_type=str(self._station_meta.get("receiver_type", "")),
            receiver_version=str(self._station_meta.get("receiver_version", "")),
            receiver_serial=str(self._station_meta.get("receiver_serial", "")),
            marker_name=str(self._station_meta.get("marker_name", "")),
            site_name=str(self._station_meta.get("site_name", "")),
            site_identifier=site_identifier,
            metadata_format=str(self._station_meta.get("metadata_format", "")),
        )
        return station if station.has_any_metadata() else None

    def _set_station_id(self, station_id: int) -> None:
        self._station_id = station_id
        self._station_meta["station_id"] = station_id

    def _decode_1005(self, reader: _BitReader) -> StationInfo | None:
        station_id = reader.unsigned(12)
        self._set_station_id(station_id)
        reader.unsigned(6)
        reader.unsigned(4)
        x = reader.signed(38) * 0.0001
        reader.unsigned(2)
        y = reader.signed(38) * 0.0001
        reader.unsigned(2)
        z = reader.signed(38) * 0.0001
        self._station_meta["ecef_xyz_m"] = (x, y, z)
        self._station_meta.setdefault("antenna_height_m", 0.0)
        return self._build_station_info()

    def _decode_1006(self, reader: _BitReader) -> StationInfo | None:
        station_id = reader.unsigned(12)
        self._set_station_id(station_id)
        reader.unsigned(6)
        reader.unsigned(4)
        x = reader.signed(38) * 0.0001
        reader.unsigned(2)
        y = reader.signed(38) * 0.0001
        reader.unsigned(2)
        z = reader.signed(38) * 0.0001
        height = reader.unsigned(16) * 0.0001
        self._station_meta["ecef_xyz_m"] = (x, y, z)
        self._station_meta["antenna_height_m"] = height
        return self._build_station_info()

    def _decode_1007(self, reader: _BitReader) -> StationInfo | None:
        station_id = reader.unsigned(12)
        self._set_station_id(station_id)
        descriptor = reader.ascii(reader.unsigned(8))
        reader.unsigned(8)
        antenna_descriptor, antenna_radome = _split_antenna_descriptor(descriptor)
        self._station_meta["antenna_descriptor"] = antenna_descriptor
        self._station_meta["antenna_radome"] = antenna_radome
        return self._build_station_info()

    def _decode_1008(self, reader: _BitReader) -> StationInfo | None:
        station_id = reader.unsigned(12)
        self._set_station_id(station_id)
        descriptor = reader.ascii(reader.unsigned(8))
        reader.unsigned(8)
        serial = reader.ascii(reader.unsigned(8))
        antenna_descriptor, antenna_radome = _split_antenna_descriptor(descriptor)
        self._station_meta["antenna_descriptor"] = antenna_descriptor
        self._station_meta["antenna_radome"] = antenna_radome
        self._station_meta["antenna_serial"] = serial.rstrip()
        return self._build_station_info()

    def _decode_1033(self, reader: _BitReader) -> StationInfo | None:
        station_id = reader.unsigned(12)
        self._set_station_id(station_id)
        descriptor = reader.ascii(reader.unsigned(8))
        reader.unsigned(8)
        antenna_serial = reader.ascii(reader.unsigned(8))
        receiver_type = reader.ascii(reader.unsigned(8))
        receiver_version = reader.ascii(reader.unsigned(8))
        receiver_serial = reader.ascii(reader.unsigned(8))
        antenna_descriptor, antenna_radome = _split_antenna_descriptor(descriptor)
        self._station_meta["antenna_descriptor"] = antenna_descriptor
        self._station_meta["antenna_radome"] = antenna_radome
        self._station_meta["antenna_serial"] = antenna_serial.rstrip()
        self._station_meta["receiver_type"] = receiver_type.rstrip()
        self._station_meta["receiver_version"] = receiver_version.rstrip()
        self._station_meta["receiver_serial"] = receiver_serial.rstrip()
        return self._build_station_info()

    def _reference_week(self) -> int | None:
        return self._reference_time.gps_week_tow()[0] if self._reference_time else None

    def _decode_legacy_gps_epoch(self, tow_ms: int) -> GNSSTime:
        reference = self._last_epoch_time.get(Constellation.GPS, self._reference_time)
        tow = tow_ms * 0.001
        return GNSSTime(adjust_week(reference.gps_seconds, tow))

    def _decode_legacy_glonass_epoch(self, tod_ms: int) -> GNSSTime:
        reference = self._last_epoch_time.get(Constellation.GLO, self._reference_time)
        tod = tod_ms * 0.001
        ref_local = reference.datetime_utc + timedelta(hours=3)
        tod_prev = (
            ref_local.hour * 3600.0
            + ref_local.minute * 60.0
            + ref_local.second
            + ref_local.microsecond * 1e-6
        )
        if tod < tod_prev - 43200.0:
            tod += 86400.0
        elif tod > tod_prev + 43200.0:
            tod -= 86400.0
        local_midnight = datetime.combine(ref_local.date(), time(0, 0), tzinfo=UTC)
        local_time = local_midnight + timedelta(seconds=tod)
        return GNSSTime(utc_to_gpst_seconds(local_time - timedelta(hours=3)))

    def _update_epoch_reference(self, system: Constellation, epoch_time: GNSSTime) -> None:
        self._last_epoch_time[system] = epoch_time
        self._reference_time = epoch_time

    def _legacy_phase_cycles(
        self,
        source_family: str,
        system: Constellation,
        prn: int,
        label: str,
        phase_range_m: int,
        wavelength: float,
    ) -> float:
        if wavelength <= 0.0:
            return 0.0
        cycles = phase_range_m * 0.0005 / wavelength
        phase_key = (source_family, system, prn, label)
        previous_cycles = self._carrier_phases.get(phase_key)
        if previous_cycles is not None:
            if cycles < previous_cycles - 750.0:
                cycles += 1500.0
            elif cycles > previous_cycles + 750.0:
                cycles -= 1500.0
        self._carrier_phases[phase_key] = cycles
        return cycles

    def _legacy_slip_detected(
        self,
        source_family: str,
        system: Constellation,
        prn: int,
        label: str,
        lock: int,
    ) -> bool:
        phase_key = (source_family, system, prn, label)
        previous_lock = self._lock_values.get(phase_key)
        slip = previous_lock is not None and lock < previous_lock
        self._lock_values[phase_key] = lock
        return slip

    def _gps_legacy_l1_label(self, code_indicator: int) -> str:
        return "1P" if code_indicator else "1C"

    def _gps_legacy_l2_label(self, code_indicator: int) -> str:
        return {0: "2X", 1: "2P", 2: "2D", 3: "2W"}[code_indicator]

    def _glonass_legacy_l1_label(self, code_indicator: int) -> str:
        return "1P" if code_indicator else "1C"

    def _glonass_legacy_l2_label(self, code_indicator: int) -> str:
        return "2P" if code_indicator else "2C"

    def _decode_1004(self, reader: _BitReader) -> EpochObservations:
        reader.unsigned(12)
        tow_ms = reader.unsigned(30)
        reader.unsigned(1)
        satellite_count = reader.unsigned(5)
        reader.unsigned(1)
        reader.unsigned(3)

        epoch_time = self._decode_legacy_gps_epoch(tow_ms)
        satellites: list[SatelliteObservation] = []
        for _ in range(satellite_count):
            prn = reader.unsigned(6)
            code1 = reader.unsigned(1)
            l1_pseudorange_raw = reader.unsigned(24)
            l1_phase_range = reader.signed(20)
            l1_lock = reader.unsigned(7)
            ambiguity = reader.unsigned(8)
            l1_cnr = reader.unsigned(8)
            code2 = reader.unsigned(2)
            l2_pseudorange_delta = reader.signed(14)
            l2_phase_range = reader.signed(20)
            l2_lock = reader.unsigned(7)
            l2_cnr = reader.unsigned(8)

            if not (1 <= prn <= 32):
                continue

            l1_label = self._gps_legacy_l1_label(code1)
            l2_label = self._gps_legacy_l2_label(code2)
            l1_wavelength = wavelength_m(Constellation.GPS, l1_label)
            l2_wavelength = wavelength_m(Constellation.GPS, l2_label)
            l1_pseudorange = l1_pseudorange_raw * 0.02 + ambiguity * RANGE_MS
            signals: list[SignalObservation] = []

            l1_carrier = 0.0
            if l1_phase_range != -524288:
                l1_carrier = (
                    l1_pseudorange / l1_wavelength
                    + self._legacy_phase_cycles(
                        "legacy",
                        Constellation.GPS,
                        prn,
                        l1_label,
                        l1_phase_range,
                        l1_wavelength,
                    )
                )
            l1_slip = self._legacy_slip_detected("legacy", Constellation.GPS, prn, l1_label, l1_lock)
            signals.append(
                SignalObservation(
                    signal_label=l1_label,
                    pseudorange_m=l1_pseudorange,
                    carrier_cycles=l1_carrier,
                    doppler_hz=0.0,
                    cnr_dbhz=l1_cnr * 0.25,
                    frequency_slot=signal_definition(Constellation.GPS, l1_label).slot,
                    source_priority=LEGACY_SIGNAL_PRIORITY,
                    slip_detected=l1_slip,
                    lli=1 if l1_slip else 0,
                )
            )

            l2_pseudorange = 0.0
            if l2_pseudorange_delta != -8192:
                l2_pseudorange = l1_pseudorange + l2_pseudorange_delta * 0.02

            l2_carrier = 0.0
            if l2_phase_range != -524288:
                l2_carrier = (
                    l1_pseudorange / l2_wavelength
                    + self._legacy_phase_cycles(
                        "legacy",
                        Constellation.GPS,
                        prn,
                        l2_label,
                        l2_phase_range,
                        l2_wavelength,
                    )
                )
            l2_slip = self._legacy_slip_detected("legacy", Constellation.GPS, prn, l2_label, l2_lock)
            signals.append(
                SignalObservation(
                    signal_label=l2_label,
                    pseudorange_m=l2_pseudorange,
                    carrier_cycles=l2_carrier,
                    doppler_hz=0.0,
                    cnr_dbhz=l2_cnr * 0.25,
                    frequency_slot=signal_definition(Constellation.GPS, l2_label).slot,
                    source_priority=LEGACY_SIGNAL_PRIORITY,
                    slip_detected=l2_slip,
                    lli=1 if l2_slip else 0,
                )
            )

            satellites.append(SatelliteObservation(system=Constellation.GPS, prn=prn, signals=signals))

        epoch = EpochObservations(time=epoch_time, satellites=satellites)
        self._update_epoch_reference(Constellation.GPS, epoch_time)
        return epoch

    def _decode_1012(self, reader: _BitReader) -> EpochObservations:
        reader.unsigned(12)
        tod_ms = reader.unsigned(27)
        reader.unsigned(1)
        satellite_count = reader.unsigned(5)
        reader.unsigned(1)
        reader.unsigned(3)

        epoch_time = self._decode_legacy_glonass_epoch(tod_ms)
        satellites: list[SatelliteObservation] = []
        for _ in range(satellite_count):
            prn = reader.unsigned(6)
            code1 = reader.unsigned(1)
            frequency_channel_raw = reader.unsigned(5)
            l1_pseudorange_raw = reader.unsigned(25)
            l1_phase_range = reader.signed(20)
            l1_lock = reader.unsigned(7)
            ambiguity = reader.unsigned(7)
            l1_cnr = reader.unsigned(8)
            code2 = reader.unsigned(2)
            l2_pseudorange_delta = reader.signed(14)
            l2_phase_range = reader.signed(20)
            l2_lock = reader.unsigned(7)
            l2_cnr = reader.unsigned(8)

            if not (1 <= prn <= 24):
                continue

            glonass_fcn = frequency_channel_raw - 7
            l1_label = self._glonass_legacy_l1_label(code1)
            l2_label = self._glonass_legacy_l2_label(code2)
            l1_wavelength = wavelength_m(Constellation.GLO, l1_label, glonass_fcn)
            l2_wavelength = wavelength_m(Constellation.GLO, l2_label, glonass_fcn)
            l1_pseudorange = l1_pseudorange_raw * 0.02 + ambiguity * RANGE_MS_GLO
            signals: list[SignalObservation] = []

            l1_carrier = 0.0
            if l1_phase_range != -524288:
                l1_carrier = (
                    l1_pseudorange / l1_wavelength
                    + self._legacy_phase_cycles(
                        "legacy",
                        Constellation.GLO,
                        prn,
                        l1_label,
                        l1_phase_range,
                        l1_wavelength,
                    )
                )
            l1_slip = self._legacy_slip_detected("legacy", Constellation.GLO, prn, l1_label, l1_lock)
            signals.append(
                SignalObservation(
                    signal_label=l1_label,
                    pseudorange_m=l1_pseudorange,
                    carrier_cycles=l1_carrier,
                    doppler_hz=0.0,
                    cnr_dbhz=l1_cnr * 0.25,
                    frequency_slot=signal_definition(Constellation.GLO, l1_label).slot,
                    source_priority=LEGACY_SIGNAL_PRIORITY,
                    slip_detected=l1_slip,
                    lli=1 if l1_slip else 0,
                )
            )

            l2_pseudorange = 0.0
            if l2_pseudorange_delta != -8192:
                l2_pseudorange = l1_pseudorange + l2_pseudorange_delta * 0.02

            l2_carrier = 0.0
            if l2_phase_range != -524288:
                l2_carrier = (
                    l1_pseudorange / l2_wavelength
                    + self._legacy_phase_cycles(
                        "legacy",
                        Constellation.GLO,
                        prn,
                        l2_label,
                        l2_phase_range,
                        l2_wavelength,
                    )
                )
            l2_slip = self._legacy_slip_detected("legacy", Constellation.GLO, prn, l2_label, l2_lock)
            signals.append(
                SignalObservation(
                    signal_label=l2_label,
                    pseudorange_m=l2_pseudorange,
                    carrier_cycles=l2_carrier,
                    doppler_hz=0.0,
                    cnr_dbhz=l2_cnr * 0.25,
                    frequency_slot=signal_definition(Constellation.GLO, l2_label).slot,
                    source_priority=LEGACY_SIGNAL_PRIORITY,
                    slip_detected=l2_slip,
                    lli=1 if l2_slip else 0,
                )
            )

            satellites.append(
                SatelliteObservation(
                    system=Constellation.GLO,
                    prn=prn,
                    signals=signals,
                    glonass_fcn=glonass_fcn,
                )
            )

        epoch = EpochObservations(time=epoch_time, satellites=satellites)
        self._update_epoch_reference(Constellation.GLO, epoch_time)
        return epoch

    def _decode_1019(self, reader: _BitReader) -> KeplerEphemeris:
        prn = reader.unsigned(6)
        week = _expand_week(reader.unsigned(10), 1024, self._reference_week())
        sva = reader.unsigned(4)
        code = reader.unsigned(2)
        idot = reader.signed(14) * P2_43 * SC2RAD
        iode = reader.unsigned(8)
        toc = reader.unsigned(16) * 16.0
        f2 = reader.signed(8) * P2_55
        f1 = reader.signed(16) * P2_43
        f0 = reader.signed(22) * P2_31
        iodc = reader.unsigned(10)
        crs = reader.signed(16) * P2_5
        deln = reader.signed(16) * P2_43 * SC2RAD
        m0 = reader.signed(32) * P2_31 * SC2RAD
        cuc = reader.signed(16) * P2_29
        e = reader.unsigned(32) * P2_33
        cus = reader.signed(16) * P2_29
        sqrt_a = reader.unsigned(32) * P2_19
        toes = reader.unsigned(16) * 16.0
        cic = reader.signed(16) * P2_29
        omega0 = reader.signed(32) * P2_31 * SC2RAD
        cis = reader.signed(16) * P2_29
        i0 = reader.signed(32) * P2_31 * SC2RAD
        crc = reader.signed(16) * P2_5
        omega = reader.signed(32) * P2_31 * SC2RAD
        omega_dot = reader.signed(24) * P2_43 * SC2RAD
        tgd = reader.signed(8) * P2_31
        svh = reader.unsigned(6)
        flag = reader.unsigned(1)
        fit = 4.0 if reader.unsigned(1) == 0 else 0.0
        toc_time = GNSSTime.from_gps_week_tow(week, toc)
        toe_time = GNSSTime.from_gps_week_tow(week, toes)
        eph = KeplerEphemeris(
            system=Constellation.GPS,
            prn=prn,
            toe=toe_time,
            week=week,
            toes=toes,
            toc=toc_time,
            ttr=toc_time,
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
            sva=sva,
            svh=svh,
            tgd=(tgd, 0.0),
            code=code,
            flag=flag,
            fit=fit,
        )
        self._reference_time = toc_time
        return eph

    def _decode_1042(self, reader: _BitReader) -> KeplerEphemeris:
        prn = reader.unsigned(6)
        ref_week = self._reference_time.bdt_week_tow()[0]
        week = _expand_week(reader.unsigned(13), 8192, ref_week)
        sva = reader.unsigned(4)
        idot = reader.signed(14) * P2_43 * SC2RAD
        iode = reader.unsigned(5)
        toc = reader.unsigned(17) * 8.0
        f2 = reader.signed(11) * P2_66
        f1 = reader.signed(22) * P2_50
        f0 = reader.signed(24) * P2_33
        iodc = reader.unsigned(5)
        crs = reader.signed(18) * P2_6
        deln = reader.signed(16) * P2_43 * SC2RAD
        m0 = reader.signed(32) * P2_31 * SC2RAD
        cuc = reader.signed(18) * P2_31
        e = reader.unsigned(32) * P2_33
        cus = reader.signed(18) * P2_31
        sqrt_a = reader.unsigned(32) * P2_19
        toes = reader.unsigned(17) * 8.0
        cic = reader.signed(18) * P2_31
        omega0 = reader.signed(32) * P2_31 * SC2RAD
        cis = reader.signed(18) * P2_31
        i0 = reader.signed(32) * P2_31 * SC2RAD
        crc = reader.signed(18) * P2_6
        omega = reader.signed(32) * P2_31 * SC2RAD
        omega_dot = reader.signed(24) * P2_43 * SC2RAD
        tgd1 = reader.signed(10) * 1e-10
        tgd2 = reader.signed(10) * 1e-10
        svh = reader.unsigned(1)
        toc_time = GNSSTime.from_bdt_week_tow(week, toc)
        toe_time = GNSSTime.from_bdt_week_tow(week, toes)
        eph = KeplerEphemeris(
            system=Constellation.BDS,
            prn=prn,
            toe=toe_time,
            week=week,
            toes=toes,
            toc=toc_time,
            ttr=toc_time,
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
            sva=sva,
            svh=svh,
            tgd=(tgd1, tgd2),
        )
        self._reference_time = toc_time
        return eph

    def _decode_1044(self, reader: _BitReader) -> KeplerEphemeris:
        prn = reader.unsigned(4)
        toc = reader.unsigned(16) * 16.0
        f2 = reader.signed(8) * P2_55
        f1 = reader.signed(16) * P2_43
        f0 = reader.signed(22) * P2_31
        iode = reader.unsigned(8)
        crs = reader.signed(16) * P2_5
        deln = reader.signed(16) * P2_43 * SC2RAD
        m0 = reader.signed(32) * P2_31 * SC2RAD
        cuc = reader.signed(16) * P2_29
        e = reader.unsigned(32) * P2_33
        cus = reader.signed(16) * P2_29
        sqrt_a = reader.unsigned(32) * P2_19
        toes = reader.unsigned(16) * 16.0
        cic = reader.signed(16) * P2_29
        omega0 = reader.signed(32) * P2_31 * SC2RAD
        cis = reader.signed(16) * P2_29
        i0 = reader.signed(32) * P2_31 * SC2RAD
        crc = reader.signed(16) * P2_5
        omega = reader.signed(32) * P2_31 * SC2RAD
        omega_dot = reader.signed(24) * P2_43 * SC2RAD
        idot = reader.signed(14) * P2_43 * SC2RAD
        code = reader.unsigned(2)
        week = _expand_week(reader.unsigned(10), 1024, self._reference_week())
        sva = reader.unsigned(4)
        svh = reader.unsigned(6)
        tgd = reader.signed(8) * P2_31
        iodc = reader.unsigned(10)
        fit = 2.0 if reader.unsigned(1) == 0 else 0.0
        toc_time = GNSSTime.from_gps_week_tow(week, toc)
        toe_time = GNSSTime.from_gps_week_tow(week, toes)
        eph = KeplerEphemeris(
            system=Constellation.QZS,
            prn=prn,
            toe=toe_time,
            week=week,
            toes=toes,
            toc=toc_time,
            ttr=toc_time,
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
            sva=sva,
            svh=svh,
            tgd=(tgd, 0.0),
            code=code,
            fit=fit,
        )
        self._reference_time = toc_time
        return eph

    def _decode_1045(self, reader: _BitReader) -> KeplerEphemeris:
        prn = reader.unsigned(6)
        reference_week = self._reference_week()
        adjusted_reference = None if reference_week is None else max(0, reference_week - 1024)
        week = _expand_week(reader.unsigned(12), 4096, adjusted_reference) + 1024
        iode = reader.unsigned(10)
        sva = reader.unsigned(8)
        idot = reader.signed(14) * P2_43 * SC2RAD
        toc = reader.unsigned(14) * 60.0
        f2 = reader.signed(6) * P2_59
        f1 = reader.signed(21) * P2_46
        f0 = reader.signed(31) * P2_34
        crs = reader.signed(16) * P2_5
        deln = reader.signed(16) * P2_43 * SC2RAD
        m0 = reader.signed(32) * P2_31 * SC2RAD
        cuc = reader.signed(16) * P2_29
        e = reader.unsigned(32) * P2_33
        cus = reader.signed(16) * P2_29
        sqrt_a = reader.unsigned(32) * P2_19
        toes = reader.unsigned(14) * 60.0
        cic = reader.signed(16) * P2_29
        omega0 = reader.signed(32) * P2_31 * SC2RAD
        cis = reader.signed(16) * P2_29
        i0 = reader.signed(32) * P2_31 * SC2RAD
        crc = reader.signed(16) * P2_5
        omega = reader.signed(32) * P2_31 * SC2RAD
        omega_dot = reader.signed(24) * P2_43 * SC2RAD
        tgd = reader.signed(10) * P2_32
        svh = (reader.unsigned(2) << 4) | (reader.unsigned(1) << 3)
        reader.unsigned(7)
        toc_time = GNSSTime.from_gps_week_tow(week, toc)
        toe_time = GNSSTime.from_gps_week_tow(week, toes)
        eph = KeplerEphemeris(
            system=Constellation.GAL,
            prn=prn,
            toe=toe_time,
            week=week,
            toes=toes,
            toc=toc_time,
            ttr=toc_time,
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
            sva=sva,
            svh=svh,
            tgd=(tgd, 0.0),
            code=GALILEO_FNAV_DATA_SOURCE,
        )
        self._reference_time = toc_time
        return eph

    def _decode_1046(self, reader: _BitReader) -> KeplerEphemeris:
        prn = reader.unsigned(6)
        reference_week = self._reference_week()
        adjusted_reference = None if reference_week is None else max(0, reference_week - 1024)
        week = _expand_week(reader.unsigned(12), 4096, adjusted_reference) + 1024
        iode = reader.unsigned(10)
        sva = reader.unsigned(8)
        idot = reader.signed(14) * P2_43 * SC2RAD
        toc = reader.unsigned(14) * 60.0
        f2 = reader.signed(6) * P2_59
        f1 = reader.signed(21) * P2_46
        f0 = reader.signed(31) * P2_34
        crs = reader.signed(16) * P2_5
        deln = reader.signed(16) * P2_43 * SC2RAD
        m0 = reader.signed(32) * P2_31 * SC2RAD
        cuc = reader.signed(16) * P2_29
        e = reader.unsigned(32) * P2_33
        cus = reader.signed(16) * P2_29
        sqrt_a = reader.unsigned(32) * P2_19
        toes = reader.unsigned(14) * 60.0
        cic = reader.signed(16) * P2_29
        omega0 = reader.signed(32) * P2_31 * SC2RAD
        cis = reader.signed(16) * P2_29
        i0 = reader.signed(32) * P2_31 * SC2RAD
        crc = reader.signed(16) * P2_5
        omega = reader.signed(32) * P2_31 * SC2RAD
        omega_dot = reader.signed(24) * P2_43 * SC2RAD
        tgd1 = reader.signed(10) * P2_32
        tgd2 = reader.signed(10) * P2_32
        e5b_hs = reader.unsigned(2)
        e5b_dvs = reader.unsigned(1)
        e1_hs = reader.unsigned(2)
        e1_dvs = reader.unsigned(1)
        svh = (e5b_hs << 7) | (e5b_dvs << 6) | (e1_hs << 1) | e1_dvs
        toc_time = GNSSTime.from_gps_week_tow(week, toc)
        toe_time = GNSSTime.from_gps_week_tow(week, toes)
        eph = KeplerEphemeris(
            system=Constellation.GAL,
            prn=prn,
            toe=toe_time,
            week=week,
            toes=toes,
            toc=toc_time,
            ttr=toc_time,
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
            sva=sva,
            svh=svh,
            tgd=(tgd1, tgd2),
            code=GALILEO_INAV_DATA_SOURCE,
        )
        self._reference_time = toc_time
        return eph

    def _decode_1020(self, reader: _BitReader) -> GlonassEphemeris:
        prn = reader.unsigned(6)
        frequency_channel = reader.unsigned(5) - 7
        reader.unsigned(4)
        tk_h = reader.unsigned(5)
        tk_m = reader.unsigned(6)
        tk_s = reader.unsigned(1) * 30
        svh = reader.unsigned(1)
        reader.unsigned(1)
        tb = reader.unsigned(7)
        velocity = []
        position = []
        acceleration = []
        for _ in range(3):
            velocity.append(reader.sign_magnitude(24) * P2_20 * 1e3)
            position.append(reader.sign_magnitude(27) * P2_11 * 1e3)
            acceleration.append(reader.sign_magnitude(5) * P2_30 * 1e3)
        reader.unsigned(1)
        gamn = reader.sign_magnitude(11) * P2_40
        reader.unsigned(3)
        taun = reader.sign_magnitude(22) * P2_30
        dtaun = reader.unsigned(5) * P2_30
        age = reader.unsigned(5)
        reader.unsigned(1)
        reader.unsigned(4)
        nt = reader.unsigned(11)
        reader.unsigned(2)
        reader.unsigned(1)
        reader.unsigned(11)
        reader.unsigned(32)
        reader.unsigned(5)
        reader.unsigned(22)
        reader.unsigned(1)
        reader.unsigned(7)

        reference = self._last_epoch_time.get(Constellation.GLO, self._reference_time)
        ref_local = reference.datetime_utc + timedelta(hours=3)
        cycle_start_year = ref_local.year - (ref_local.year % 4)
        cycle_start = datetime(cycle_start_year, 1, 1, tzinfo=UTC)
        local_date = (cycle_start + timedelta(days=max(nt - 1, 0))).date()
        local_midnight = datetime.combine(local_date, time(0, 0), tzinfo=UTC)
        tof_local = local_midnight + timedelta(hours=tk_h, minutes=tk_m, seconds=tk_s)
        toe_local = local_midnight + timedelta(seconds=tb * 900.0)
        toe = GNSSTime(utc_to_gpst_seconds(toe_local - timedelta(hours=3)))
        tof = GNSSTime(utc_to_gpst_seconds(tof_local - timedelta(hours=3)))
        eph = GlonassEphemeris(
            system=Constellation.GLO,
            prn=prn,
            toe=toe,
            tof=tof,
            taun=taun,
            gamn=gamn,
            dtaun=dtaun,
            position_m=tuple(position),
            velocity_mps=tuple(velocity),
            acceleration_mps2=tuple(acceleration),
            svh=svh,
            frequency_channel=frequency_channel,
            age=age,
            iode=tb,
        )
        self._reference_time = toe
        return eph

    def _decode_msm_epoch(self, system: Constellation, epoch_value: int) -> GNSSTime:
        reference = self._last_epoch_time.get(system, self._reference_time)
        if system is Constellation.GLO:
            day = epoch_value >> 27
            milliseconds = epoch_value & ((1 << 27) - 1)
            ref_local = reference.datetime_utc + timedelta(hours=3)
            ref_day = ref_local.weekday() + 1
            if ref_day == 7:
                ref_day = 0
            delta_days = day - ref_day
            while delta_days > 3:
                delta_days -= 7
            while delta_days < -3:
                delta_days += 7
            local_date = ref_local.date() + timedelta(days=delta_days)
            local_midnight = datetime.combine(local_date, time(0, 0), tzinfo=UTC)
            local_time = local_midnight + timedelta(milliseconds=milliseconds)
            return GNSSTime(utc_to_gpst_seconds(local_time - timedelta(hours=3)))
        tow = epoch_value / 1000.0
        if system is Constellation.BDS:
            base_seconds = reference.gps_seconds - BDS_TO_GPS_SECONDS_OFFSET
            return GNSSTime(adjust_week(base_seconds, tow) + BDS_TO_GPS_SECONDS_OFFSET)
        return GNSSTime(adjust_week(reference.gps_seconds, tow))

    def _decode_msm(self, message_type: int, reader: _BitReader) -> EpochObservations:
        system, level = _system_from_msm_message(message_type)
        reader.unsigned(12)
        epoch_value = reader.unsigned(30)
        reader.unsigned(1)
        reader.unsigned(3)
        reader.unsigned(7)
        reader.unsigned(2)
        reader.unsigned(2)
        reader.unsigned(1)
        reader.unsigned(3)
        sat_ids = [sat_id for sat_id in range(1, 65) if reader.unsigned(1)]
        msm_ids = [msm_id for msm_id in range(1, 33) if reader.unsigned(1)]
        cell_mask = [[bool(reader.unsigned(1)) for _ in msm_ids] for _ in sat_ids]
        ncell = sum(1 for row in cell_mask for value in row if value)

        rrng_int = [reader.unsigned(8) for _ in sat_ids]
        info = [reader.unsigned(4) for _ in sat_ids] if level in {5, 7} else [0 for _ in sat_ids]
        rrng_mod = [reader.unsigned(10) for _ in sat_ids]
        rrate = [reader.signed(14) for _ in sat_ids] if level in {5, 7} else [0 for _ in sat_ids]

        if level in {4, 5}:
            psrng = [reader.signed(15) for _ in range(ncell)]
            phrng = [reader.signed(22) for _ in range(ncell)]
            lock = [reader.unsigned(4) for _ in range(ncell)]
            half = [reader.unsigned(1) for _ in range(ncell)]
            cnr = [reader.unsigned(6) for _ in range(ncell)]
            rate = [reader.signed(15) for _ in range(ncell)] if level == 5 else [0 for _ in range(ncell)]
            psrng_scale = RANGE_MS * P2_24
            phrng_scale = RANGE_MS * P2_29
            cnr_scale = 1.0
            invalid_psrng = -16384
            invalid_phrng = -2097152
            invalid_rate = -16384
        else:
            psrng = [reader.signed(20) for _ in range(ncell)]
            phrng = [reader.signed(24) for _ in range(ncell)]
            lock = [reader.unsigned(10) for _ in range(ncell)]
            half = [reader.unsigned(1) for _ in range(ncell)]
            cnr = [reader.unsigned(10) for _ in range(ncell)]
            rate = [reader.signed(15) for _ in range(ncell)] if level == 7 else [0 for _ in range(ncell)]
            psrng_scale = RANGE_MS * P2_29
            phrng_scale = RANGE_MS * P2_31
            cnr_scale = 0.0625
            invalid_psrng = -524288
            invalid_phrng = -8388608
            invalid_rate = -16384

        epoch_time = self._decode_msm_epoch(system, epoch_value)
        label_by_msm_id = _msm_id_to_label(system)
        rough_ranges = []
        glonass_fcn = []
        for sat_index, int_value in enumerate(rrng_int):
            if int_value == 255:
                rough_ranges.append(None)
            else:
                rough_value = ((int_value << 10) | rrng_mod[sat_index]) * RANGE_MS * P2_10
                rough_ranges.append(rough_value)
            if system is Constellation.GLO:
                glonass_fcn.append(None if info[sat_index] == 15 else info[sat_index] - 7)
            else:
                glonass_fcn.append(None)

        satellites: list[SatelliteObservation] = []
        cell_index = 0
        for sat_index, sat_id in enumerate(sat_ids):
            signals: list[SignalObservation] = []
            for signal_index, msm_id in enumerate(msm_ids):
                if not cell_mask[sat_index][signal_index]:
                    continue
                label = label_by_msm_id.get(msm_id)
                if label is None:
                    cell_index += 1
                    continue
                rough_range = rough_ranges[sat_index]
                if rough_range is None:
                    cell_index += 1
                    continue
                glonass_slot = glonass_fcn[sat_index]
                lam = wavelength_m(system, label, glonass_slot)

                pseudorange = rough_range
                if psrng[cell_index] != invalid_psrng:
                    pseudorange += psrng[cell_index] * psrng_scale

                carrier_cycles = 0.0
                if phrng[cell_index] != invalid_phrng and lam > 0.0:
                    carrier_cycles = (rough_range + phrng[cell_index] * phrng_scale) / lam

                doppler_hz = 0.0
                if level in {5, 7} and rate[cell_index] != invalid_rate and lam > 0.0:
                    range_rate = rrate[sat_index] + rate[cell_index] * 0.0001
                    doppler_hz = -range_rate / lam

                cnr_dbhz = cnr[cell_index] * cnr_scale
                phase_key = ("msm", system, sat_id, label)
                previous_lock = self._lock_values.get(phase_key)
                slip = previous_lock is not None and lock[cell_index] < previous_lock
                self._lock_values[phase_key] = lock[cell_index]
                lli = (1 if slip else 0) | (2 if half[cell_index] else 0)
                signals.append(
                    SignalObservation(
                        signal_label=label,
                        pseudorange_m=pseudorange,
                        carrier_cycles=carrier_cycles,
                        doppler_hz=doppler_hz,
                        cnr_dbhz=cnr_dbhz,
                        frequency_slot=signal_definition(system, label).slot,
                        source_priority=MSM_SIGNAL_PRIORITY,
                        half_cycle_ambiguity=bool(half[cell_index]),
                        slip_detected=slip,
                        lli=lli,
                    )
                )
                cell_index += 1

            if not signals:
                continue

            prn = sat_id
            if system is Constellation.SBS:
                prn = sat_id + 119
            satellites.append(SatelliteObservation(system=system, prn=prn, signals=signals, glonass_fcn=glonass_fcn[sat_index]))

        epoch = EpochObservations(time=epoch_time, satellites=satellites)
        self._update_epoch_reference(system, epoch_time)
        return epoch
