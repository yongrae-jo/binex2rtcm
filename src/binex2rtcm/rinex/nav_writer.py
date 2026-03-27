"""RINEX 3.05 navigation writer."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from ..gnss_time import BDS_TO_GPS_SECONDS_OFFSET, GPS_EPOCH, SECONDS_PER_WEEK
from ..model.ephemeris import Ephemeris, GlonassEphemeris, KeplerEphemeris, SbasEphemeris
from ..model.signals import Constellation
from .header import (
    format_nav_value,
    header_line,
    leap_seconds,
    nav_continuation_prefix,
    nav_first_line_prefix,
    nav_header_type_line,
    program_line,
    rinex_sat_id,
    rinex_sat_sort_key,
)


def _bdt_datetime(seconds: float) -> datetime:
    return GPS_EPOCH + timedelta(seconds=seconds - BDS_TO_GPS_SECONDS_OFFSET)


def _utc_seconds_of_week(dt: datetime) -> float:
    return (dt - GPS_EPOCH).total_seconds() % SECONDS_PER_WEEK


def _gpst_seconds_of_week(seconds: float) -> tuple[int, float]:
    week = int(seconds // SECONDS_PER_WEEK)
    tow = seconds - week * SECONDS_PER_WEEK
    return week, tow


def _continuation_line(*values: float) -> str:
    return f"{nav_continuation_prefix()}{''.join(format_nav_value(value) for value in values)}\n"


def _write_kepler_record(fp, eph: KeplerEphemeris) -> None:
    sat_id = rinex_sat_id(eph.system, eph.prn)
    if eph.system is Constellation.BDS:
        toc = _bdt_datetime(eph.toc.gps_seconds)
        ttr_week, ttr_tow = eph.ttr.bdt_week_tow()
    else:
        toc = eph.toc.datetime_gpst
        ttr_week, ttr_tow = eph.ttr.gps_week_tow()

    line1 = (
        f"{nav_first_line_prefix(sat_id, toc)}"
        f"{format_nav_value(eph.f0)}{format_nav_value(eph.f1)}{format_nav_value(eph.f2)}\n"
    )
    fp.write(line1)
    fp.write(_continuation_line(float(eph.iode), eph.crs, eph.deln, eph.m0))
    fp.write(_continuation_line(eph.cuc, eph.e, eph.cus, eph.sqrt_a))
    fp.write(_continuation_line(eph.toes, eph.cic, eph.omega0, eph.cis))
    fp.write(_continuation_line(eph.i0, eph.crc, eph.omega, eph.omega_dot))
    fp.write(_continuation_line(eph.idot, float(eph.code), float(eph.week), float(eph.flag)))

    if eph.system in {Constellation.GAL, Constellation.BDS}:
        fp.write(_continuation_line(float(eph.sva), float(eph.svh), eph.tgd[0], eph.tgd[1]))
    else:
        fp.write(_continuation_line(float(eph.sva), float(eph.svh), eph.tgd[0], float(eph.iodc)))

    ttr_value = ttr_tow + (ttr_week - eph.week) * SECONDS_PER_WEEK
    if eph.system in {Constellation.GPS, Constellation.QZS, Constellation.IRN}:
        fp.write(_continuation_line(ttr_value, eph.fit))
    elif eph.system is Constellation.BDS:
        fp.write(_continuation_line(ttr_value, float(eph.iodc)))
    else:
        fp.write(_continuation_line(ttr_value, 0.0))


def _write_glonass_record(fp, eph: GlonassEphemeris) -> None:
    toe = eph.toe.datetime_utc
    sat_id = rinex_sat_id(Constellation.GLO, eph.prn)
    tof = _utc_seconds_of_week(eph.tof.datetime_utc)
    line1 = (
        f"{nav_first_line_prefix(sat_id, toe)}"
        f"{format_nav_value(-eph.taun)}{format_nav_value(eph.gamn)}{format_nav_value(tof)}\n"
    )
    fp.write(line1)
    fp.write(
        _continuation_line(
            eph.position_m[0] / 1e3,
            eph.velocity_mps[0] / 1e3,
            eph.acceleration_mps2[0] / 1e3,
            float(eph.svh),
        )
    )
    fp.write(
        _continuation_line(
            eph.position_m[1] / 1e3,
            eph.velocity_mps[1] / 1e3,
            eph.acceleration_mps2[1] / 1e3,
            float(eph.frequency_channel),
        )
    )
    fp.write(
        _continuation_line(
            eph.position_m[2] / 1e3,
            eph.velocity_mps[2] / 1e3,
            eph.acceleration_mps2[2] / 1e3,
            float(eph.age),
        )
    )


def _write_sbas_record(fp, eph: SbasEphemeris) -> None:
    toe = eph.toe.datetime_gpst
    sat_id = rinex_sat_id(Constellation.SBS, eph.prn)
    _, tof = _gpst_seconds_of_week(eph.tof.gps_seconds)
    line1 = (
        f"{nav_first_line_prefix(sat_id, toe)}"
        f"{format_nav_value(eph.af0)}{format_nav_value(0.0)}{format_nav_value(tof)}\n"
    )
    fp.write(line1)
    fp.write(
        _continuation_line(
            eph.position_m[0] / 1e3,
            eph.velocity_mps[0] / 1e3,
            eph.acceleration_mps2[0] / 1e3,
            float(eph.svh),
        )
    )
    fp.write(
        _continuation_line(
            eph.position_m[1] / 1e3,
            eph.velocity_mps[1] / 1e3,
            eph.acceleration_mps2[1] / 1e3,
            float(eph.sva),
        )
    )
    fp.write(
        _continuation_line(
            eph.position_m[2] / 1e3,
            eph.velocity_mps[2] / 1e3,
            eph.acceleration_mps2[2] / 1e3,
            0.0,
        )
    )


def _sort_key(eph: Ephemeris) -> tuple[int, int, str, float]:
    if isinstance(eph, KeplerEphemeris):
        epoch = eph.toc.gps_seconds
    else:
        epoch = eph.toe.gps_seconds
    system_rank, prn, sat_id = rinex_sat_sort_key(eph.system, eph.prn)
    return system_rank, prn, sat_id, epoch


class RinexNavWriter:
    """Write buffered ephemerides into a RINEX 3.05 navigation file."""

    def write(self, path: Path, ephemerides: list[Ephemeris], generated_at: datetime | None = None) -> Path | None:
        if not ephemerides:
            return None
        first_time = None
        if isinstance(ephemerides[0], KeplerEphemeris):
            first_time = ephemerides[0].toc
        else:
            first_time = ephemerides[0].toe

        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="ascii", newline="") as fp:
            fp.write(nav_header_type_line())
            fp.write(program_line(generated_at))
            fp.write(header_line(f"{leap_seconds(first_time):6d}", "LEAP SECONDS"))
            fp.write(header_line("", "END OF HEADER"))
            for eph in sorted(ephemerides, key=_sort_key):
                if isinstance(eph, KeplerEphemeris):
                    _write_kepler_record(fp, eph)
                elif isinstance(eph, GlonassEphemeris):
                    _write_glonass_record(fp, eph)
                elif isinstance(eph, SbasEphemeris):
                    _write_sbas_record(fp, eph)
        return path
