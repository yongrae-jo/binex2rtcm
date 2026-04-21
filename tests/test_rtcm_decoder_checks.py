from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from binex2rtcm.gnss_time import GNSSTime
from binex2rtcm.model.ephemeris import (
    GALILEO_FNAV_DATA_SOURCE,
    GALILEO_INAV_DATA_SOURCE,
    KeplerEphemeris,
)
from binex2rtcm.model.signals import Constellation
from binex2rtcm.rinex.nav_writer import RinexNavWriter
from binex2rtcm.rinex.segment import RinexSegmentBuffer
from binex2rtcm.rtcm.decoder import RtcmDecoder
from binex2rtcm.rtcm.encoder import RtcmEncoder
from binex2rtcm.rtcm.messages import EphemerisMessage
from binex2rtcm.rtcm.scheduler import RtcmScheduler
from binex2rtcm.config import RinexExportConfig, SchedulerConfig


class RtcmDecoderChecks(unittest.TestCase):
    @staticmethod
    def _galileo_ephemeris(*, code: int, svh: int, tgd: tuple[float, float]) -> KeplerEphemeris:
        toc_time = GNSSTime.from_gps_week_tow(2400, 3600.0)
        toe_time = GNSSTime.from_gps_week_tow(2400, 7200.0)
        return KeplerEphemeris(
            system=Constellation.GAL,
            prn=11,
            toe=toe_time,
            week=2400,
            toes=7200.0,
            toc=toc_time,
            ttr=toc_time,
            iode=33,
            iodc=33,
            f0=1.0e-4,
            f1=-2.0e-12,
            f2=0.0,
            deln=0.0,
            m0=0.1,
            e=0.01,
            sqrt_a=5440.0,
            cuc=0.0,
            cus=0.0,
            crc=100.0,
            crs=-50.0,
            cic=0.0,
            cis=0.0,
            omega0=0.2,
            omega=-0.3,
            i0=0.9,
            omega_dot=0.0,
            idot=0.0,
            sva=12,
            svh=svh,
            tgd=tgd,
            code=code,
        )

    def test_gps_legacy_l2_code_zero_maps_to_l2x_like_rtklib(self) -> None:
        decoder = RtcmDecoder()
        self.assertEqual(decoder._gps_legacy_l2_label(0), "2X")

    def test_galileo_1045_sets_rtklib_data_source_and_rinex_value(self) -> None:
        toc_time = GNSSTime.from_gps_week_tow(2400, 3600.0)
        original = self._galileo_ephemeris(
            code=GALILEO_FNAV_DATA_SOURCE,
            svh=(2 << 4) | (1 << 3),
            tgd=(1.0e-8, 0.0),
        )

        frame = RtcmEncoder(station_id=0).encode(EphemerisMessage(message_type=1045, ephemeris=original))
        decoded_items = RtcmDecoder(reference_time=toc_time).decode(frame)

        self.assertEqual(len(decoded_items), 1)
        decoded = decoded_items[0]
        self.assertIsInstance(decoded, KeplerEphemeris)
        assert isinstance(decoded, KeplerEphemeris)
        self.assertEqual(decoded.code, GALILEO_FNAV_DATA_SOURCE)
        self.assertEqual(decoded.svh, original.svh)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "galileo.nav"
            RinexNavWriter().write(path, [decoded], generated_at=toc_time.datetime_gpst)
            text = path.read_text(encoding="ascii")

        self.assertIn("2.580000000000D+02", text)

    def test_galileo_1046_roundtrip_preserves_inav_source_and_health(self) -> None:
        toc_time = GNSSTime.from_gps_week_tow(2400, 3600.0)
        original = self._galileo_ephemeris(
            code=GALILEO_INAV_DATA_SOURCE,
            svh=(1 << 7) | (1 << 6) | (2 << 1) | 1,
            tgd=(1.0e-8, -2.0e-8),
        )

        frame = RtcmEncoder(station_id=0).encode(EphemerisMessage(message_type=1046, ephemeris=original))
        decoded_items = RtcmDecoder(reference_time=toc_time).decode(frame)

        self.assertEqual(len(decoded_items), 1)
        decoded = decoded_items[0]
        self.assertIsInstance(decoded, KeplerEphemeris)
        assert isinstance(decoded, KeplerEphemeris)
        self.assertEqual(decoded.code, GALILEO_INAV_DATA_SOURCE)
        self.assertEqual(decoded.svh, original.svh)
        self.assertAlmostEqual(decoded.tgd[0], original.tgd[0], places=10)
        self.assertAlmostEqual(decoded.tgd[1], original.tgd[1], places=10)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "galileo_inav.nav"
            RinexNavWriter().write(path, [decoded], generated_at=toc_time.datetime_gpst)
            text = path.read_text(encoding="ascii")

        self.assertIn("5.170000000000D+02", text)

    def test_scheduler_and_rinex_buffer_keep_both_galileo_sources(self) -> None:
        fnav = self._galileo_ephemeris(
            code=GALILEO_FNAV_DATA_SOURCE,
            svh=(2 << 4) | (1 << 3),
            tgd=(1.0e-8, 0.0),
        )
        inav = self._galileo_ephemeris(
            code=GALILEO_INAV_DATA_SOURCE,
            svh=(1 << 7) | (1 << 6) | (2 << 1) | 1,
            tgd=(1.0e-8, -2.0e-8),
        )

        scheduler = RtcmScheduler(SchedulerConfig())
        messages = [
            *scheduler.ingest(fnav),
            *scheduler.ingest(inav),
        ]

        self.assertEqual([message.message_type for message in messages], [1045, 1046])

        buffer = RinexSegmentBuffer(RinexExportConfig(enabled=True, observation=False, navigation=True))
        buffer.ingest_ephemeris(fnav)
        buffer.ingest_ephemeris(inav)
        snapshot = buffer.detach_snapshot()

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(len(snapshot.ephemerides), 2)


if __name__ == "__main__":
    unittest.main()
