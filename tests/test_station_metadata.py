from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from binex2rtcm.config import SchedulerConfig
from binex2rtcm.gnss_time import GNSSTime
from binex2rtcm.model.observation import EpochObservations, SatelliteObservation, SignalObservation
from binex2rtcm.model.signals import Constellation, signal_definition
from binex2rtcm.model.station import StationInfo
from binex2rtcm.rinex.obs_writer import RinexObsWriter
from binex2rtcm.rtcm.decoder import RtcmDecoder
from binex2rtcm.rtcm.encoder import RtcmEncoder
from binex2rtcm.rtcm.messages import StationMessage
from binex2rtcm.rtcm.scheduler import RtcmScheduler


class StationMetadataTests(unittest.TestCase):
    def test_rtcm_1005_and_1007_update_station_info(self) -> None:
        station = StationInfo(
            station_id=1234,
            ecef_xyz_m=(1111.1, 2222.2, 3333.3),
            antenna_descriptor="TRM57971.00",
            antenna_radome="NONE",
        )
        encoder = RtcmEncoder(station_id=station.station_id)
        decoder = RtcmDecoder()

        from_1005 = decoder.decode(encoder.encode(StationMessage(1005, station)))[0]
        from_1007 = decoder.decode(encoder.encode(StationMessage(1007, station)))[0]

        self.assertEqual(from_1005.station_id, 1234)
        self.assertEqual(from_1005.site_identifier, "1234")
        assert from_1005.ecef_xyz_m is not None
        for actual, expected in zip(from_1005.ecef_xyz_m, station.ecef_xyz_m):
            self.assertAlmostEqual(actual, expected, places=4)
        self.assertEqual(from_1007.station_id, 1234)
        self.assertEqual(from_1007.antenna_descriptor, "TRM57971.00")
        self.assertEqual(from_1007.antenna_radome, "NONE")
        assert from_1007.ecef_xyz_m is not None
        for actual, expected in zip(from_1007.ecef_xyz_m, station.ecef_xyz_m):
            self.assertAlmostEqual(actual, expected, places=4)

    def test_partial_station_info_still_populates_rinex_header(self) -> None:
        station = StationInfo(
            station_id=1234,
            ecef_xyz_m=None,
            antenna_descriptor="TRM57971.00",
            antenna_radome="NONE",
            receiver_type="RXTYPE",
            receiver_version="FW1.0",
            site_identifier="1234",
        )
        signal = SignalObservation(
            signal_label="1C",
            pseudorange_m=20200000.0,
            carrier_cycles=0.0,
            doppler_hz=0.0,
            cnr_dbhz=45.0,
            frequency_slot=signal_definition(Constellation.GPS, "1C").slot,
        )
        epoch = EpochObservations(
            time=GNSSTime.from_gps_week_tow(2400, 0.0),
            satellites=[SatelliteObservation(system=Constellation.GPS, prn=1, signals=[signal])],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "partial.obs.rnx"
            RinexObsWriter().write(path, station, [epoch])
            text = path.read_text(encoding="ascii")

        self.assertIn("1234", text)
        self.assertIn("RXTYPE", text)
        self.assertIn("FW1.0", text)
        self.assertIn("TRM57971.00", text)
        self.assertIn("APPROX POSITION XYZ", text)
        self.assertIn("0.0000", text)

    def test_scheduler_skips_1006_when_position_is_missing(self) -> None:
        scheduler = RtcmScheduler(SchedulerConfig())
        station = StationInfo(
            station_id=1234,
            ecef_xyz_m=None,
            antenna_descriptor="TRM57971.00",
            antenna_radome="NONE",
            receiver_type="RXTYPE",
        )

        messages = scheduler.ingest(station)
        message_types = [message.message_type for message in messages]

        self.assertNotIn(1006, message_types)
        self.assertIn(1007, message_types)
        self.assertIn(1033, message_types)
        self.assertIn(1230, message_types)


if __name__ == "__main__":
    unittest.main()
