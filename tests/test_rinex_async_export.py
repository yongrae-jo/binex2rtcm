from __future__ import annotations

import tempfile
from pathlib import Path
import unittest

from binex2rtcm.config import RinexExportConfig
from binex2rtcm.rinex import BackgroundRinexExporter, RinexSegmentBuffer


class _FakeSnapshot:
    def __init__(self, exported_paths: list[Path]) -> None:
        self._exported_paths = exported_paths

    def empty(self) -> bool:
        return False

    def export(self, segment_path: Path, generated_at=None) -> list[Path]:
        segment_path.parent.mkdir(parents=True, exist_ok=True)
        segment_path.write_text("exported", encoding="ascii")
        self._exported_paths.append(segment_path)
        return [segment_path]


class RinexSegmentBufferTests(unittest.TestCase):
    def test_detach_snapshot_moves_current_segment_without_clearing_snapshot(self) -> None:
        buffer = RinexSegmentBuffer(RinexExportConfig(enabled=True), marker_name="TEST")
        station = object()
        epoch = object()
        ephemeris = object()

        buffer.station = station
        buffer.epochs = [epoch]
        buffer._ephemerides = {("G", 1): ephemeris}

        snapshot = buffer.detach_snapshot()

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertIs(snapshot.station, station)
        self.assertEqual(snapshot.epochs, [epoch])
        self.assertEqual(snapshot.ephemerides, {("G", 1): ephemeris})
        self.assertEqual(buffer.epochs, [])
        self.assertEqual(buffer._ephemerides, {})
        self.assertIs(buffer.station, station)


class BackgroundRinexExporterTests(unittest.IsolatedAsyncioTestCase):
    async def test_close_waits_for_queued_exports(self) -> None:
        exported_paths: list[Path] = []
        exporter = BackgroundRinexExporter("test-exporter")

        with tempfile.TemporaryDirectory() as tmpdir:
            segment_path = Path(tmpdir) / "segment.obs"
            await exporter.start()
            exporter.submit(_FakeSnapshot(exported_paths), segment_path)
            await exporter.close()

            self.assertEqual(exported_paths, [segment_path])
            self.assertTrue(segment_path.exists())


if __name__ == "__main__":
    unittest.main()
