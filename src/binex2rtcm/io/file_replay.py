"""File-based input and output."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
import logging
from pathlib import Path

from ..binex import BinexDecoder, BinexFramer
from ..config import InputConfig, OutputConfig
from ..errors import ProtocolError, UnsupportedMessageError, UnsupportedRecordError
from ..model.ephemeris import Ephemeris
from ..model.observation import EpochObservations
from ..model.station import StationInfo
from ..rinex import BackgroundRinexExporter, RinexSegmentBuffer
from ..rtcm import RtcmDecoder, RtcmFramer
from ..stream_logging import LogSegment, RotatingBinaryLog
from .base import InputAdapter, QueuedOutputAdapter

LOGGER = logging.getLogger(__name__)


class FileReplayInput(InputAdapter):
    def __init__(self, config: InputConfig) -> None:
        self._config = config

    async def iter_chunks(self) -> AsyncIterator[bytes]:
        replay_delay = 0.0 if self._config.replay_rate <= 0 else 0.01 / self._config.replay_rate
        with Path(self._config.path or "").expanduser().open("rb") as fp:
            while chunk := fp.read(self._config.chunk_size):
                yield chunk
                if replay_delay > 0:
                    await asyncio.sleep(replay_delay)


class FileOutput(QueuedOutputAdapter):
    def __init__(self, config: OutputConfig, rnx2crx_path: str | None = None) -> None:
        super().__init__(config.max_queue)
        marker_name = config.session or config.name
        self._rinex = (
            RinexSegmentBuffer(config.rinex, rnx2crx_path=rnx2crx_path, marker_name=marker_name)
            if config.rinex.enabled
            else None
        )
        self._rinex_exporter = BackgroundRinexExporter(f"output-{config.name}") if self._rinex is not None else None
        self._data_format = config.data_format.strip().lower()
        self._framer = None
        self._decoder = None
        if self._rinex is not None:
            if self._data_format == "rtcm":
                self._framer = RtcmFramer()
                self._decoder = RtcmDecoder(station_id=0)
            elif self._data_format == "binex":
                self._framer = BinexFramer()
                self._decoder = BinexDecoder(station_id=0)
            else:
                raise ValueError(f"Unsupported file output data format: {config.data_format}")

        def flush_rinex(segment: LogSegment) -> None:
            if self._rinex is None or self._rinex_exporter is None:
                return
            self._rinex_exporter.submit(self._rinex.detach_snapshot(), segment.path, segment.closed_at)

        self._writer = RotatingBinaryLog(Path(config.path or ""), config.interval, on_close=flush_rinex)

    async def start(self) -> None:
        if self._rinex_exporter is not None:
            await self._rinex_exporter.start()
        await super().start()

    async def _write(self, data: bytes, logical_time=None) -> None:
        self._writer.write(data, logical_time)
        if self._rinex is None or self._framer is None or self._decoder is None:
            return
        try:
            for frame in self._framer.feed(data):
                decoded = self._decoder.decode(frame if self._data_format == "rtcm" else frame)
                for item in decoded:
                    if isinstance(item, StationInfo):
                        self._rinex.ingest_station(item)
                    elif isinstance(item, EpochObservations):
                        self._rinex.ingest_epoch(item)
                    elif isinstance(item, Ephemeris):
                        self._rinex.ingest_ephemeris(item)
        except (ProtocolError, UnsupportedMessageError, UnsupportedRecordError) as exc:
            LOGGER.warning("skipping %s->RINEX decode for file output: %s", self._data_format.upper(), exc)

    async def close(self) -> None:
        await super().close()
        try:
            self._writer.close()
        finally:
            if self._rinex_exporter is not None:
                await self._rinex_exporter.close()
