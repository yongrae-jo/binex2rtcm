"""Runtime service wiring."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass
import logging
from pathlib import Path

from .binex import BinexDecoder, BinexEncoder, BinexFramer, BinexScheduler
from .config import AppConfig, InputConfig, OutputConfig
from .errors import ProtocolError, UnsupportedMessageError, UnsupportedRecordError
from .io import FileOutput, FileReplayInput, NtripClientInput, OutputAdapter, TcpClientInput, TcpClientOutput, TcpServerOutput
from .model.ephemeris import Ephemeris
from .model.observation import EpochObservations, SatelliteObservation
from .model.station import StationInfo
from .monitor import ConsoleMonitor
from .rinex import RinexSegmentBuffer
from .rinex.header import rinex_sat_sort_key
from .rtcm import MsmMessage, RtcmDecoder, RtcmEncoder, RtcmFramer, RtcmScheduler
from .stats import InputStats, OutputStats, RuntimeStats
from .stream_logging import LogSegment, RotatingBinaryLog
from .validation import RtcmValidator

LOGGER = logging.getLogger(__name__)


def _normalized_format(value: str) -> str:
    return value.strip().lower()


def _logical_time_from_item(item: object):
    if isinstance(item, EpochObservations):
        return item.time.datetime_gpst
    return None


def _logical_time_from_items(items: list[object]):
    for item in items:
        logical_time = _logical_time_from_item(item)
        if logical_time is not None:
            return logical_time
    return None


def _logical_time_from_payload(payload: object, fallback=None):
    if isinstance(payload, MsmMessage):
        return payload.epoch.time.datetime_gpst
    return fallback


def _merge_epoch_observations(left: EpochObservations, right: EpochObservations) -> EpochObservations:
    satellites = {}
    for source in (*left.satellites, *right.satellites):
        key = (source.system, source.prn)
        current = satellites.get(key)
        if current is None:
            satellites[key] = source
            continue
        signal_map = {signal.signal_label: signal for signal in current.signals}
        for signal in source.signals:
            signal_map[signal.signal_label] = signal
        satellites[key] = SatelliteObservation(
            system=source.system,
            prn=source.prn,
            signals=[signal_map[label] for label in sorted(signal_map)],
            glonass_fcn=source.glonass_fcn if source.glonass_fcn is not None else current.glonass_fcn,
        )
    return EpochObservations(
        time=left.time,
        receiver_clock_offset_s=left.receiver_clock_offset_s
        if left.receiver_clock_offset_s is not None
        else right.receiver_clock_offset_s,
        satellites=sorted(satellites.values(), key=lambda item: rinex_sat_sort_key(item.system, item.prn)),
    )


@dataclass(slots=True)
class _OutputTarget:
    config: OutputConfig
    adapter: OutputAdapter


@dataclass(slots=True)
class _OutputGroup:
    data_format: str
    targets: list[_OutputTarget]
    scheduler: object
    encoder: object
    validator: RtcmValidator | None = None


class _InputCodec:
    def __init__(self, data_format: str) -> None:
        self.data_format = _normalized_format(data_format)
        if self.data_format == "binex":
            self._framer = BinexFramer()
            self._decoder = BinexDecoder(station_id=0)
        elif self.data_format == "rtcm":
            self._framer = RtcmFramer()
            self._decoder = RtcmDecoder(station_id=0)
        else:
            raise ValueError(f"Unsupported input data format: {data_format}")

    def feed(self, chunk: bytes) -> list[object]:
        return self._framer.feed(chunk)

    def reset_framer(self) -> None:
        self._framer.reset()

    def raw_frame(self, frame: object) -> bytes:
        return frame.raw if self.data_format == "binex" else frame

    def decode(self, frame: object) -> list[object]:
        return self._decoder.decode(frame)


class ConversionService:
    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._outputs = [(item, self._build_output(item)) for item in config.outputs]
        self._session_outputs: dict[str, list[_OutputTarget]] = defaultdict(list)
        for item, output in self._outputs:
            self._session_outputs[self._session_name(item.session)].append(_OutputTarget(item, output))
        self._stats = RuntimeStats(
            inputs={
                item.name: InputStats(item.name, item.kind, self._session_name(item.session))
                for item in config.inputs
            },
            outputs={
                item.name: OutputStats(item.name, item.kind, self._session_name(item.session))
                for item in config.outputs
            },
        )
        self._monitor = ConsoleMonitor(self._stats, config.monitor.interval_s) if config.monitor.enabled else None

    async def run(self) -> None:
        for _, output in self._outputs:
            await output.start()
        monitor_task = (
            asyncio.create_task(self._monitor.run(), name="console-monitor")
            if self._monitor is not None
            else None
        )
        tasks = [
            asyncio.create_task(self._run_input_pipeline(input_config), name=f"input-{input_config.name}")
            for input_config in self._config.inputs
        ]
        try:
            if self._config.run_duration_s is None:
                await asyncio.gather(*tasks)
            else:
                done, pending = await asyncio.wait(tasks, timeout=self._config.run_duration_s)
                if pending:
                    LOGGER.info("run duration reached; stopping after %.1fs", self._config.run_duration_s)
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                for task in done:
                    task.result()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if self._monitor is not None:
                self._monitor.stop()
            if monitor_task is not None:
                with suppress(Exception):
                    await monitor_task
            for _, output in self._outputs:
                await output.close()

    async def _run_input_pipeline(self, input_config: InputConfig) -> None:
        input_stats = self._stats.inputs[input_config.name]
        session_targets = self._session_outputs[self._session_name(input_config.session)]
        output_groups = self._build_output_groups(session_targets)
        adapter = self._build_input(input_config)
        input_codec = _InputCodec(input_config.data_format)
        capture_log = None
        capture_rinex = None
        if input_config.capture_path and input_config.capture_rinex.enabled:
            capture_rinex = RinexSegmentBuffer(input_config.capture_rinex, marker_name=input_config.session or input_config.name)

        def flush_capture_rinex(segment: LogSegment) -> None:
            if capture_rinex is None:
                return
            capture_rinex.export(segment.path, segment.closed_at)
            capture_rinex.reset()

        if input_config.capture_path:
            capture_log = RotatingBinaryLog(
                Path(input_config.capture_path),
                input_config.capture_interval,
                on_close=flush_capture_rinex,
            )

        pending_epoch: EpochObservations | None = None

        async def process_item(item: object) -> None:
            item_time = _logical_time_from_item(item)
            if isinstance(item, EpochObservations):
                input_stats.epochs += 1
            elif isinstance(item, Ephemeris):
                input_stats.ephemerides += 1
            for group in output_groups:
                await self._emit_item_to_group(group, item, item_time, input_stats)

        async def flush_pending_epoch() -> None:
            nonlocal pending_epoch
            if pending_epoch is None:
                return
            epoch = pending_epoch
            pending_epoch = None
            await process_item(epoch)

        cancelled_error: asyncio.CancelledError | None = None
        try:
            for group in output_groups:
                await self._emit_bootstrap(group, input_stats)

            async for chunk in adapter.iter_chunks():
                if not chunk:
                    input_codec.reset_framer()
                    await flush_pending_epoch()
                    continue
                try:
                    input_stats.mark_activity(len(chunk))
                    frames = input_codec.feed(chunk)
                    input_stats.binex_frames += len(frames)
                except ProtocolError as exc:
                    input_stats.errors += 1
                    input_stats.last_error = str(exc)
                    LOGGER.warning("discarding malformed %s chunk: %s", input_codec.data_format.upper(), exc)
                    continue

                for frame in frames:
                    raw_frame = input_codec.raw_frame(frame)
                    try:
                        decoded_items = input_codec.decode(frame)
                    except (UnsupportedRecordError, UnsupportedMessageError) as exc:
                        input_stats.ignored_records += 1
                        input_stats.last_error = str(exc)
                        LOGGER.debug("ignoring %s frame: %s", input_codec.data_format.upper(), exc)
                        if capture_log is not None:
                            input_stats.capture_bytes += capture_log.write(raw_frame)
                        continue
                    except ProtocolError as exc:
                        input_stats.errors += 1
                        input_stats.last_error = str(exc)
                        LOGGER.warning("discarding malformed %s frame: %s", input_codec.data_format.upper(), exc)
                        if capture_log is not None:
                            input_stats.capture_bytes += capture_log.write(raw_frame)
                        continue

                    frame_time = _logical_time_from_items(decoded_items)
                    if capture_log is not None:
                        input_stats.capture_bytes += capture_log.write(raw_frame, frame_time)

                    for item in decoded_items:
                        if capture_rinex is not None:
                            if isinstance(item, StationInfo):
                                capture_rinex.ingest_station(item)
                            elif isinstance(item, EpochObservations):
                                capture_rinex.ingest_epoch(item)
                            elif isinstance(item, Ephemeris):
                                capture_rinex.ingest_ephemeris(item)
                        if isinstance(item, EpochObservations):
                            if pending_epoch is None:
                                pending_epoch = item
                            elif item.time.gps_seconds == pending_epoch.time.gps_seconds:
                                pending_epoch = _merge_epoch_observations(pending_epoch, item)
                            else:
                                await flush_pending_epoch()
                                pending_epoch = item
                            continue
                        await process_item(item)
            await flush_pending_epoch()
        except asyncio.CancelledError as exc:
            cancelled_error = exc
        finally:
            try:
                await flush_pending_epoch()
            finally:
                if capture_log is not None:
                    capture_log.close()
        if cancelled_error is not None:
            raise cancelled_error

    async def _emit_bootstrap(self, group: _OutputGroup, input_stats: InputStats) -> None:
        if group.data_format == "rtcm":
            scheduler = group.scheduler
            encoder = group.encoder
            validator = group.validator
            for payload in scheduler.bootstrap():
                try:
                    encoded_frames = encoder.encode_many(payload)
                except Exception as exc:
                    input_stats.errors += 1
                    input_stats.last_error = str(exc)
                    LOGGER.warning("failed to emit bootstrap RTCM payload: %s", exc)
                    continue
                for data in encoded_frames:
                    try:
                        if validator is not None:
                            validator.validate(data)
                        await self._emit_targets(group.targets, data, _logical_time_from_payload(payload))
                        input_stats.rtcm_messages += 1
                    except Exception as exc:
                        input_stats.errors += 1
                        input_stats.last_error = str(exc)
                        LOGGER.warning("failed to emit bootstrap RTCM payload: %s", exc)
        else:
            scheduler = group.scheduler
            encoder = group.encoder
            for scheduled_item in scheduler.bootstrap():
                logical_time = _logical_time_from_item(scheduled_item)
                try:
                    await self._emit_targets(group.targets, encoder.encode(scheduled_item, logical_time), logical_time)
                except Exception as exc:
                    input_stats.errors += 1
                    input_stats.last_error = str(exc)
                    LOGGER.warning("failed to emit bootstrap BINEX payload: %s", exc)

    async def _emit_item_to_group(
        self,
        group: _OutputGroup,
        item: object,
        item_time,
        input_stats: InputStats,
    ) -> None:
        if group.data_format == "rtcm":
            scheduler = group.scheduler
            encoder = group.encoder
            validator = group.validator
            scheduled_payloads = scheduler.ingest(item)
            msm_payload_indexes = [
                index for index, payload in enumerate(scheduled_payloads) if isinstance(payload, MsmMessage)
            ]
            shared_msm_sequence = encoder.current_msm_sequence() if msm_payload_indexes else None
            last_msm_payload_index = msm_payload_indexes[-1] if msm_payload_indexes else None
            for index, payload in enumerate(scheduled_payloads):
                try:
                    if isinstance(payload, MsmMessage):
                        encoded_frames = encoder.encode_many(
                            payload,
                            final_observation_message=index == last_msm_payload_index,
                            sequence=shared_msm_sequence,
                            advance_sequence=False,
                        )
                    else:
                        encoded_frames = encoder.encode_many(payload)
                except UnsupportedMessageError as exc:
                    input_stats.errors += 1
                    input_stats.last_error = str(exc)
                    LOGGER.warning("skipping unsupported RTCM payload: %s", exc)
                    continue
                except Exception as exc:
                    input_stats.errors += 1
                    input_stats.last_error = str(exc)
                    LOGGER.warning("failed to encode RTCM payload: %s", exc)
                    continue
                for data in encoded_frames:
                    try:
                        if validator is not None:
                            validator.validate(data)
                        await self._emit_targets(group.targets, data, _logical_time_from_payload(payload, item_time))
                        input_stats.rtcm_messages += 1
                    except Exception as exc:
                        input_stats.errors += 1
                        input_stats.last_error = str(exc)
                        LOGGER.warning("failed to emit RTCM payload: %s", exc)
            if msm_payload_indexes:
                encoder.advance_msm_sequence()
        else:
            scheduler = group.scheduler
            encoder = group.encoder
            for scheduled_item in scheduler.ingest(item):
                logical_time = _logical_time_from_item(scheduled_item) or item_time
                try:
                    data = encoder.encode(scheduled_item, logical_time)
                except UnsupportedRecordError as exc:
                    input_stats.errors += 1
                    input_stats.last_error = str(exc)
                    LOGGER.warning("skipping unsupported BINEX payload: %s", exc)
                    continue
                except Exception as exc:
                    input_stats.errors += 1
                    input_stats.last_error = str(exc)
                    LOGGER.warning("failed to encode BINEX payload: %s", exc)
                    continue
                try:
                    await self._emit_targets(group.targets, data, logical_time)
                except Exception as exc:
                    input_stats.errors += 1
                    input_stats.last_error = str(exc)
                    LOGGER.warning("failed to emit BINEX payload: %s", exc)

    async def _emit_targets(self, targets: list[_OutputTarget], data: bytes, logical_time=None) -> None:
        for target in targets:
            output_stats = self._stats.outputs[target.config.name]
            try:
                await target.adapter.write(data, logical_time)
                output_stats.mark_activity(len(data))
            except Exception as exc:
                output_stats.errors += 1
                output_stats.last_error = str(exc)
                raise

    def _build_output_groups(self, targets: list[_OutputTarget]) -> list[_OutputGroup]:
        grouped_targets: dict[str, list[_OutputTarget]] = defaultdict(list)
        for target in targets:
            grouped_targets[_normalized_format(target.config.data_format)].append(target)

        groups: list[_OutputGroup] = []
        for data_format, grouped in grouped_targets.items():
            if data_format == "rtcm":
                groups.append(
                    _OutputGroup(
                        data_format=data_format,
                        targets=grouped,
                        scheduler=RtcmScheduler(self._config.scheduler),
                        encoder=RtcmEncoder(0),
                        validator=RtcmValidator(self._config.validate_rtcm),
                    )
                )
            elif data_format == "binex":
                groups.append(
                    _OutputGroup(
                        data_format=data_format,
                        targets=grouped,
                        scheduler=BinexScheduler(self._config.scheduler),
                        encoder=BinexEncoder(),
                    )
                )
            else:
                raise ValueError(f"Unsupported output data format: {data_format}")
        return groups

    def _build_input(self, config: InputConfig):
        kind = config.kind.lower()
        if kind == "ntrip_client":
            return NtripClientInput(config)
        if kind == "tcp_client":
            return TcpClientInput(config)
        if kind == "file_replay":
            return FileReplayInput(config)
        raise ValueError(f"Unsupported input kind: {config.kind}")

    def _build_output(self, config: OutputConfig) -> OutputAdapter:
        kind = config.kind.lower()
        if kind == "tcp_server":
            return TcpServerOutput(config)
        if kind == "tcp_client":
            return TcpClientOutput(config)
        if kind == "file":
            return FileOutput(config)
        raise ValueError(f"Unsupported output kind: {config.kind}")

    def _session_name(self, session: str | None) -> str:
        return session or "default"
