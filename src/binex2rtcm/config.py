"""Configuration models."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import tomllib

from .errors import ConfigurationError
from .stream_logging import normalize_log_interval

SUPPORTED_DATA_FORMATS = {"binex", "rtcm"}


@dataclass(slots=True)
class InputConfig:
    name: str
    kind: str
    session: str | None = None
    data_format: str = "binex"
    host: str | None = None
    port: int | None = None
    mountpoint: str | None = None
    username: str | None = None
    password: str | None = None
    path: str | None = None
    chunk_size: int = 4096
    replay_rate: float = 1.0
    connect_timeout_s: float = 10.0
    reconnect_delay_s: float = 5.0
    send_nmea_gga: bool = False
    gga_interval_s: float = 15.0
    source_position_llh: tuple[float, float, float] | None = None
    capture_path: str | None = None
    capture_interval: str | None = None
    capture_rinex: RinexExportConfig = field(default_factory=lambda: RinexExportConfig())


@dataclass(slots=True)
class OutputConfig:
    name: str
    kind: str
    session: str | None = None
    data_format: str = "rtcm"
    host: str | None = None
    port: int | None = None
    path: str | None = None
    interval: str | None = None
    max_queue: int = 512
    rinex: RinexExportConfig = field(default_factory=lambda: RinexExportConfig())


@dataclass(slots=True)
class RinexExportConfig:
    enabled: bool = False
    observation: bool = True
    navigation: bool = True

    @property
    def emits_any(self) -> bool:
        return self.enabled and (self.observation or self.navigation)


@dataclass(slots=True)
class SchedulerConfig:
    metadata_interval_s: float = 30.0
    ephemeris_interval_s: float = 300.0
    emit_ephemeris_on_change: bool = True
    emit_metadata_on_start: bool = True
    msm_level_by_system: dict[str, int] = field(
        default_factory=lambda: {
            "GPS": 7,
            "GLO": 7,
            "GAL": 7,
            "BDS": 7,
            "QZS": 7,
            "IRN": 7,
            "SBS": 7,
        }
    )


@dataclass(slots=True)
class MonitorConfig:
    enabled: bool = False
    interval_s: float = 1.0


@dataclass(slots=True)
class AppConfig:
    logging_level: str = "INFO"
    validate_rtcm: bool = True
    run_duration_s: float | None = None
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    monitor: MonitorConfig = field(default_factory=MonitorConfig)
    inputs: list[InputConfig] = field(default_factory=list)
    outputs: list[OutputConfig] = field(default_factory=list)


def _as_llh(value: object) -> tuple[float, float, float] | None:
    if value is None:
        return None
    if not isinstance(value, (list, tuple)) or len(value) != 3:
        raise ConfigurationError("source_position_llh must be a 3-element array")
    return float(value[0]), float(value[1]), float(value[2])


def _as_interval(value: object, field_name: str) -> str | None:
    try:
        return normalize_log_interval(None if value is None else str(value))
    except ValueError as exc:
        raise ConfigurationError(f"{field_name}: {exc}") from exc


def _as_optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def _as_rinex_export(value: object, field_name: str) -> RinexExportConfig:
    if value is None:
        return RinexExportConfig()
    if not isinstance(value, dict):
        raise ConfigurationError(f"{field_name} must be a table or inline table")
    return RinexExportConfig(
        enabled=bool(value.get("enabled", False)),
        observation=bool(value.get("observation", True)),
        navigation=bool(value.get("navigation", True)),
    )


def _validate_config(app: AppConfig) -> None:
    input_names = {item.name for item in app.inputs}
    if len(input_names) != len(app.inputs):
        raise ConfigurationError("Input names must be unique")
    output_names = {item.name for item in app.outputs}
    if len(output_names) != len(app.outputs):
        raise ConfigurationError("Output names must be unique")

    for item in app.inputs:
        kind = item.kind.lower()
        data_format = item.data_format.strip().lower()
        if data_format not in SUPPORTED_DATA_FORMATS:
            raise ConfigurationError(
                f"Input {item.name} has unsupported data_format {item.data_format!r}; use binex or rtcm"
            )
        if kind == "file_replay" and not item.path:
            raise ConfigurationError(f"Input {item.name} requires path for file_replay")
        if kind == "ntrip_client":
            if not item.host or item.port is None or not item.mountpoint:
                raise ConfigurationError(
                    f"Input {item.name} requires host, port, and mountpoint for ntrip_client"
                )
        if kind == "tcp_client" and (not item.host or item.port is None):
            raise ConfigurationError(f"Input {item.name} requires host and port for tcp_client")
        if item.capture_interval and not item.capture_path:
            raise ConfigurationError(f"Input {item.name} defines capture_interval without capture_path")
        if item.capture_rinex.enabled and not item.capture_path:
            raise ConfigurationError(f"Input {item.name} enables capture_rinex without capture_path")
        if item.capture_rinex.enabled and not item.capture_rinex.emits_any:
            raise ConfigurationError(
                f"Input {item.name} capture_rinex enables no output; set observation or navigation"
            )
        if item.send_nmea_gga and item.source_position_llh is None:
            raise ConfigurationError(
                f"Input {item.name} enables send_nmea_gga but does not define source_position_llh"
            )

    for item in app.outputs:
        kind = item.kind.lower()
        data_format = item.data_format.strip().lower()
        if data_format not in SUPPORTED_DATA_FORMATS:
            raise ConfigurationError(
                f"Output {item.name} has unsupported data_format {item.data_format!r}; use binex or rtcm"
            )
        if kind == "file" and not item.path:
            raise ConfigurationError(f"Output {item.name} requires path for file output")
        if kind in {"tcp_client", "tcp_server"} and (not item.host or item.port is None):
            raise ConfigurationError(f"Output {item.name} requires host and port for {kind}")
        if item.interval and kind != "file":
            raise ConfigurationError(f"Output {item.name} interval is only valid for file outputs")
        if item.rinex.enabled and kind != "file":
            raise ConfigurationError(f"Output {item.name} rinex is only valid for file outputs")
        if item.rinex.enabled and not item.rinex.emits_any:
            raise ConfigurationError(
                f"Output {item.name} rinex enables no output; set observation or navigation"
            )

    input_sessions = {item.session or "default" for item in app.inputs}
    output_sessions = {item.session or "default" for item in app.outputs}
    if len(app.inputs) > 1 and any(item.session is None for item in app.inputs):
        raise ConfigurationError("When multiple inputs are configured, every input must declare session")
    if not app.outputs:
        return
    if len(input_sessions) > 1 and any(item.session is None for item in app.outputs):
        raise ConfigurationError("When multiple sessions are configured, every output must declare session")
    missing_output_sessions = sorted(input_sessions - output_sessions)
    if missing_output_sessions:
        raise ConfigurationError(
            f"Missing outputs for session(s): {', '.join(missing_output_sessions)}"
        )
    unknown_output_sessions = sorted(output_sessions - input_sessions)
    if unknown_output_sessions:
        raise ConfigurationError(
            f"Output session(s) without matching input: {', '.join(unknown_output_sessions)}"
        )

def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path)
    with config_path.open("rb") as fp:
        raw = tomllib.load(fp)

    app = AppConfig()
    app.logging_level = str(raw.get("logging", {}).get("level", app.logging_level))
    app.validate_rtcm = bool(raw.get("validation", {}).get("parse_with_pyrtcm", app.validate_rtcm))
    runtime_raw = raw.get("runtime", {})
    if runtime_raw.get("duration_s") is not None:
        duration_s = float(runtime_raw["duration_s"])
        app.run_duration_s = duration_s if duration_s > 0 else None

    scheduler_raw = raw.get("scheduler", {})
    app.scheduler = SchedulerConfig(
        metadata_interval_s=float(
            scheduler_raw.get("metadata_interval_s", app.scheduler.metadata_interval_s)
        ),
        ephemeris_interval_s=float(
            scheduler_raw.get("ephemeris_interval_s", app.scheduler.ephemeris_interval_s)
        ),
        emit_ephemeris_on_change=bool(
            scheduler_raw.get("emit_ephemeris_on_change", app.scheduler.emit_ephemeris_on_change)
        ),
        emit_metadata_on_start=bool(
            scheduler_raw.get("emit_metadata_on_start", app.scheduler.emit_metadata_on_start)
        ),
        msm_level_by_system={
            key.upper(): int(value)
            for key, value in scheduler_raw.get("msm_level_by_system", app.scheduler.msm_level_by_system).items()
        },
    )
    monitor_raw = raw.get("monitor", {})
    app.monitor = MonitorConfig(
        enabled=bool(monitor_raw.get("enabled", app.monitor.enabled)),
        interval_s=float(monitor_raw.get("interval_s", app.monitor.interval_s)),
    )

    for item in raw.get("inputs", []):
        app.inputs.append(
            InputConfig(
                name=str(item["name"]),
                kind=str(item["kind"]),
                session=_as_optional_str(item.get("session")),
                data_format=str(item.get("data_format", "binex")),
                host=item.get("host"),
                port=int(item["port"]) if item.get("port") is not None else None,
                mountpoint=item.get("mountpoint"),
                username=item.get("username"),
                password=item.get("password"),
                path=item.get("path"),
                chunk_size=int(item.get("chunk_size", 4096)),
                replay_rate=float(item.get("replay_rate", 1.0)),
                connect_timeout_s=float(item.get("connect_timeout_s", 10.0)),
                reconnect_delay_s=float(item.get("reconnect_delay_s", 5.0)),
                send_nmea_gga=bool(item.get("send_nmea_gga", False)),
                gga_interval_s=float(item.get("gga_interval_s", 15.0)),
                source_position_llh=_as_llh(item.get("source_position_llh")),
                capture_path=item.get("capture_path"),
                capture_interval=_as_interval(item.get("capture_interval"), "inputs.capture_interval"),
                capture_rinex=_as_rinex_export(item.get("capture_rinex"), "inputs.capture_rinex"),
            )
        )

    for item in raw.get("outputs", []):
        app.outputs.append(
            OutputConfig(
                name=str(item["name"]),
                kind=str(item["kind"]),
                session=_as_optional_str(item.get("session")),
                data_format=str(item.get("data_format", "rtcm")),
                host=item.get("host"),
                port=int(item["port"]) if item.get("port") is not None else None,
                path=item.get("path"),
                interval=_as_interval(item.get("interval"), "outputs.interval"),
                max_queue=int(item.get("max_queue", 512)),
                rinex=_as_rinex_export(item.get("rinex"), "outputs.rinex"),
            )
        )

    if not app.inputs:
        raise ConfigurationError("At least one input must be configured")

    _validate_config(app)
    return app
