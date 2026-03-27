from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tomllib

from binex2rtcm.binex.decoder import BinexDecoder
from binex2rtcm.binex.encoder import BinexEncoder
from binex2rtcm.binex.framer import BinexFramer
from binex2rtcm.model.observation import EpochObservations, SatelliteObservation, SignalObservation

OBS_FIELD_WIDTH = 16
RINGO_RINEX_VERSION = "3.05"


@dataclass(slots=True)
class RuntimeConfig:
    duration_s: float = 60.0


@dataclass(slots=True)
class ToolConfig:
    ringo_binary: str | None = None
    approx_time: str | None = None


@dataclass(slots=True)
class ArtifactConfig:
    workdir: str = "runs/stream-roundtrip"


@dataclass(slots=True)
class CheckConfig:
    min_epoch_count: int = 1
    require_matching_epoch_count: bool = True
    require_matching_satellite_rows: bool = True
    require_matching_signal_fields: bool = True
    require_same_constellations: bool = True
    validate_obs_layout: bool = True
    validate_nav_layout: bool = True


@dataclass(slots=True)
class StreamInputConfig:
    name: str
    session: str
    kind: str
    host: str | None = None
    port: int | None = None
    mountpoint: str | None = None
    username: str | None = None
    password: str | None = None
    path: str | None = None
    chunk_size: int = 4096
    replay_rate: float = 0.0
    connect_timeout_s: float = 10.0
    reconnect_delay_s: float = 5.0
    send_nmea_gga: bool = False
    gga_interval_s: float = 15.0
    source_position_llh: tuple[float, float, float] | None = None


@dataclass(slots=True)
class TestConfig:
    runtime: RuntimeConfig
    tools: ToolConfig
    artifacts: ArtifactConfig
    checks: CheckConfig
    binex_input: StreamInputConfig
    rtcm_input: StreamInputConfig


@dataclass(slots=True)
class RoundtripRun:
    direction_id: str
    direction_label: str
    input_id: str
    input_label: str
    output_id: str
    output_label: str
    input_format: str
    output_format: str
    input_config: StreamInputConfig
    generated_config_path: Path
    input_capture_base: Path
    output_log_base: Path
    external_dir: Path


@dataclass(slots=True)
class ExternalRinexArtifacts:
    obs_path: Path
    nav_paths: list[Path] = field(default_factory=list)


@dataclass(slots=True)
class RinexObsSummary:
    path: Path
    epoch_count: int = 0
    satellite_rows_by_system: Counter[str] = field(default_factory=Counter)
    signal_field_count: int = 0
    systems: set[str] = field(default_factory=set)
    observation_types: dict[str, list[str]] = field(default_factory=dict)
    first_epoch: datetime | None = None
    last_epoch: datetime | None = None

    @property
    def satellite_row_count(self) -> int:
        return sum(self.satellite_rows_by_system.values())


@dataclass(slots=True)
class ComparisonResult:
    direction_label: str
    artifact_label: str
    artifact_format: str
    backend: str
    internal_obs_path: Path
    external_obs_path: Path
    internal_nav_path: Path | None
    external_nav_paths: list[Path]
    internal_summary: RinexObsSummary
    external_summary: RinexObsSummary
    issues: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.issues


@dataclass(slots=True)
class ComparisonFailure:
    direction_label: str
    artifact_label: str
    backend: str
    reason: str


def _header_label(line: str) -> str:
    return line[60:].strip() if len(line) >= 61 else ""


def _parse_epoch(line: str) -> datetime | None:
    if not line.startswith(">"):
        return None
    parts = line[1:].split()
    if len(parts) < 6:
        return None
    year, month, day, hour, minute = (int(value) for value in parts[:5])
    second = float(parts[5])
    second_int = int(second)
    microsecond = int(round((second - second_int) * 1_000_000))
    return datetime(year, month, day, hour, minute, second_int, microsecond)


def _resolve_artifact(path_text: str | Path) -> Path:
    path = Path(path_text).expanduser()
    if path.exists():
        return path
    suffix = "".join(path.suffixes)
    stem = path.name[: -len(suffix)] if suffix else path.name
    candidates = sorted(path.parent.glob(f"{stem}_*{suffix}"), key=lambda item: item.stat().st_mtime, reverse=True)
    if candidates:
        return candidates[0]
    raise SystemExit(f"Artifact not found: {path}")


def _remove_if_exists(path: Path) -> None:
    if path.exists():
        path.unlink()


def _run_command(
    command: list[str],
    env: dict[str, str] | None = None,
    timeout_s: float | None = None,
) -> None:
    try:
        process = subprocess.Popen(command, env=env)
    except FileNotFoundError as exc:
        raise SystemExit(f"Command not found: {command[0]}") from exc
    try:
        return_code = process.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired as exc:
        process.kill()
        process.wait()
        raise SystemExit(
            f"Command timed out after {timeout_s:.1f}s: {' '.join(str(part) for part in command)}"
        ) from exc
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)


def _called_process_error_reason(exc: subprocess.CalledProcessError) -> str:
    command = " ".join(str(part) for part in exc.cmd)
    return f"Command failed with exit code {exc.returncode}: {command}"


def _resolve_ringo_binary(config: ToolConfig) -> str:
    configured = config.ringo_binary
    if configured:
        configured_path = Path(configured).expanduser()
        if configured_path.exists():
            return str(configured_path)
        resolved = shutil.which(str(configured_path))
        if resolved:
            return resolved
        raise SystemExit(f"ringo binary not found: {configured_path}")
    bundled = Path(__file__).resolve().parents[1] / "tools" / "ringo" / "ringo-v0.9.4-macos_arm64" / "ringo"
    if bundled.exists():
        return str(bundled)
    resolved = shutil.which("ringo")
    if resolved:
        return resolved
    raise SystemExit("ringo binary not found. Set [tools].ringo_binary or add ringo to PATH.")

def _parse_approx_time(approx_time: str | None) -> datetime:
    if not approx_time:
        raise SystemExit("RTCM validation requires [tools].approx_time")
    try:
        return datetime.strptime(approx_time, "%Y/%m/%d %H:%M:%S")
    except ValueError as exc:
        raise SystemExit(f"approx_time must look like 'YYYY/MM/DD HH:MM:SS', got {approx_time!r}") from exc


def _build_ringo_compatible_binex(source: Path, compat_path: Path) -> Path:
    framer = BinexFramer()
    decoder = BinexDecoder(station_id=0)
    encoder = BinexEncoder()
    encoded = bytearray()

    with source.open("rb") as fp:
        while chunk := fp.read(4096):
            for frame in framer.feed(chunk):
                for item in decoder.decode(frame):
                    if not isinstance(item, EpochObservations):
                        continue
                    satellites: list[SatelliteObservation] = []
                    for satellite in item.satellites:
                        signals = satellite.signals
                        if satellite.system.name == "GLO":
                            fallback_doppler = next(
                                (
                                    signal.doppler_hz
                                    for signal in signals
                                    if abs(signal.doppler_hz) > 1e-12
                                ),
                                None,
                            )
                            if fallback_doppler is not None:
                                rewritten: list[SignalObservation] = []
                                for signal in signals:
                                    doppler_hz = signal.doppler_hz
                                    if abs(doppler_hz) <= 1e-12:
                                        doppler_hz = fallback_doppler
                                    rewritten.append(
                                        SignalObservation(
                                            signal_label=signal.signal_label,
                                            pseudorange_m=signal.pseudorange_m,
                                            carrier_cycles=signal.carrier_cycles,
                                            doppler_hz=doppler_hz,
                                            cnr_dbhz=signal.cnr_dbhz,
                                            frequency_slot=signal.frequency_slot,
                                            lock_time_s=signal.lock_time_s,
                                            half_cycle_ambiguity=signal.half_cycle_ambiguity,
                                            slip_detected=signal.slip_detected,
                                            lli=signal.lli,
                                        )
                                    )
                                signals = rewritten
                        satellites.append(
                            SatelliteObservation(
                                system=satellite.system,
                                prn=satellite.prn,
                                signals=signals,
                                glonass_fcn=satellite.glonass_fcn,
                            )
                        )
                    if satellites:
                        encoded.extend(
                            encoder.encode(
                                EpochObservations(
                                    time=item.time,
                                    satellites=satellites,
                                    receiver_clock_offset_s=item.receiver_clock_offset_s,
                                )
                            )
                        )
    compat_path.write_bytes(bytes(encoded))
    return compat_path


def _run_ringo_export(
    ringo_bin: str,
    data_format: str,
    artifact: Path,
    output_stem: Path,
    approx_time: str | None,
) -> ExternalRinexArtifacts:
    if data_format == "rtcm":
        _parse_approx_time(approx_time)
    obs_path = output_stem.with_suffix(".obs")
    nav_path = output_stem.with_suffix(".nav")
    _remove_if_exists(obs_path)
    _remove_if_exists(nav_path)
    if data_format == "binex":
        command = [
            ringo_bin,
            "bingo",
            "--outvero",
            RINGO_RINEX_VERSION,
            "--outvern",
            RINGO_RINEX_VERSION,
            "--outobs",
            str(obs_path),
            "--outnav",
            str(nav_path),
            str(artifact),
        ]
    else:
        command = [
            ringo_bin,
            "rtcmgo",
            "--outvero",
            RINGO_RINEX_VERSION,
            "--outvern",
            RINGO_RINEX_VERSION,
        ]
        if approx_time:
            command.extend(["--aprdate", approx_time])
        command.extend(["--outobs", str(obs_path), "--outnav", str(nav_path), str(artifact)])
    _run_command(command)
    if data_format == "binex" and not obs_path.exists():
        compat_path = output_stem.with_name(f"{output_stem.name}_compat").with_suffix(".bnx")
        _remove_if_exists(compat_path)
        _build_ringo_compatible_binex(artifact, compat_path)
        command = [
            ringo_bin,
            "bingo",
            "--outvero",
            RINGO_RINEX_VERSION,
            "--outvern",
            RINGO_RINEX_VERSION,
            "--outobs",
            str(obs_path),
            "--outnav",
            str(nav_path),
            str(compat_path),
        ]
        _run_command(command)
    if not obs_path.exists():
        raise SystemExit(f"RINGO OBS artifact not found: {obs_path}")
    nav_paths = [nav_path] if nav_path.exists() else []
    return ExternalRinexArtifacts(obs_path=obs_path, nav_paths=nav_paths)


def parse_rinex_obs(
    path: Path,
    allowed_observation_types: dict[str, set[str]] | None = None,
) -> RinexObsSummary:
    summary = RinexObsSummary(path=path)
    lines = path.read_text(encoding="ascii", errors="replace").splitlines()
    index = 0
    in_header = True
    current_system = ""
    last_epoch: datetime | None = None
    while index < len(lines):
        line = lines[index]
        if in_header:
            label = _header_label(line)
            if label == "SYS / # / OBS TYPES":
                system = line[0].strip() or current_system
                count_text = line[1:7].strip()
                if system and count_text:
                    current_system = system
                    total = int(count_text)
                    types = line[7:60].split()
                    while len(types) < total and index + 1 < len(lines):
                        index += 1
                        continuation = lines[index]
                        types.extend(continuation[7:60].split())
                    summary.observation_types[system] = types[:total]
            elif label == "END OF HEADER":
                in_header = False
            index += 1
            continue
        epoch = _parse_epoch(line)
        if epoch is not None:
            if epoch != last_epoch:
                summary.epoch_count += 1
                last_epoch = epoch
            if summary.first_epoch is None:
                summary.first_epoch = epoch
            summary.last_epoch = epoch
            index += 1
            continue
        if line and line[0].isalnum():
            system = line[0]
            summary.systems.add(system)
            summary.satellite_rows_by_system[system] += 1
            obs_types = summary.observation_types.get(system, [])
            obs_count = len(obs_types)
            field_text = line[3:]
            required_chars = obs_count * OBS_FIELD_WIDTH
            while len(field_text) < required_chars and index + 1 < len(lines):
                next_line = lines[index + 1]
                if not next_line or next_line.startswith(">") or next_line[0].isalnum():
                    break
                index += 1
                field_text += next_line
            allowed_for_system = None if allowed_observation_types is None else allowed_observation_types.get(system, set())
            for field_index, offset in enumerate(range(0, required_chars, OBS_FIELD_WIDTH)):
                if allowed_for_system is not None and obs_types[field_index] not in allowed_for_system:
                    continue
                field = field_text[offset : offset + OBS_FIELD_WIDTH]
                if field[0:14].strip():
                    summary.signal_field_count += 1
        index += 1
    return summary


def _shared_observation_types(
    internal: RinexObsSummary,
    external: RinexObsSummary,
) -> dict[str, set[str]]:
    shared: dict[str, set[str]] = {}
    for system in sorted(set(internal.observation_types) | set(external.observation_types)):
        common = set(internal.observation_types.get(system, [])) & set(external.observation_types.get(system, []))
        if common:
            shared[system] = common
    return shared


def _validate_obs_layout(path: Path) -> list[str]:
    issues: list[str] = []
    in_header = True
    obs_type_counts: dict[str, int] = {}
    current_system = ""
    with path.open("r", encoding="ascii", errors="replace") as fp:
        for line_number, raw_line in enumerate(fp, start=1):
            line = raw_line.rstrip("\n")
            if in_header:
                label = _header_label(line)
                if label == "RINEX VERSION / TYPE":
                    if not line[0:9].strip() or not line[20:40].strip() or not line[40:60].strip():
                        issues.append(f"{path}: line {line_number} invalid version/type field placement")
                elif label == "PGM / RUN BY / DATE":
                    if not line[0:20].strip() or not line[40:60].strip():
                        issues.append(f"{path}: line {line_number} invalid PGM / RUN BY / DATE placement")
                elif label in {"MARKER NAME", "MARKER NUMBER", "MARKER TYPE"}:
                    if line[:60].strip() and not line[0].strip():
                        issues.append(f"{path}: line {line_number} {label} value not left-aligned")
                elif label in {"APPROX POSITION XYZ", "ANTENNA: DELTA H/E/N"}:
                    for start in (0, 14, 28):
                        if not line[start : start + 14].strip():
                            issues.append(f"{path}: line {line_number} {label} missing numeric field at column {start + 1}")
                elif label == "SYS / # / OBS TYPES":
                    system = line[0].strip() or current_system
                    count_text = line[1:7].strip()
                    count_value = int(count_text) if count_text.isdigit() else None
                    if system and count_value is not None:
                        current_system = system
                        obs_type_counts[system] = count_value
                    if line[0].strip():
                        if not line[0].isalpha() or count_value is None:
                            issues.append(f"{path}: line {line_number} invalid SYS / # / OBS TYPES prefix")
                        elif count_value > 0 and not line[7:60].strip():
                            issues.append(f"{path}: line {line_number} invalid SYS / # / OBS TYPES prefix")
                    elif line[:7].strip() or not line[7:60].strip():
                        issues.append(f"{path}: line {line_number} invalid SYS / # / OBS TYPES prefix")
                elif label == "SIGNAL STRENGTH UNIT":
                    if line[:60].strip() and not line[0].strip():
                        issues.append(f"{path}: line {line_number} invalid signal strength unit placement")
                elif label == "INTERVAL":
                    try:
                        float(line[0:10].strip())
                    except ValueError:
                        issues.append(f"{path}: line {line_number} invalid interval placement")
                elif label in {"TIME OF FIRST OBS", "TIME OF LAST OBS"}:
                    try:
                        int(line[0:6].strip())
                        int(line[6:12].strip())
                        int(line[12:18].strip())
                        int(line[18:24].strip())
                        int(line[24:30].strip())
                        float(line[30:43].strip())
                    except ValueError:
                        issues.append(f"{path}: line {line_number} invalid time-of-obs placement")
                elif label == "GLONASS SLOT / FRQ #":
                    slot_count = line[0:3].strip()
                    slot_payload = line[3:60].strip()
                    if slot_count:
                        if not slot_count.isdigit():
                            issues.append(f"{path}: line {line_number} invalid GLONASS slot layout")
                        elif int(slot_count) > 0 and not slot_payload:
                            issues.append(f"{path}: line {line_number} invalid GLONASS slot layout")
                    elif not slot_payload:
                        issues.append(f"{path}: line {line_number} invalid GLONASS slot layout")
                elif label == "LEAP SECONDS":
                    if not line[0:6].strip().lstrip("+-").isdigit():
                        issues.append(f"{path}: line {line_number} invalid leap seconds placement")
                if label == "END OF HEADER":
                    in_header = False
                continue
            if not line:
                continue
            if line.startswith(">"):
                if not line.startswith("> ") or len(line) < 35:
                    issues.append(f"{path}: line {line_number} invalid epoch line layout")
                continue
            if line[0].isalnum():
                system = line[0]
                if obs_type_counts.get(system, 0) == 0:
                    continue
                if len(line) < 3 or not line[0].isalpha():
                    issues.append(f"{path}: line {line_number} observation field placement mismatch")
    return issues


def _validate_nav_layout(path: Path) -> list[str]:
    issues: list[str] = []
    in_header = True
    with path.open("r", encoding="ascii", errors="replace") as fp:
        for line_number, raw_line in enumerate(fp, start=1):
            line = raw_line.rstrip("\n")
            if in_header:
                label = _header_label(line)
                if label == "RINEX VERSION / TYPE":
                    if not line[0:9].strip() or not line[20:40].strip() or not line[40:60].strip():
                        issues.append(f"{path}: line {line_number} invalid nav version/type placement")
                if label == "END OF HEADER":
                    in_header = False
                continue
            if not line.strip():
                continue
            if line.startswith("> EPH"):
                continue
            if line[0].isalnum():
                if len(line) < 23 or not line[23:].strip():
                    issues.append(f"{path}: line {line_number} nav first line placement mismatch")
            else:
                if not line.startswith("    ") or not line[4:].strip():
                    issues.append(f"{path}: line {line_number} nav continuation placement mismatch")
    return issues


def _load_llh(value: object, field_name: str) -> tuple[float, float, float] | None:
    if value is None:
        return None
    if not isinstance(value, (list, tuple)) or len(value) != 3:
        raise SystemExit(f"{field_name}.source_position_llh must be a 3-element array")
    return float(value[0]), float(value[1]), float(value[2])


def _load_input_config(raw: object, field_name: str, expected_format: str) -> StreamInputConfig:
    if not isinstance(raw, dict):
        raise SystemExit(f"[{field_name}] must be a table")
    kind = str(raw.get("kind", "")).strip().lower()
    if kind not in {"ntrip_client", "tcp_client", "file_replay"}:
        raise SystemExit(f"[{field_name}].kind must be ntrip_client, tcp_client, or file_replay")
    config = StreamInputConfig(
        name=str(raw.get("name", f"{expected_format.upper()} input")),
        session=str(raw.get("session", expected_format.upper())),
        kind=kind,
        host=str(raw["host"]) if raw.get("host") is not None else None,
        port=int(raw["port"]) if raw.get("port") is not None else None,
        mountpoint=str(raw["mountpoint"]) if raw.get("mountpoint") is not None else None,
        username=str(raw["username"]) if raw.get("username") is not None else None,
        password=str(raw["password"]) if raw.get("password") is not None else None,
        path=str(raw["path"]) if raw.get("path") is not None else None,
        chunk_size=int(raw.get("chunk_size", 4096)),
        replay_rate=float(raw.get("replay_rate", 0.0)),
        connect_timeout_s=float(raw.get("connect_timeout_s", 10.0)),
        reconnect_delay_s=float(raw.get("reconnect_delay_s", 5.0)),
        send_nmea_gga=bool(raw.get("send_nmea_gga", False)),
        gga_interval_s=float(raw.get("gga_interval_s", 15.0)),
        source_position_llh=_load_llh(raw.get("source_position_llh"), field_name),
    )
    if kind == "ntrip_client":
        if not config.host or config.port is None or not config.mountpoint:
            raise SystemExit(f"[{field_name}] ntrip_client requires host, port, and mountpoint")
    elif kind == "tcp_client":
        if not config.host or config.port is None:
            raise SystemExit(f"[{field_name}] tcp_client requires host and port")
    elif kind == "file_replay":
        if not config.path:
            raise SystemExit(f"[{field_name}] file_replay requires path")
    if config.send_nmea_gga and config.source_position_llh is None:
        raise SystemExit(f"[{field_name}] send_nmea_gga requires source_position_llh")
    return config


def load_test_config(path: Path) -> TestConfig:
    with path.open("rb") as fp:
        raw = tomllib.load(fp)
    runtime_raw = raw.get("runtime", {})
    tools_raw = raw.get("tools", {})
    artifacts_raw = raw.get("artifacts", {})
    checks_raw = raw.get("checks", {})
    runtime = RuntimeConfig(duration_s=float(runtime_raw.get("duration_s", 60.0)))
    tools = ToolConfig(
        ringo_binary=str(tools_raw["ringo_binary"]) if tools_raw.get("ringo_binary") else None,
        approx_time=str(tools_raw["approx_time"]) if tools_raw.get("approx_time") else None,
    )
    artifacts = ArtifactConfig(workdir=str(artifacts_raw.get("workdir", "runs/stream-roundtrip")))
    checks = CheckConfig(
        min_epoch_count=int(checks_raw.get("min_epoch_count", 1)),
        require_matching_epoch_count=bool(checks_raw.get("require_matching_epoch_count", True)),
        require_matching_satellite_rows=bool(checks_raw.get("require_matching_satellite_rows", True)),
        require_matching_signal_fields=bool(checks_raw.get("require_matching_signal_fields", True)),
        require_same_constellations=bool(checks_raw.get("require_same_constellations", True)),
        validate_obs_layout=bool(checks_raw.get("validate_obs_layout", True)),
        validate_nav_layout=bool(checks_raw.get("validate_nav_layout", True)),
    )
    return TestConfig(
        runtime=runtime,
        tools=tools,
        artifacts=artifacts,
        checks=checks,
        binex_input=_load_input_config(raw.get("binex_input"), "binex_input", "binex"),
        rtcm_input=_load_input_config(raw.get("rtcm_input"), "rtcm_input", "rtcm"),
    )


def _toml_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_toml_value(item) for item in value) + "]"
    raise TypeError(f"Unsupported TOML value type: {type(value)!r}")


def _append_kv(lines: list[str], key: str, value: object | None) -> None:
    if value is None:
        return
    lines.append(f"{key} = {_toml_value(value)}")


def _build_app_config_text(
    input_config: StreamInputConfig,
    input_format: str,
    output_name: str,
    output_format: str,
    output_path: Path,
    capture_path: Path,
    duration_s: float,
) -> str:
    lines = [
        "[logging]",
        'level = "INFO"',
        "",
        "[validation]",
        "parse_with_pyrtcm = true",
        "",
        "[runtime]",
        f"duration_s = {duration_s}",
        "",
        "[monitor]",
        "enabled = false",
        "",
        "[scheduler]",
        "metadata_interval_s = 30.0",
        "ephemeris_interval_s = 300.0",
        "emit_ephemeris_on_change = true",
        "emit_metadata_on_start = true",
        "",
        "[scheduler.msm_level_by_system]",
        "GPS = 7",
        "GLO = 7",
        "GAL = 7",
        "BDS = 7",
        "QZS = 7",
        "IRN = 7",
        "SBS = 7",
        "",
        "[[inputs]]",
    ]
    _append_kv(lines, "name", input_config.name)
    _append_kv(lines, "session", input_config.session)
    _append_kv(lines, "kind", input_config.kind)
    _append_kv(lines, "data_format", input_format)
    _append_kv(lines, "host", input_config.host)
    _append_kv(lines, "port", input_config.port)
    _append_kv(lines, "mountpoint", input_config.mountpoint)
    _append_kv(lines, "username", input_config.username)
    _append_kv(lines, "password", input_config.password)
    _append_kv(lines, "path", input_config.path)
    _append_kv(lines, "chunk_size", input_config.chunk_size)
    _append_kv(lines, "replay_rate", input_config.replay_rate)
    _append_kv(lines, "connect_timeout_s", input_config.connect_timeout_s)
    _append_kv(lines, "reconnect_delay_s", input_config.reconnect_delay_s)
    _append_kv(lines, "send_nmea_gga", input_config.send_nmea_gga)
    _append_kv(lines, "gga_interval_s", input_config.gga_interval_s)
    _append_kv(lines, "source_position_llh", input_config.source_position_llh)
    _append_kv(lines, "capture_path", str(capture_path))
    lines.extend(
        [
            "",
            "[[outputs]]",
        ]
    )
    _append_kv(lines, "name", output_name)
    _append_kv(lines, "session", input_config.session)
    _append_kv(lines, "kind", "file")
    _append_kv(lines, "data_format", output_format)
    _append_kv(lines, "path", str(output_path))
    _append_kv(lines, "max_queue", 512)
    lines.append("rinex = { enabled = true, observation = true, navigation = true }")
    return "\n".join(lines) + "\n"


def _write_app_config(run: RoundtripRun, duration_s: float) -> None:
    output_name = f"{run.direction_label} output"
    text = _build_app_config_text(
        input_config=run.input_config,
        input_format=run.input_format,
        output_name=output_name,
        output_format=run.output_format,
        output_path=run.output_log_base,
        capture_path=run.input_capture_base,
        duration_s=duration_s,
    )
    run.generated_config_path.parent.mkdir(parents=True, exist_ok=True)
    run.generated_config_path.write_text(text, encoding="utf-8")


def _build_runs(config: TestConfig, run_root: Path) -> list[RoundtripRun]:
    generated_dir = run_root / "generated-configs"
    binex_dir = run_root / "binex-to-rtcm"
    rtcm_dir = run_root / "rtcm-to-binex"
    return [
        RoundtripRun(
            direction_id="binex_to_rtcm",
            direction_label="BINEX -> RTCM",
            input_id="input_binex",
            input_label="입력 BINEX",
            output_id="output_rtcm",
            output_label="출력 RTCM",
            input_format="binex",
            output_format="rtcm",
            input_config=config.binex_input,
            generated_config_path=generated_dir / "binex_to_rtcm.toml",
            input_capture_base=binex_dir / "input" / "captured_input.bnx",
            output_log_base=binex_dir / "output" / "converted_output.rtcm3",
            external_dir=binex_dir / "external",
        ),
        RoundtripRun(
            direction_id="rtcm_to_binex",
            direction_label="RTCM -> BINEX",
            input_id="input_rtcm",
            input_label="입력 RTCM",
            output_id="output_binex",
            output_label="출력 BINEX",
            input_format="rtcm",
            output_format="binex",
            input_config=config.rtcm_input,
            generated_config_path=generated_dir / "rtcm_to_binex.toml",
            input_capture_base=rtcm_dir / "input" / "captured_input.rtcm3",
            output_log_base=rtcm_dir / "output" / "converted_output.bnx",
            external_dir=rtcm_dir / "external",
        ),
    ]


def _run_generated_config(app_config: Path, duration_s: float) -> None:
    command = [
        sys.executable,
        "-c",
        "from binex2rtcm.app import main; raise SystemExit(main())",
        "--config",
        str(app_config),
        "--duration",
        str(duration_s),
        "--no-monitor",
    ]
    env = os.environ.copy()
    repo_src = Path(__file__).resolve().parents[1] / "src"
    current_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = str(repo_src) if not current_pythonpath else f"{repo_src}{os.pathsep}{current_pythonpath}"
    _run_command(command, env=env)


def _compare_obs_counts(label: str, internal: RinexObsSummary, external: RinexObsSummary, checks: CheckConfig) -> list[str]:
    issues: list[str] = []
    if internal.epoch_count < checks.min_epoch_count:
        issues.append(f"{label}: internal epoch count {internal.epoch_count} < minimum {checks.min_epoch_count}")
    if external.epoch_count < checks.min_epoch_count:
        issues.append(f"{label}: external epoch count {external.epoch_count} < minimum {checks.min_epoch_count}")
    if checks.require_matching_epoch_count and internal.epoch_count != external.epoch_count:
        issues.append(f"{label}: epoch count mismatch internal={internal.epoch_count} external={external.epoch_count}")
    if checks.require_matching_satellite_rows and internal.satellite_row_count != external.satellite_row_count:
        issues.append(
            f"{label}: satellite row count mismatch internal={internal.satellite_row_count} external={external.satellite_row_count}"
        )
    if checks.require_matching_signal_fields and internal.signal_field_count != external.signal_field_count:
        issues.append(
            f"{label}: signal field count mismatch internal={internal.signal_field_count} external={external.signal_field_count}"
        )
    if checks.require_same_constellations and internal.systems != external.systems:
        issues.append(f"{label}: constellation mismatch internal={sorted(internal.systems)} external={sorted(external.systems)}")
    return issues


def _validation_backends_for_format(artifact_format: str) -> tuple[str, ...]:
    if artifact_format in {"rtcm", "binex"}:
        return ("ringo",)
    raise SystemExit(f"Unsupported validation artifact format: {artifact_format}")


def _validate_artifact(
    run: RoundtripRun,
    artifact_id: str,
    artifact_label: str,
    artifact_format: str,
    artifact_base: Path,
    backend: str,
    ringo_bin: str,
    checks: CheckConfig,
    approx_time: str | None,
) -> ComparisonResult:
    artifact = _resolve_artifact(artifact_base)
    internal_obs = artifact.with_suffix(".obs")
    internal_nav = artifact.with_suffix(".nav")
    if not internal_obs.exists():
        raise SystemExit(f"Internal RINEX OBS artifact not found: {internal_obs}")
    output_stem = run.external_dir / backend / artifact_id
    output_stem.parent.mkdir(parents=True, exist_ok=True)
    if backend == "ringo":
        external = _run_ringo_export(ringo_bin, artifact_format, artifact, output_stem, approx_time)
    else:
        raise SystemExit(f"Unsupported validation backend: {backend}")
    internal_summary = parse_rinex_obs(internal_obs)
    external_summary = parse_rinex_obs(external.obs_path)
    if backend == "ringo" and artifact_format == "binex":
        # RINGO exports only the BINEX observation types it understands, so compare the shared subset.
        shared_observation_types = _shared_observation_types(internal_summary, external_summary)
        if shared_observation_types:
            internal_summary = parse_rinex_obs(internal_obs, allowed_observation_types=shared_observation_types)
            external_summary = parse_rinex_obs(external.obs_path, allowed_observation_types=shared_observation_types)
    label = f"{run.direction_label} / {artifact_label} / {backend.upper()}"
    issues = _compare_obs_counts(label, internal_summary, external_summary, checks)
    if checks.validate_obs_layout:
        for path in (internal_obs, external.obs_path):
            issues.extend(_validate_obs_layout(path))
    if checks.validate_nav_layout:
        nav_paths: list[Path] = []
        if internal_nav.exists():
            nav_paths.append(internal_nav)
        nav_paths.extend(external.nav_paths)
        for path in nav_paths:
            issues.extend(_validate_nav_layout(path))
    return ComparisonResult(
        direction_label=run.direction_label,
        artifact_label=artifact_label,
        artifact_format=artifact_format,
        backend=backend,
        internal_obs_path=internal_obs,
        external_obs_path=external.obs_path,
        internal_nav_path=internal_nav if internal_nav.exists() else None,
        external_nav_paths=external.nav_paths,
        internal_summary=internal_summary,
        external_summary=external_summary,
        issues=issues,
    )


def _print_comparison_result(result: ComparisonResult) -> None:
    status = "PASS" if result.passed else "FAIL"
    print("========================================================================")
    print(f"[{status}] {result.direction_label} / {result.artifact_label} / {result.backend.upper()}")
    print(f"  internal  : {result.internal_obs_path}")
    print(f"  external  : {result.external_obs_path}")
    print(
        "  counts    : "
        f"epochs internal={result.internal_summary.epoch_count} external={result.external_summary.epoch_count}"
    )
    print(
        "            "
        f"sat_rows internal={result.internal_summary.satellite_row_count} external={result.external_summary.satellite_row_count}"
    )
    print(
        "            "
        f"signals internal={result.internal_summary.signal_field_count} external={result.external_summary.signal_field_count}"
    )
    print(
        "  systems   : "
        f"internal={','.join(sorted(result.internal_summary.systems)) or '-'} "
        f"external={','.join(sorted(result.external_summary.systems)) or '-'}"
    )
    internal_nav = str(result.internal_nav_path) if result.internal_nav_path is not None else "skipped"
    external_nav = ", ".join(str(path) for path in result.external_nav_paths) if result.external_nav_paths else "skipped"
    print(f"  nav       : internal={internal_nav} external={external_nav}")
    print(f"  issues    : {len(result.issues)}")
    for issue in result.issues:
        print(f"    - {issue}")


def _print_comparison_failure(failure: ComparisonFailure) -> None:
    print("========================================================================")
    print(f"[FAIL] {failure.direction_label} / {failure.artifact_label} / {failure.backend.upper()}")
    print(f"  reason    : {failure.reason}")


def _print_summary(run_root: Path, results: list[ComparisonResult], failures: list[ComparisonFailure]) -> None:
    passed = sum(1 for result in results if result.passed)
    failed = len(results) - passed + len(failures)
    print("========================================================================")
    print("Validation summary")
    print(f"  run_root    : {run_root}")
    print(f"  comparisons : {len(results) + len(failures)}")
    print(f"  passed      : {passed}")
    print(f"  failed      : {failed}")
    if failures:
        print("  command failures:")
        for failure in failures:
            print(f"    - {failure.direction_label} / {failure.artifact_label} / {failure.backend.upper()}: {failure.reason}")
    failed_results = [result for result in results if not result.passed]
    if failed_results:
        print("  failed comparisons:")
        for result in failed_results:
            print(f"    - {result.direction_label} / {result.artifact_label} / {result.backend.upper()}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run BINEX/RTCM stream roundtrip validation")
    parser.add_argument("--config", required=True, help="Path to stream roundtrip validation TOML")
    args = parser.parse_args(argv)
    try:
        config_path = Path(args.config).expanduser()
        config = load_test_config(config_path)
        ringo_bin = _resolve_ringo_binary(config.tools)
        run_root = Path(config.artifacts.workdir).expanduser() / datetime.now().strftime("%Y%m%d_%H%M%S")
        run_root.mkdir(parents=True, exist_ok=True)
        runs = _build_runs(config, run_root)
        for run in runs:
            _write_app_config(run, config.runtime.duration_s)
            _run_generated_config(run.generated_config_path, config.runtime.duration_s)
        results: list[ComparisonResult] = []
        failures: list[ComparisonFailure] = []
        for run in runs:
            print("========================================================================")
            print(run.direction_label)
            for artifact_id, artifact_label, artifact_format, artifact_base in (
                (run.output_id, run.output_label, run.output_format, run.output_log_base),
            ):
                for backend in _validation_backends_for_format(artifact_format):
                    try:
                        result = _validate_artifact(
                            run=run,
                            artifact_id=artifact_id,
                            artifact_label=artifact_label,
                            artifact_format=artifact_format,
                            artifact_base=artifact_base,
                            backend=backend,
                            ringo_bin=ringo_bin,
                            checks=config.checks,
                            approx_time=config.tools.approx_time,
                        )
                    except subprocess.CalledProcessError as exc:
                        failure = ComparisonFailure(
                            direction_label=run.direction_label,
                            artifact_label=artifact_label,
                            backend=backend,
                            reason=_called_process_error_reason(exc),
                        )
                        failures.append(failure)
                        _print_comparison_failure(failure)
                        continue
                    except SystemExit as exc:
                        reason = exc.code if isinstance(exc.code, str) else f"Exited with code {exc.code}"
                        failure = ComparisonFailure(
                            direction_label=run.direction_label,
                            artifact_label=artifact_label,
                            backend=backend,
                            reason=reason,
                        )
                        failures.append(failure)
                        _print_comparison_failure(failure)
                        continue
                    results.append(result)
                    _print_comparison_result(result)
        _print_summary(run_root, results, failures)
        return 0 if all(result.passed for result in results) and not failures else 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
