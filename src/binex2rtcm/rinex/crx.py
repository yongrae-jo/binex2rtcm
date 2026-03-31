"""CRX conversion helpers for observation RINEX files."""

from __future__ import annotations

import logging
import shutil
import subprocess
import sys
from pathlib import Path

LOGGER = logging.getLogger(__name__)
_RNX2CRX_CANDIDATE_NAMES = ("RNX2CRX", "rnx2crx")


def _candidate_tool_directories() -> list[Path]:
    directories: list[Path] = []
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            directories.append(parent / "tools")
            break
    directories.append(Path.cwd() / "tools")
    unique_directories: list[Path] = []
    seen: set[Path] = set()
    for directory in directories:
        resolved = directory.resolve(strict=False)
        if resolved in seen:
            continue
        seen.add(resolved)
        unique_directories.append(directory)
    return unique_directories


def _candidate_tool_names() -> tuple[str, ...]:
    if sys.platform.startswith("win"):
        names: list[str] = []
        for name in _RNX2CRX_CANDIDATE_NAMES:
            names.extend((name, f"{name}.exe"))
        return tuple(names)
    return _RNX2CRX_CANDIDATE_NAMES


def resolve_rnx2crx_binary() -> Path | None:
    for directory in _candidate_tool_directories():
        for name in _candidate_tool_names():
            candidate = directory / name
            if candidate.is_file():
                return candidate
    for name in _RNX2CRX_CANDIDATE_NAMES:
        resolved = shutil.which(name)
        if resolved:
            return Path(resolved)
    return None


def _resolve_crx_output_path(observation_rnx_path: Path) -> Path | None:
    candidates = (
        observation_rnx_path.with_suffix(".crx"),
        observation_rnx_path.with_suffix(".CRX"),
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def convert_observation_rnx_to_crx(observation_rnx_path: Path) -> Path | None:
    binary_path = resolve_rnx2crx_binary()
    if binary_path is None:
        LOGGER.warning(
            "RNX2CRX tool not found; leaving observation RINEX uncompressed: %s",
            observation_rnx_path,
        )
        return None

    command = [str(binary_path), "-f", str(observation_rnx_path)]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
    except OSError as exc:
        LOGGER.warning("failed to start RNX2CRX for %s: %s", observation_rnx_path, exc)
        return None

    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or f"exit code {completed.returncode}"
        LOGGER.warning("RNX2CRX failed for %s: %s", observation_rnx_path, detail)
        return None

    crx_path = _resolve_crx_output_path(observation_rnx_path)
    if crx_path is None:
        LOGGER.warning(
            "RNX2CRX finished without creating a CRX artifact for %s",
            observation_rnx_path,
        )
        return None
    if crx_path.suffix == ".CRX":
        normalized_path = observation_rnx_path.with_suffix(".crx")
        crx_path.replace(normalized_path)
        return normalized_path
    return crx_path
