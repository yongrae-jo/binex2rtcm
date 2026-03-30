"""Logging helpers."""

from __future__ import annotations

from datetime import datetime
import logging
from pathlib import Path
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import InputConfig


def configure_logging(level: str = "INFO") -> None:
    """Configure a single process-wide structured-ish logger."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


def _safe_log_name(value: str) -> str:
    safe = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value.strip())
    return safe or "default"


def input_error_log_path(config: "InputConfig") -> Path:
    session_name = _safe_log_name(config.session or config.name or "default")
    return Path("runs") / session_name / "log" / f"{session_name}.error.log"


def append_input_error(config: "InputConfig", level: str, message: str) -> Path:
    path = input_error_log_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    with path.open("a", encoding="utf-8") as fp:
        fp.write(f"{timestamp} {level.upper()} {message}\n")
    return path
