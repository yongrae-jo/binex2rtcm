"""Validation helpers."""

from __future__ import annotations

import logging

from .errors import ValidationError

LOGGER = logging.getLogger(__name__)


class RtcmValidator:
    def __init__(self, enabled: bool = True) -> None:
        self._enabled = enabled
        self._parser = None
        if not enabled:
            return
        try:
            from pyrtcm import RTCMReader

            self._parser = RTCMReader
        except Exception:
            LOGGER.info("pyrtcm is not installed; RTCM parse validation disabled")
            self._enabled = False

    def validate(self, data: bytes) -> None:
        if not self._enabled or self._parser is None:
            return
        try:
            self._parser.parse(data)
        except Exception as exc:  # pragma: no cover - depends on optional dependency
            raise ValidationError(f"RTCM parse validation failed: {exc}") from exc
