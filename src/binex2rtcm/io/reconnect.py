"""Shared reconnect policy for network inputs."""

from __future__ import annotations

RECONNECT_FAILURE_COOLDOWN_THRESHOLD = 5
RECONNECT_COOLDOWN_DELAY_S = 3600.0


def next_reconnect_delay_s(base_delay_s: float, consecutive_failures: int) -> float:
    """Return the delay before the next reconnect attempt."""
    if consecutive_failures >= RECONNECT_FAILURE_COOLDOWN_THRESHOLD:
        return RECONNECT_COOLDOWN_DELAY_S
    return base_delay_s
