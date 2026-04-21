"""Shared reconnect policy for network inputs."""

from __future__ import annotations

from dataclasses import dataclass

RECONNECT_FAILURE_COOLDOWN_THRESHOLD = 5
RECONNECT_COOLDOWN_DELAY_S = 3600.0


@dataclass(frozen=True)
class ReconnectDecision:
    """Reconnect wait decision for the current failure streak."""

    failure_count: int
    delay_s: float
    cooldown_active: bool


def next_reconnect_delay_s(base_delay_s: float, consecutive_failures: int) -> float:
    """Return the delay before the next reconnect attempt."""
    if consecutive_failures >= RECONNECT_FAILURE_COOLDOWN_THRESHOLD:
        return RECONNECT_COOLDOWN_DELAY_S
    return base_delay_s


def plan_reconnect(base_delay_s: float, consecutive_failures: int) -> ReconnectDecision:
    """Increment the failure streak and return the wait decision."""
    failure_count = consecutive_failures + 1
    return ReconnectDecision(
        failure_count=failure_count,
        delay_s=next_reconnect_delay_s(base_delay_s, failure_count),
        cooldown_active=failure_count >= RECONNECT_FAILURE_COOLDOWN_THRESHOLD,
    )


def reset_failure_count_after_wait(decision: ReconnectDecision) -> int:
    """Restart the burst retry window after a cooldown wait completes."""
    if decision.cooldown_active:
        return 0
    return decision.failure_count
