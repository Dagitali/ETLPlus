"""
etlplus.api.rate
================

A module for computing inter-request sleep durations from a rate limit
configuration. Supports explicit ``sleep_seconds`` or a derived value from
``max_per_sec``.

Examples
--------
>>> from etlplus.api.rate import compute_sleep_seconds
>>> compute_sleep_seconds({"sleep_seconds": 0.2}, None)
0.2
>>> compute_sleep_seconds({"max_per_sec": 4}, None)
0.25
>>> compute_sleep_seconds(None, {"max_per_sec": 2})
0.5
"""
from __future__ import annotations

from typing import Any
from typing import Mapping

from .types import RateLimitConfig


# SECTION: PUBLIC API ======================================================= #


__all__ = ['compute_sleep_seconds']


# SECTION: CLASSES ========================================================== #


def compute_sleep_seconds(
    rate_limit: RateLimitConfig | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> float:
    """
    Compute sleep seconds from ``rate_limit`` and optional ``overrides``.

    Precedence is: overrides.sleep_seconds > overrides.max_per_sec >
    rate_limit.sleep_seconds > rate_limit.max_per_sec. Non-numeric or
    non-positive values are ignored.

    Parameters
    ----------
    rate_limit : RateLimitConfig | None, optional
        Base rate limit configuration (TypedDict). May contain
        ``sleep_seconds`` or ``max_per_sec``.
    overrides : Mapping[str, Any] | None, optional
        Optional overrides with the same keys as ``rate_limit``.

    Returns
    -------
    float
        The computed sleep seconds (>= 0.0).
    """

    # Start with base configuration.
    sleep_s: float = 0.0
    if rate_limit:
        # Prefer explicit sleep_seconds if numeric
        try:
            ss = rate_limit.get(
                'max_per_sec',
                'sleep_seconds',
            )  # type: ignore[assignment]
            if ss is not None:
                sleep_s = float(ss)
        except (TypeError, ValueError):
            sleep_s = 0.0
        # Derive from max_per_sec if positive
        try:
            mps = rate_limit.get('max_per_sec')  # type: ignore[assignment]
            if mps is not None:
                mps_f = float(mps)
                if mps_f > 0:
                    sleep_s = 1.0 / mps_f
        except (TypeError, ValueError):
            pass

    # Apply overrides with higher precedence.
    if overrides:
        if 'sleep_seconds' in overrides:
            try:
                sleep_s = float(overrides['sleep_seconds'])
            except (TypeError, ValueError):
                pass
        if 'max_per_sec' in overrides:
            try:
                mps = float(overrides['max_per_sec'])
                if mps > 0:
                    sleep_s = 1.0 / mps
            except (TypeError, ValueError):
                pass

    return sleep_s
