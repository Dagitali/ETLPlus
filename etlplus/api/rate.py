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

    def _from_cfg(cfg: Mapping[str, Any] | None) -> float | None:
        if not cfg:
            return None

        # Prefer explicit positive sleep_seconds if numeric.
        if 'sleep_seconds' in cfg:
            try:
                ss = float(cfg['sleep_seconds'])
                if ss > 0:
                    return ss
            except (TypeError, ValueError):
                pass

        # Else derive from positive max_per_sec.
        if 'max_per_sec' in cfg:
            try:
                mps = float(cfg['max_per_sec'])
                if mps > 0:
                    return 1.0 / mps
            except (TypeError, ValueError):
                pass

        return None

    # Precedence: overrides > rate_limit
    value = _from_cfg(overrides)
    if value is None:
        value = _from_cfg(rate_limit)

    return float(value) if value is not None else 0.0
