"""
etlplus.api.rate module.

Compute inter-request sleep durations from a rate limit configuration.

Summary
-------
Supports explicit ``sleep_seconds`` or deriving a delay from ``max_per_sec``.

Notes
-----
- Precedence is: overrides.sleep_seconds > overrides.max_per_sec >
    rate_limit.sleep_seconds > rate_limit.max_per_sec.
- Non-numeric or non-positive values are ignored; fallback is ``0.0``.

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

from .rate_limiter import RateLimiter
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

    # Precedence: overrides > rate_limit
    cfg = overrides if overrides else rate_limit
    limiter = RateLimiter.from_config(cfg or {})
    return limiter.sleep_seconds if limiter.enabled else 0.0
