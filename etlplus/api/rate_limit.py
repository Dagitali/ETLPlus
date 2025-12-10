"""Compatibility wrappers for historical rate-limit helpers.

The canonical implementation now lives in :mod:`etlplus.api.rate_limiter`. This
module keeps a tiny :class:`RateLimitConfig` shim plus convenience re-exports,
so existing imports continue to function.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .rate_limiter import RateLimitConfig
from .rate_limiter import RateLimitConfigMap
from .rate_limiter import RateLimiter
from .types import RateLimitOverrides

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'RateLimitConfig',
    'RateLimitConfigMap',
    'RateLimiter',
    'compute_sleep_seconds',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _as_mapping(
    rate_limit: Mapping[str, Any] | RateLimitConfig | None,
) -> RateLimitConfigMap | None:
    """
    Helper to normalize legacy inputs before delegating to request helpers.
    """
    if rate_limit is None:
        return None
    if isinstance(rate_limit, RateLimitConfig):
        mapping = rate_limit.as_mapping()
        return mapping or None
    if isinstance(rate_limit, Mapping):
        candidate = RateLimitConfig.from_obj(rate_limit)
        return candidate.as_mapping() if candidate else None
    return None


# SECTION: FUNCTIONS ======================================================== #


def compute_sleep_seconds(
    rate_limit: Mapping[str, Any] | RateLimitConfig | None = None,
    overrides: RateLimitOverrides = None,
) -> float:
    """
    Compute the per-request delay by delegating to :mod:`etlplus.api.request`.

    Parameters
    ----------
    rate_limit : Mapping[str, Any] | RateLimitConfig | None, optional
        Base rate-limit configuration, by default None.
    overrides : RateLimitOverrides, optional
        Override settings to apply atop the base config, by default None.

    Returns
    -------
    float
        Computed inter-request delay in seconds.
    """
    normalized = _as_mapping(rate_limit)
    return RateLimiter.resolve_sleep_seconds(
        rate_limit=normalized,
        overrides=overrides,
    )


# Re-export "public" helpers for callers previously importing from here.
compute_sleep_seconds.__doc__ = (
    RateLimiter.resolve_sleep_seconds.__doc__
    or compute_sleep_seconds.__doc__
)
