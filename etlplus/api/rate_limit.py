"""Compatibility wrappers for historical rate-limit helpers.

The canonical implementation now lives in :mod:`etlplus.api.request`. This
module keeps a tiny :class:`RateLimitConfig` shim plus convenience re-exports,
so existing imports continue to function.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ..utils import to_float
from .rate_limiter import RateLimitConfigMap
from .rate_limiter import RateLimiter
from .rate_limiter import compute_sleep_seconds as _compute_sleep_seconds

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'RateLimitConfig',
    'RateLimitConfigMap',
    'RateLimiter',
    'compute_sleep_seconds',
]


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class RateLimitConfig:
    """Lightweight container for optional rate-limit settings."""

    sleep_seconds: float | int | None = None
    max_per_sec: float | int | None = None

    # -- Instance Methods -- #

    def as_mapping(self) -> RateLimitConfigMap:
        """
        Return a normalized mapping consumable by :mod:`etlplus.api.request`.

        Drops any unset or invalid values.

        Returns
        -------
        RateLimitConfigMap
            Mapping with normalized numeric values.
        """
        cfg: RateLimitConfigMap = {}
        if (sleep := to_float(self.sleep_seconds)) is not None:
            cfg['sleep_seconds'] = sleep
        if (rate := to_float(self.max_per_sec)) is not None:
            cfg['max_per_sec'] = rate
        return cfg

    def validate_bounds(self) -> list[str]:
        """Return human-readable warnings for suspicious numeric bounds."""
        warnings: list[str] = []
        if (sleep := to_float(self.sleep_seconds)) is not None and sleep < 0:
            warnings.append('sleep_seconds should be >= 0')
        if (rate := to_float(self.max_per_sec)) is not None and rate <= 0:
            warnings.append('max_per_sec should be > 0')
        return warnings

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: Mapping[str, Any] | RateLimitConfig | None,
    ) -> RateLimitConfig | None:
        """
        Parse mappings or existing configs into a :class:`RateLimitConfig`.

        Parameters
        ----------
        obj : Mapping[str, Any] | RateLimitConfig | None
            Input configuration to parse.

        Returns
        -------
        RateLimitConfig | None
            Parsed configuration instance, or ``None`` if input is invalid.
        """
        if obj is None:
            return None
        if isinstance(obj, cls):
            return obj
        if not isinstance(obj, Mapping):
            return None
        return cls(
            sleep_seconds=to_float(obj.get('sleep_seconds')),
            max_per_sec=to_float(obj.get('max_per_sec')),
        )


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
    overrides: Mapping[str, Any] | None = None,
) -> float:
    """
    Compute the per-request delay by delegating to :mod:`etlplus.api.request`.

    Parameters
    ----------
    rate_limit : Mapping[str, Any] | RateLimitConfig | None, optional
        Base rate-limit configuration, by default None.
    overrides : Mapping[str, Any] | None, optional
        Override settings to apply atop the base config, by default None.

    Returns
    -------
    float
        Computed inter-request delay in seconds.
    """
    normalized = _as_mapping(rate_limit)
    return _compute_sleep_seconds(normalized, overrides)


# Re-export "public" helpers for callers previously importing from here.
compute_sleep_seconds.__doc__ = (
    _compute_sleep_seconds.__doc__ or compute_sleep_seconds.__doc__
)
