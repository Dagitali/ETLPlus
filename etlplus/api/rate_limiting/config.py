"""
:mod:`etlplus.api.rate_limiting.config` module.

Rate limiting configuration primitives.

This module defines the lightweight, typed configuration objects used by
``etlplus.api.rate_limiting``. The configuration layer is intentionally
separated from the runtime :class:`RateLimiter` so that higher-level
configs can depend on it without pulling in runtime behavior.

Examples
--------
Build a configuration and normalize it into a mapping::

    from etlplus.api.rate_limiting import RateLimitConfig

    cfg = RateLimitConfig(sleep_seconds=0.5)
    as_mapping = cfg.as_mapping()
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import Self
from typing import TypedDict
from typing import overload

from ...mixins import BoundsWarningsMixin
from ...types import StrAnyMap
from ...utils import to_float
from ...utils import to_positive_float

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'RateLimitConfig',
    # Type Aliases
    'RateLimitOverrides',
    # Typed Dicts
    'RateLimitConfigMap',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_rate_limit_map(
    rate_limit: StrAnyMap | RateLimitConfig | None,
) -> RateLimitConfigMap | None:
    """
    Normalize user inputs into a :class:`RateLimitConfigMap`.

    This helper is the single entry point for converting loosely-typed
    configuration into the canonical mapping consumed by downstream
    helpers.

    Parameters
    ----------
    rate_limit : StrAnyMap | RateLimitConfig | None
        User-supplied rate-limit configuration.

    Returns
    -------
    RateLimitConfigMap | None
        Normalized mapping, or ``None`` if input couldn't be parsed.
    """
    if rate_limit is None:
        return None
    if isinstance(rate_limit, RateLimitConfig):
        mapping = rate_limit.as_mapping()
        return mapping or None
    if isinstance(rate_limit, Mapping):
        cfg = RateLimitConfig.from_obj(rate_limit)
        return cfg.as_mapping() if cfg else None
    return None


def _merge_rate_limit(
    rate_limit: StrAnyMap | None,
    overrides: RateLimitOverrides = None,
) -> dict[str, Any]:
    """
    Merge *rate_limit* and *overrides* honoring override precedence.

    Parameters
    ----------
    rate_limit : StrAnyMap | None
        Base rate-limit configuration.
    overrides : RateLimitOverrides, optional
        Override configuration with precedence over *rate_limit*.

    Returns
    -------
    dict[str, Any]
        Merged configuration with overrides applied.
    """
    merged: dict[str, Any] = {}
    if rate_limit:
        merged.update(rate_limit)
    if overrides:
        merged.update({k: v for k, v in overrides.items() if v is not None})
    return merged


def _normalized_rate_values(
    cfg: Mapping[str, Any] | None,
) -> tuple[float | None, float | None]:
    """
    Return sanitized ``(sleep_seconds, max_per_sec)`` pair.

    Parameters
    ----------
    cfg : Mapping[str, Any] | None
        Rate-limit configuration.

    Returns
    -------
    tuple[float | None, float | None]
        Normalized ``(sleep_seconds, max_per_sec)`` values.
    """
    if not cfg:
        return None, None
    return (
        to_positive_float(cfg.get('sleep_seconds')),
        to_positive_float(cfg.get('max_per_sec')),
    )


# SECTION: TYPED DICTS ====================================================== #


class RateLimitConfigMap(TypedDict, total=False):
    """
    Configuration mapping for HTTP request rate limits.

    All keys are optional and intended to be mutually exclusive, positive
    values.

    Attributes
    ----------
    sleep_seconds : float, optional
        Delay in seconds between requests.
    max_per_sec : float, optional
        Maximum requests per second.

    Examples
    --------
    >>> rl: RateLimitConfigMap = {'max_per_sec': 4}
    ... # sleep ~= 0.25s between calls
    """

    # -- Attributes -- #

    sleep_seconds: float
    max_per_sec: float


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
# @dataclass(frozen=True, kw_only=True, slots=True)
class RateLimitConfig(BoundsWarningsMixin):
    """
    Lightweight container for optional API request rate-limit settings.

    Attributes
    ----------
    sleep_seconds : float | None, optional
        Number of seconds to sleep between requests.
    max_per_sec : float | None, optional
        Maximum number of requests per second.
    """

    # -- Attributes -- #

    sleep_seconds: float | None = None
    max_per_sec: float | None = None

    # -- Properties -- #

    @property
    def enabled(self) -> bool:
        """
        Whether this configuration enables rate limiting.

        The configuration is considered enabled when either
        ``sleep_seconds`` or ``max_per_sec`` contains a positive,
        numeric value after coercion.
        """
        sleep, per_sec = _normalized_rate_values(self.as_mapping())
        return sleep is not None or per_sec is not None

    # -- Instance Methods -- #

    def as_mapping(self) -> RateLimitConfigMap:
        """Return a normalized mapping consumable by rate-limit helpers."""
        cfg: RateLimitConfigMap = {}
        if (sleep := to_float(self.sleep_seconds)) is not None:
            cfg['sleep_seconds'] = sleep
        if (rate := to_float(self.max_per_sec)) is not None:
            cfg['max_per_sec'] = rate
        return cfg

    def validate_bounds(self) -> list[str]:
        """Return human-readable warnings for suspicious numeric bounds."""
        warnings: list[str] = []
        self._warn_if(
            (sleep := to_float(self.sleep_seconds)) is not None and sleep < 0,
            'sleep_seconds should be >= 0',
            warnings,
        )
        self._warn_if(
            (rate := to_float(self.max_per_sec)) is not None and rate <= 0,
            'max_per_sec should be > 0',
            warnings,
        )
        return warnings

    # -- Class Methods -- #

    @classmethod
    def from_defaults(
        cls,
        obj: StrAnyMap | None,
    ) -> Self | None:
        """
        Parse default rate-limit mapping, returning ``None`` if empty.

        Only supports ``sleep_seconds`` and ``max_per_sec`` keys. Other keys
        are ignored.

        Parameters
        ----------
        obj : StrAnyMap | None
            Defaults mapping (non-mapping inputs return ``None``).

        Returns
        -------
        Self | None
            Parsed instance with numeric fields coerced, or ``None`` if no
            relevant keys are present or parsing failed.
        """
        if not isinstance(obj, Mapping):
            return None

        sleep_seconds = obj.get('sleep_seconds')
        max_per_sec = obj.get('max_per_sec')

        if sleep_seconds is None and max_per_sec is None:
            return None

        return cls(
            sleep_seconds=to_float(sleep_seconds),
            max_per_sec=to_float(max_per_sec),
        )

    @classmethod
    def from_inputs(
        cls,
        *,
        rate_limit: StrAnyMap | RateLimitConfig | None = None,
        overrides: RateLimitOverrides = None,
    ) -> Self:
        """
        Normalize rate-limit config and overrides into a single instance.

        Parameters
        ----------
        rate_limit : StrAnyMap | RateLimitConfig | None, optional
            Base rate-limit configuration to normalize.
        overrides : RateLimitOverrides, optional
            Override values that take precedence over *rate_limit*.

        Returns
        -------
        Self
            Normalized rate-limit configuration.
        """
        normalized = _coerce_rate_limit_map(rate_limit)
        cfg = _merge_rate_limit(normalized, overrides)
        sleep, max_per_sec = _normalized_rate_values(cfg)
        if sleep is not None:
            return cls(sleep_seconds=sleep, max_per_sec=1.0 / sleep)
        if max_per_sec is not None:
            delay = 1.0 / max_per_sec
            return cls(sleep_seconds=delay, max_per_sec=max_per_sec)
        return cls()

    @classmethod
    @overload
    def from_obj(
        cls,
        obj: None,
    ) -> None: ...

    @classmethod
    @overload
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self: ...

    @classmethod
    @overload
    def from_obj(
        cls,
        obj: RateLimitConfigMap,
    ) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap | RateLimitConfig | None,
    ) -> Self | None:
        """
        Parse a mapping or existing config into a :class:`RateLimitConfig`
        instance.

        Parameters
        ----------
        obj : StrAnyMap | RateLimitConfig | None
            Existing config instance or mapping with optional
            rate-limit fields, or ``None``.

        Returns
        -------
        Self | None
            Parsed instance, or ``None`` if *obj* isn't a mapping.
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


# SECTION: TYPE ALIASES ===================================================== #


# Common input type accepted by helpers and runtime utilities.
type RateLimitInput = StrAnyMap | RateLimitConfig | None

# Optional mapping of rate-limit fields to override values.
type RateLimitOverrides = RateLimitConfigMap | None
