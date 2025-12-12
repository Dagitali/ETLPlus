"""
:mod:`etlplus.api.rate_limiter` module.

Centralized logic for limiting HTTP request rates.

Examples
--------
Create a limiter from static configuration and apply it before each
request:

    cfg = {"max_per_sec": 5}
    limiter = RateLimiter.from_config(cfg)

    for payload in batch:
        limiter.enforce()
        client.send(payload)
"""
from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import TypedDict

from ..config.mixins import BoundsWarningsMixin
from ..utils import to_float
from ..utils import to_positive_float
from .types import RateLimitOverrides

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RateLimitConfig',
    'RateLimitPlan',
    'RateLimiter',

    # Typed Dicts
    'RateLimitConfigMap',
]


# SECTION: TYPED DICTS ====================================================== #


class RateLimitConfigMap(TypedDict, total=False):
    """
    Configuration mapping for limiting HTTP request rates.

    All keys are optional and intended to be mutually exclusive, positive
    values.

    Attributes
    ----------
    sleep_seconds : float | int, optional
        Number of seconds to sleep between requests.
    max_per_sec : float | int, optional
        Maximum requests per second.

    Examples
    --------
    >>> rl: RateLimitConfigMap = {'max_per_sec': 4}
    ... # sleep ~= 0.25s between calls
    """

    # -- Attributes -- #

    sleep_seconds: float | int
    max_per_sec: float | int


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_rate_limit_map(
    rate_limit: Mapping[str, Any] | RateLimitConfig | None,
) -> RateLimitConfigMap | None:
    """Normalize legacy inputs into a concrete mapping."""
    if rate_limit is None:
        return None
    if isinstance(rate_limit, RateLimitConfig):
        mapping = rate_limit.as_mapping()
        return mapping or None
    if isinstance(rate_limit, Mapping):
        candidate = RateLimitConfig.from_obj(rate_limit)
        return candidate.as_mapping() if candidate else None
    return None


def _merge_rate_limit(
    rate_limit: Mapping[str, Any] | None,
    overrides: RateLimitOverrides = None,
) -> dict[str, Any]:
    """
    Merge ``rate_limit`` and ``overrides`` honoring override precedence.

    Parameters
    ----------
    rate_limit : Mapping[str, Any] | None
        Base rate-limit configuration.
    overrides : RateLimitOverrides, optional
        Override configuration with precedence over ``rate_limit``.

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


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True)
class RateLimitConfig(BoundsWarningsMixin):
    """Lightweight container for optional rate-limit settings."""

    sleep_seconds: float | int | None = None
    max_per_sec: float | int | None = None

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
    def from_obj(
        cls,
        obj: Mapping[str, Any] | RateLimitConfig | None,
    ) -> RateLimitConfig | None:
        """Parse mappings or existing configs into :class:`RateLimitConfig`."""
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


@dataclass(slots=True, frozen=True)
class RateLimitPlan:
    """Canonical, normalized view of rate-limit inputs."""

    # -- Attributes -- #

    sleep_seconds: float = 0.0
    max_per_sec: float | None = None

    # -- Properties -- #

    @property
    def enabled(self) -> bool:
        """Whether this plan enforces a delay."""
        return self.sleep_seconds > 0

    # -- Class Methods -- #

    @classmethod
    def from_inputs(
        cls,
        *,
        rate_limit: Mapping[str, Any] | RateLimitConfig | None = None,
        overrides: RateLimitOverrides = None,
    ) -> RateLimitPlan:
        """
        Normalize user config and overrides into a single plan.
        """
        normalized = _coerce_rate_limit_map(rate_limit)
        cfg = _merge_rate_limit(normalized, overrides)
        sleep, per_sec = _normalized_rate_values(cfg)
        if sleep is not None:
            return cls(sleep_seconds=sleep, max_per_sec=1.0 / sleep)
        if per_sec is not None:
            delay = 1.0 / per_sec
            return cls(sleep_seconds=delay, max_per_sec=per_sec)
        return cls()


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True, kw_only=True)
class RateLimiter:
    """
    HTTP request rate limit manager.

    Parameters
    ----------
    sleep_seconds : float, optional
        Fixed delay between requests, in seconds. Defaults to ``0.0``.
    max_per_sec : float | None, optional
        Maximum requests-per-second rate. When positive, it is converted
        to a delay of ``1 / max_per_sec`` seconds between requests.
        Defaults to ``None``.

    Attributes
    ----------
    sleep_seconds : float
        Effective delay between requests, in seconds.
    max_per_sec : float | None
        Effective maximum requests-per-second rate, or ``None`` when
        rate limiting is disabled.
    """

    # -- Attributes -- #

    sleep_seconds: float = 0.0
    max_per_sec: float | None = None

    # -- Magic Methods (Object Lifecycle) -- #

    def __post_init__(self) -> None:
        """
        Normalize internal state and enforce invariants.

        The two attributes ``sleep_seconds`` and ``max_per_sec`` are kept
        consistent according to the following precedence:

        1. If ``sleep_seconds`` is positive, it is treated as canonical.
        2. Else if ``max_per_sec`` is positive, it is used to derive
            ``sleep_seconds``.
        3. Otherwise the limiter is disabled.
        """
        sleep = to_positive_float(self.sleep_seconds)
        rate = to_positive_float(self.max_per_sec)

        if sleep is not None:
            self.sleep_seconds = sleep
            self.max_per_sec = 1.0 / sleep
        elif rate is not None:
            self.max_per_sec = rate
            self.sleep_seconds = 1.0 / rate
        else:
            self.sleep_seconds = 0.0
            self.max_per_sec = None

    # -- Magic Methods (Object Representation) -- #

    def __bool__(self) -> bool:
        """
        Return whether the limiter is enabled.

        Returns
        -------
        bool
            ``True`` if the limiter currently applies a delay, ``False``
            otherwise.
        """
        return self.enabled

    # -- Getters -- #

    @property
    def enabled(self) -> bool:
        """
        Whether this limiter currently applies any delay.

        Returns
        -------
        bool
            ``True`` if ``sleep_seconds`` is positive, ``False`` otherwise.
        """
        return self.sleep_seconds > 0

    # -- Instance Methods -- #

    def enforce(self) -> None:
        """
        Apply rate limiting by sleeping if configured.

        Notes
        -----
        This method is a no-op when ``sleep_seconds`` is not positive.
        """
        if self.sleep_seconds > 0:
            time.sleep(self.sleep_seconds)

    # -- Class Methods -- #

    @classmethod
    def disabled(cls) -> RateLimiter:
        """
        Create a limiter that never sleeps.

        Returns
        -------
        RateLimiter
            Instance with rate limiting disabled.
        """
        return cls(sleep_seconds=0.0)

    @classmethod
    def fixed(
        cls,
        seconds: float,
    ) -> RateLimiter:
        """
        Create a limiter with a fixed non-negative delay.

        Parameters
        ----------
        seconds : float
            Desired delay between requests, in seconds. Negative values
            are treated as ``0.0``.

        Returns
        -------
        RateLimiter
            Instance with the specified delay.
        """
        value = to_float(seconds, 0.0, minimum=0.0) or 0.0

        return cls(sleep_seconds=value)

    @classmethod
    def from_config(
        cls,
        cfg: Mapping[str, Any] | RateLimitConfig | None,
    ) -> RateLimiter:
        """
        Build a :class:`RateLimiter` from a configuration mapping.

        The mapping may contain the following keys:

        - ``"sleep_seconds"``: positive number of seconds between requests.
        - ``"max_per_sec"``: positive requests-per-second rate, converted to
            a delay of ``1 / max_per_sec`` seconds between requests.

        If neither key is provided or all values are invalid or non-positive,
        the returned limiter has rate limiting disabled.

        Parameters
        ----------
        cfg : Mapping[str, Any] | RateLimitConfig | None
            Configuration mapping from which to derive rate-limit settings.

        Returns
        -------
        RateLimiter
            Instance with normalized ``sleep_seconds`` and ``max_per_sec``.
        """
        plan = RateLimitPlan.from_inputs(rate_limit=cfg)
        return cls(
            sleep_seconds=plan.sleep_seconds,
            max_per_sec=plan.max_per_sec,
        )

    @classmethod
    def resolve_sleep_seconds(
        cls,
        *,
        rate_limit: RateLimitConfigMap | RateLimitConfig | None,
        overrides: RateLimitOverrides = None,
    ) -> float:
        """
        Normalize the supplied mappings into a concrete delay.

        Precedence is:

        1. ``overrides["sleep_seconds"]``
        2. ``overrides["max_per_sec"]``
        3. ``rate_limit["sleep_seconds"]``
        4. ``rate_limit["max_per_sec"]``

        Non-numeric or non-positive values are ignored.

        Parameters
        ----------
        rate_limit : RateLimitConfigMap | RateLimitConfig | None
            Base rate-limit configuration. May contain ``"sleep_seconds"`` or
            ``"max_per_sec"``.
        overrides : RateLimitOverrides, optional
            Optional overrides with the same keys as ``rate_limit``.

        Returns
        -------
        float
            Normalized delay in seconds (always >= 0).

        Notes
        -----
        The returned value is always non-negative, even when the limiter is
        disabled.

        Examples
        --------
        >>> from etlplus.api.rate_limiter import RateLimiter
        >>> RateLimiter.resolve_sleep_seconds(
        ...     rate_limit={'max_per_sec': 5},
        ...     overrides={'sleep_seconds': 0.25},
        ... )
        0.25
        """
        plan = RateLimitPlan.from_inputs(
            rate_limit=rate_limit,
            overrides=overrides,
        )
        return plan.sleep_seconds if plan.enabled else 0.0
