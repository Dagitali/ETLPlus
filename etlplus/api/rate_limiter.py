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

from ..utils import to_float
from ..utils import to_positive_float
from .types import RateLimitOverrides

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RateLimiter',

    # Functions
    'compute_sleep_seconds',

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


# SECTION: FUNCTIONS ======================================================== #


def compute_sleep_seconds(
    rate_limit: RateLimitConfigMap | None = None,
    overrides: RateLimitOverrides = None,
) -> float:
    """
    Compute a delay from the provided configuration mappings.

    Precedence is:

    1. ``overrides["sleep_seconds"]``
    2. ``overrides["max_per_sec"]``
    3. ``rate_limit["sleep_seconds"]``
    4. ``rate_limit["max_per_sec"]``

    Non-numeric or non-positive values are ignored.

    Parameters
    ----------
    rate_limit : RateLimitConfigMap | None, optional
        Base rate-limit configuration. May contain ``"sleep_seconds"`` or
        ``"max_per_sec"``.
    overrides : RateLimitOverrides, optional
        Optional overrides with the same keys as ``rate_limit``.

    Returns
    -------
    float
        Computed sleep interval in seconds (always >= 0).

    Examples
    --------
    >>> from etlplus.api.rate_limiter import compute_sleep_seconds
    >>> compute_sleep_seconds({"sleep_seconds": 0.2}, None)
    0.2
    >>> compute_sleep_seconds({"max_per_sec": 4}, None)
    0.25
    >>> compute_sleep_seconds(None, {"max_per_sec": 2})
    0.5
    """
    return RateLimiter.resolve_sleep_seconds(
        rate_limit=rate_limit,
        overrides=overrides,
    )


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
        cfg: Mapping[str, Any] | None,
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
        cfg : Mapping[str, Any] | None
            Configuration mapping from which to derive rate-limit settings.

        Returns
        -------
        RateLimiter
            Instance with normalized ``sleep_seconds`` and ``max_per_sec``.
        """
        sleep_val, rate_val = _normalized_rate_values(cfg)
        if sleep_val is None and rate_val is None:
            return cls()

        # Let __post_init__ enforce invariants and precedence rules.
        return cls(
            sleep_seconds=sleep_val if sleep_val is not None else 0.0,
            max_per_sec=rate_val,
        )

    @classmethod
    def resolve_sleep_seconds(
        cls,
        *,
        rate_limit: RateLimitConfigMap | None,
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
        rate_limit : RateLimitConfigMap | None
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
        # Precedence: overrides > rate_limit
        cfg = _merge_rate_limit(rate_limit, overrides)
        limiter = cls.from_config(cfg or None)
        return limiter.sleep_seconds if limiter.enabled else 0.0
