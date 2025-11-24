"""
``etlplus.api.request`` module.

Centralized rate limiting logic for REST API requests.

This module encapsulates rate limit configuration and behavior in a single
class. It supports instantiation from a configuration mapping, computation of
sleep intervals, and application of sleep between requests.

Examples
--------
Create a limiter from static configuration and apply it before each
request::

    cfg = {"max_per_sec": 5}
    limiter = RateLimiter.from_config(cfg)

    for payload in batch:
        limiter.enforce()
        client.send(payload)
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any
from typing import Mapping
from typing import NotRequired
from typing import TypedDict


# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RateLimitConfig',
    'RateLimiter',

    # Functions
    'compute_sleep_seconds',
]


# SECTION: TYPE ALIASES ===================================================== #


_RateLimitConfig = Mapping[str, Any]


# SECTION: TYPED DICTS ====================================================== #


class RateLimitConfig(TypedDict):
    """
    Optional rate limit configuration.

    Summary
    -------
    Provides either a fixed delay (``sleep_seconds``) or derives one from a
    maximum requests-per-second value (``max_per_sec``).

    Attributes
    ----------
    sleep_seconds : NotRequired[float | int]
        Fixed delay between requests.
    max_per_sec : NotRequired[float | int]
        Maximum requests per second; converted to ``1 / max_per_sec`` seconds
        between requests when positive.

    Examples
    --------
    >>> rl: RateLimitConfig = {'max_per_sec': 4}
    ... # sleep ~= 0.25s between calls
    """

    # -- Attributes -- #

    sleep_seconds: NotRequired[float | int]
    max_per_sec: NotRequired[float | int]


# SECTION: PROTECTED FUNCTIONS ============================================== #


def _to_positive_float(
    value: Any,
) -> float | None:
    """
    Convert a value to a positive float.

    Parameters
    ----------
    value : Any
        Value to convert.

    Returns
    -------
    float | None
        Positive float if conversion succeeds and the value is greater than
        zero; ``None`` if not.
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


# SECTION: FUNCTIONS ======================================================== #


def compute_sleep_seconds(
    rate_limit: RateLimitConfig | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> float:
    """
    Compute the sleep interval from rate-limit configuration.

    Precedence is:

    1. ``overrides["sleep_seconds"]``
    2. ``overrides["max_per_sec"]``
    3. ``rate_limit["sleep_seconds"]``
    4. ``rate_limit["max_per_sec"]``

    Non-numeric or non-positive values are ignored.

    Parameters
    ----------
    rate_limit : RateLimitConfig | None, optional
        Base rate-limit configuration. May contain ``"sleep_seconds"`` or
        ``"max_per_sec"``.
    overrides : Mapping[str, Any] | None, optional
        Optional overrides with the same keys as ``rate_limit``.

    Returns
    -------
    float
        Computed sleep interval in seconds. The value is always greater than
        or equal to zero.

    Examples
    --------
    >>> from etlplus.api.request import compute_sleep_seconds
    >>> compute_sleep_seconds({"sleep_seconds": 0.2}, None)
    0.2
    >>> compute_sleep_seconds({"max_per_sec": 4}, None)
    0.25
    >>> compute_sleep_seconds(None, {"max_per_sec": 2})
    0.5
    """
    # Precedence: overrides > rate_limit
    cfg = overrides if overrides else rate_limit
    limiter = RateLimiter.from_config(cfg or {})

    return limiter.sleep_seconds if limiter.enabled else 0.0


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class RateLimiter:
    """
    Centralized rate limiting for REST API requests.

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
        sleep = _to_positive_float(self.sleep_seconds)
        rate = _to_positive_float(self.max_per_sec)

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
        try:
            value = float(seconds)
        except (TypeError, ValueError):
            value = 0.0

        if value < 0:
            value = 0.0

        return cls(sleep_seconds=value)

    @classmethod
    def from_config(
        cls,
        cfg: _RateLimitConfig | None,
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
        cfg : _RateLimitConfig | None
            Configuration mapping from which to derive rate-limit settings.

        Returns
        -------
        RateLimiter
            Instance with normalized ``sleep_seconds`` and ``max_per_sec``.
        """
        if not cfg:
            return cls()

        sleep_val = _to_positive_float(cfg.get('sleep_seconds'))
        rate_val = _to_positive_float(cfg.get('max_per_sec'))

        # Let __post_init__ enforce invariants and precedence rules.
        if sleep_val is not None or rate_val is not None:
            return cls(
                sleep_seconds=sleep_val if sleep_val is not None else 0.0,
                max_per_sec=rate_val,
            )

        return cls()
