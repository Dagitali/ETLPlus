"""
``etlplus.api.rate_limiter`` module.

Centralized rate limiting logic for REST API requests.

Summary
-------
Encapsulates rate limit configuration and behavior in a single class.
Supports instantiation from a config dict, computation of sleep intervals,
and application of sleep between requests.

Examples
--------
Create a limiter from static config and apply it before each request::

    cfg = {"max_per_sec": 5}
    limiter = RateLimiter.from_config(cfg)

    for payload in batch:
        limiter.sleep()
        client.send(payload)
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any
from typing import Mapping
from typing import Self


# SECTION: EXPORTS ========================================================== #


__all__ = ['RateLimiter']


# SECTION: TYPE ALIASES ===================================================== #


RateLimitConfig = Mapping[str, Any]


# SECTION: PROTECTED FUNCTIONS ============================================== #


def _to_positive_float(value: Any) -> float | None:
    """
    Convert a value to a positive float or ``None``.

    Parameters
    ----------
    value : Any
        Value to convert.

    Returns
    -------
    float | None
        Positive float if conversion succeeds and the value is > 0;
        otherwise ``None``.
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class RateLimiter:
    """
    Centralized rate limiting for REST API requests.

    Attributes
    ----------
    sleep_seconds : float, optional
        Fixed delay between requests. Defaults to 0.0.
    max_per_sec : float | None, optional
        Maximum requests-per-second rate; converted to ``1 / max_per_sec``
        seconds between requests when positive. Defaults to ``None``.
    """

    # -- Attributes -- #

    sleep_seconds: float = 0.0
    max_per_sec: float | None = None

    # -- Magic Methods (Object Lifecycle) -- #

    def __post_init__(self) -> None:
        """
        Normalize internal state and keep ``sleep_seconds`` and
        ``max_per_sec`` consistent.

        Precedence:
        - If ``sleep_seconds`` is positive, it is treated as canonical.
        - Else if ``max_per_sec`` is positive, it is used to derive
          ``sleep_seconds``.
        - Otherwise the limiter is disabled.
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
        """Check if the limiter is enabled."""
        return self.enabled

    # -- Getters -- #

    @property
    def enabled(self) -> bool:
        """Check if the limiter currently applies any delay."""
        return self.sleep_seconds > 0

    # -- Instance Methods -- #

    def enforce(self) -> None:
        """Apply rate limiting by sleeping if configured."""
        if self.sleep_seconds > 0:
            time.sleep(self.sleep_seconds)

    # -- Class Methods -- #

    @classmethod
    def disabled(cls) -> Self:
        """Return a :class:`RateLimiter` that never sleeps."""
        return cls(sleep_seconds=0.0)

    @classmethod
    def fixed(cls, seconds: float) -> Self:
        """Return a :class:`RateLimiter` with a fixed non-negative delay."""
        try:
            value = float(seconds)
        except (TypeError, ValueError):
            value = 0.0

        if value < 0:
            value = 0.0

        return cls(sleep_seconds=value)

    @classmethod
    def from_config(cls, cfg: RateLimitConfig | None) -> Self:
        """
        Build :class:`RateLimiter` from a config mapping.

        The mapping may contain:

        - ``"sleep_seconds"``: a positive number of seconds between requests.
        - ``"max_per_sec"``: a positive requests-per-second rate
          (converted to ``1 / max_per_sec`` seconds between requests).

        If neither value is provided or both are invalid/non-positive,
        this returns a ``RateLimiter`` with the default ``sleep_seconds``.

        Parameters
        ----------
        cfg : RateLimitConfig | None
            Configuration mapping.

        Returns
        -------
        Self
            Instance with computed ``sleep_seconds``.
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
