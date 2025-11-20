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


# SECTION: TYPE ALIASES ===================================================== #


RateLimitConfig = Mapping[str, Any]


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class RateLimiter:
    """
    Centralized rate limiting for REST API requests.

    Attributes
    ----------
    sleep_seconds : float, optional
        Fixed delay between requests. Defaults to 0.0.
    """

    # -- Attributes -- #

    sleep_seconds: float = 0.0

    # -- Magic Methods (Object Lifecycle) -- #

    def __post_init__(self) -> None:
        """Normalize ``sleep_seconds`` to a non-negative float."""
        try:
            self.sleep_seconds = float(self.sleep_seconds)
        except (TypeError, ValueError):
            self.sleep_seconds = 0.0

        if self.sleep_seconds < 0:
            self.sleep_seconds = 0.0

    # -- Magic Methods (Object Representation) -- #

    def __bool__(self) -> bool:
        """Check if the limiter is enabled."""
        return self.enabled

    # -- Getters -- #

    @property
    def enabled(self) -> bool:
        """Check if the limiter currently applies any delay."""
        return self.sleep_seconds > 0

    @property
    def max_per_sec(self) -> float | None:
        """
        Maximum requests-per-second rate.

        Compute the maximum requests-per-second rate if ``sleep_seconds`` is
        positive, or return ``None`` if not.

        Returns
        -------
        float | None
            Requests-per-second rate if computable and positive; ``None``
            otherwise.
        """
        if self.sleep_seconds <= 0:
            return None

        try:
            rate = 1.0 / float(self.sleep_seconds)
        except (TypeError, ValueError, ZeroDivisionError):
            return None

        return rate if rate > 0 else None

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

        def _pos_float(value: Any) -> float | None:
            try:
                f = float(value)
            except (TypeError, ValueError):
                return None
            return f if f > 0 else None

        sleep_val = _pos_float(cfg.get('sleep_seconds'))
        if sleep_val is not None:
            return cls(sleep_seconds=sleep_val)

        max_per_sec_val = _pos_float(cfg.get('max_per_sec'))
        if max_per_sec_val is not None:
            return cls(sleep_seconds=1.0 / max_per_sec_val)

        return cls()

    @classmethod
    def from_max_per_sec(cls, max_per_sec: float) -> Self:
        """
        Build :class:`RateLimiter` from a maximum requests-per-second rate.

        Compute the sleep interval (in seconds) from the given rate.

        Parameters
        ----------
        max_per_sec : float
            Maximum requests per second; converted to ``1 / max_per_sec``
            seconds between requests when positive.

        Returns
        -------
        Self
            Instance with computed ``sleep_seconds``.
        """
        return cls.from_config({'max_per_sec': max_per_sec})
