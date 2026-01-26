"""
:mod:`etlplus.api.rate_limiting.rate_limiter` module.

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
from dataclasses import dataclass
from typing import Self

from ...utils import to_float
from ...utils import to_positive_float
from .config import RateLimitConfig
from .config import RateLimitConfigMap
from .config import RateLimitInput
from .config import RateLimitOverrides

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RateLimiter',
    # Data Classes
    'RateLimitConfig',
    # Typed Dicts
    'RateLimitConfigMap',
]


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
    def disabled(cls) -> Self:
        """
        Create a limiter that never sleeps.

        Returns
        -------
        Self
            Instance with rate limiting disabled.
        """
        return cls(sleep_seconds=0.0)

    @classmethod
    def fixed(
        cls,
        seconds: float,
    ) -> Self:
        """
        Create a limiter with a fixed non-negative delay.

        Parameters
        ----------
        seconds : float
            Desired delay between requests, in seconds. Negative values
            are treated as ``0.0``.

        Returns
        -------
        Self
            Instance with the specified delay.
        """
        value = to_float(seconds, 0.0, minimum=0.0) or 0.0

        return cls(sleep_seconds=value)

    @classmethod
    def from_config(
        cls,
        cfg: RateLimitInput,
    ) -> Self:
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
        cfg : RateLimitInput
            Rate-limit configuration from which to derive settings.

        Returns
        -------
        Self
            Instance with normalized ``sleep_seconds`` and ``max_per_sec``.
        """
        config = RateLimitConfig.from_inputs(rate_limit=cfg)

        # RateLimiter.__post_init__ will normalize and enforce invariants.
        return cls(**config.as_mapping())

    @classmethod
    def resolve_sleep_seconds(
        cls,
        *,
        rate_limit: RateLimitInput,
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
        rate_limit : RateLimitInput
            Base rate-limit configuration. May contain ``"sleep_seconds"`` or
            ``"max_per_sec"``.
        overrides : RateLimitOverrides, optional
            Optional overrides with the same keys as *rate_limit*.

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
        >>> from etlplus.api.rate_limiting import RateLimiter
        >>> RateLimiter.resolve_sleep_seconds(
        ...     rate_limit={'max_per_sec': 5},
        ...     overrides={'sleep_seconds': 0.25},
        ... )
        0.25
        """
        config = RateLimitConfig.from_inputs(
            rate_limit=rate_limit,
            overrides=overrides,
        )
        return float(config.sleep_seconds) if config.sleep_seconds else 0.0
