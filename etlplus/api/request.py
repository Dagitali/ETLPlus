"""
:mod:`etlplus.api.request` module.

Centralized logic for handling HTTP requests, including:
- Request rate limiting.
- Retry policies with exponential backoff.

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

import random
import time
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import ClassVar
from typing import TypedDict

import requests  # type: ignore[import]

from ..types import JSONData
from ..utils import to_float
from ..utils import to_int
from ..utils import to_positive_float
from ..utils import to_positive_int
from .errors import ApiAuthError
from .errors import ApiRequestError

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RateLimiter',

    # Functions
    'compute_sleep_seconds',

    # Typed Dicts
    'RateLimitConfigMap',
    'RetryPolicy',
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


class RetryPolicy(TypedDict, total=False):
    """
    Optional retry policy for HTTP requests.

    All keys are optional.

    Attributes
    ----------
    max_attempts : int, optional
        Maximum number of attempts (including the first). When omitted,
        callers may apply defaults.
    backoff : float, optional
        Base backoff seconds; attempt ``n`` sleeps ``backoff * 2**(n-1)``
        before retrying.
    retry_on : list[int], optional
        HTTP status codes that should trigger a retry.

    Notes
    -----
    - Controls exponential backoff with jitter (applied externally) and retry
        eligibility by HTTP status code. Used by :class:`RetryManager`.
    """

    max_attempts: int
    backoff: float
    retry_on: list[int]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _merge_rate_limit(
    rate_limit: Mapping[str, Any] | None,
    overrides: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """
    Merge ``rate_limit`` and ``overrides`` honoring override precedence.

    Parameters
    ----------
    rate_limit : Mapping[str, Any] | None
        Base rate-limit configuration.
    overrides : Mapping[str, Any] | None
        Override configuration.

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
    rate_limit : RateLimitConfigMap | None, optional
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
    cfg = _merge_rate_limit(rate_limit, overrides)
    limiter = RateLimiter.from_config(cfg or None)

    return limiter.sleep_seconds if limiter.enabled else 0.0


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


@dataclass(frozen=True, slots=True, kw_only=True)
class RetryManager:
    """
    Centralized retry logic for HTTP requests.

    Attributes
    ----------
    DEFAULT_STATUS_CODES : ClassVar[set[int]]
        Default HTTP status codes considered retryable.
    policy : RetryPolicy
        Retry policy configuration.
    retry_network_errors : bool
        Whether to retry on network errors (timeouts, connection errors).
    cap : float
        Maximum sleep seconds for jittered backoff.
    sleeper : Callable[[float], None]
        Callable used to sleep between retry attempts. Defaults to
        :func:`time.sleep`.
    """

    # -- Attributes -- #

    DEFAULT_STATUS_CODES: ClassVar[set[int]] = {429, 502, 503, 504}

    policy: RetryPolicy
    retry_network_errors: bool = False
    cap: float = 30.0
    sleeper: Callable[[float], None] = time.sleep

    # -- Getters -- #

    @property
    def backoff(self) -> float:
        """
        Backoff factor.

        Returns
        -------
        float
            Backoff factor.
        """
        return to_float(
            self.policy.get('backoff'),
            default=0.5,
            minimum=0.0,
        ) or 0.5

    @property
    def max_attempts(self) -> int:
        """
        Maximum number of retry attempts.

        Returns
        -------
        int
            Maximum number of retry attempts.
        """
        return to_positive_int(self.policy.get('max_attempts'), 3)

    @property
    def retry_on_codes(self) -> set[int]:
        """
        Set of HTTP status codes that should trigger a retry.

        Returns
        -------
        set[int]
            Retry HTTP status codes.
        """
        codes = self.policy.get('retry_on')
        if not codes:
            return self.DEFAULT_STATUS_CODES
        normalized: set[int] = set()
        for code in codes:
            value = to_int(code)
            if value is not None and value > 0:
                normalized.add(value)
        return normalized or self.DEFAULT_STATUS_CODES

    # -- Instance Methods -- #

    def get_sleep_time(
        self,
        attempt: int,
    ) -> float:
        """
        Sleep time in seconds.

        Parameters
        ----------
        attempt : int
            Attempt number.

        Returns
        -------
        float
            Sleep time in seconds.
        """
        attempt = max(1, attempt)
        exp = self.backoff * (2 ** (attempt - 1))
        upper = min(exp, self.cap)
        return random.uniform(0.0, upper)

    def run_with_retry(
        self,
        func: Callable[..., JSONData],
        url: str,
        **kwargs: Any,
    ) -> JSONData:
        """
        Execute ``func`` with exponential-backoff retries.

        Parameters
        ----------
        func : Callable[..., JSONData]
            Function to run with retry logic.
        url : str
            URL for the API request.
        **kwargs : Any
            Additional keyword arguments to pass to ``func``

        Returns
        -------
        JSONData
            Response data from the API request.

        Raises
        ------
        ApiRequestError
            Request error during API request.
        """
        for attempt in range(1, self.max_attempts + 1):
            try:
                return func(url, **kwargs)
            except requests.RequestException as e:
                status = self._extract_status(e)
                exhausted = attempt == self.max_attempts
                if not self.should_retry(status, e) or exhausted:
                    self._raise_terminal_error(url, attempt, status, e)
                self.sleeper(self.get_sleep_time(attempt))

        # ``range`` already covered all attempts; reaching this line would
        # indicate a logical error.
        raise ApiRequestError(  # pragma: no cover - defensive
            url=url,
            status=None,
            attempts=self.max_attempts,
            retried=True,
            retry_policy=self.policy,
            cause=None,
        )

    def should_retry(
        self,
        status: int | None,
        error: Exception,
    ) -> bool:
        """
        Determine whether a request should be retried.

        Parameters
        ----------
        status : int | None
            Whether the request should be retried.
        error : Exception
            The exception that was raised.

        Returns
        -------
        bool
            Whether the request should be retried.
        """
        # HTTP status-based retry
        if status is not None and status in self.retry_on_codes:
            return True

        # Network error retry
        if self.retry_network_errors:
            if isinstance(error, (requests.Timeout, requests.ConnectionError)):
                return True

        return False

    # -- Protected Instance Methods -- #

    def _raise_terminal_error(
        self,
        url: str,
        attempt: int,
        status: int | None,
        error: requests.RequestException,
    ) -> None:
        """
        Raise the appropriate terminal error after exhausting retries.

        Parameters
        ----------
        url : str
            URL for the API request.
        attempt : int
            Attempt number.
        status : int | None
            HTTP status code if available.
        error : requests.RequestException
            The exception that was raised.

        Raises
        ------
        ApiAuthError
            Authentication error during API request.
        ApiRequestError
            Request error during API request.
        """
        retried = attempt > 1
        if status in {401, 403}:
            raise ApiAuthError(
                url=url,
                status=status,
                attempts=attempt,
                retried=retried,
                retry_policy=self.policy,
                cause=error,
            ) from error

        raise ApiRequestError(
            url=url,
            status=status,
            attempts=attempt,
            retried=retried,
            retry_policy=self.policy,
            cause=error,
        ) from error

    # -- Protected Static Methods -- #

    @staticmethod
    def _extract_status(
        error: requests.RequestException,
    ) -> int | None:
        """
        Extract the HTTP status code from a RequestException.

        Parameters
        ----------
        error : requests.RequestException
            The exception from which to extract the status code.

        Returns
        -------
        int | None
            The HTTP status code if available, otherwise None.
        """
        response = getattr(error, 'response', None)
        return getattr(response, 'status_code', None)
