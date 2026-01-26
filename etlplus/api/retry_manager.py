"""
:mod:`etlplus.api.retry_manager` module.

Retry policies and exponential backoff helpers.

This module centralizes retry behavior for HTTP requests, including policy
parsing and exponential backoff with jitter.

Examples
--------
Retry a request with exponential backoff::

    >>> from etlplus.api.retry_manager import RetryManager
    >>> policy = {"max_attempts": 3, "backoff": 0.25, "retry_on": [429]}
    >>> mgr = RetryManager(policy=policy)
    >>> mgr.get_sleep_time(1)
    0.123  # jittered value in [0, min(backoff, cap)]
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import ClassVar
from typing import Final
from typing import TypedDict

import requests  # type: ignore[import]

from ..types import JSONData
from ..types import Sleeper
from ..utils import to_float
from ..utils import to_int
from ..utils import to_positive_int
from .errors import ApiAuthError
from .errors import ApiRequestError

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RetryStrategy',
    'RetryManager',
    # Typed Dicts
    'RetryPolicy',
    # Type Aliases
    'RetryInput',
]


# SECTION: CONSTANTS ======================================================== #


DEFAULT_RETRY_STATUS_CODES: Final[frozenset[int]] = frozenset(
    {
        429,
        502,
        503,
        504,
    },
)


# SECTION: TYPED DICTS ====================================================== #


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


# SECTION: TYPE ALIASES ===================================================== #


type RetryInput = RetryPolicy | None


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class RetryStrategy:
    """Normalized retry settings derived from a :class:`RetryPolicy`."""

    # -- Attributes -- #

    max_attempts: int
    backoff: float
    retry_on_codes: frozenset[int]

    DEFAULT_ATTEMPTS: ClassVar[int] = 3
    DEFAULT_BACKOFF: ClassVar[float] = 0.5

    # -- Class Methods -- #

    @classmethod
    def from_policy(
        cls,
        policy: RetryInput,
        *,
        default_codes: frozenset[int] = DEFAULT_RETRY_STATUS_CODES,
    ) -> RetryStrategy:
        """Normalize user policy values into a deterministic strategy."""
        policy = policy or {}
        attempts = to_positive_int(
            policy.get('max_attempts'),
            cls.DEFAULT_ATTEMPTS,
        )
        backoff = (
            to_float(
                policy.get('backoff'),
                default=cls.DEFAULT_BACKOFF,
                minimum=0.0,
            )
            or cls.DEFAULT_BACKOFF
        )
        retry_on = policy.get('retry_on') or []
        normalized: set[int] = set()
        for code in retry_on:
            value = to_int(code)
            if value is not None and value > 0:
                normalized.add(value)
        if not normalized:
            normalized = set(default_codes)
        return cls(
            max_attempts=attempts,
            backoff=backoff,
            retry_on_codes=frozenset(normalized),
        )


# SECTION: CLASSES ========================================================== #


@dataclass(frozen=True, slots=True, kw_only=True)
class RetryManager:
    """
    Centralized retry logic for HTTP requests.

    Attributes
    ----------
    DEFAULT_STATUS_CODES : ClassVar[frozenset[int]]
        Default HTTP status codes considered retryable.
    DEFAULT_CAP : ClassVar[float]
        Default maximum sleep seconds for jittered backoff.
    policy : RetryPolicy
        Retry policy configuration.
    retry_network_errors : bool
        Whether to retry on network errors (timeouts, connection errors).
    cap : float
        Maximum sleep seconds for jittered backoff.
    sleeper : Sleeper
        Callable used to sleep between retry attempts. Defaults to
        :func:`time.sleep`.
    strategy : RetryStrategy
        Normalized view of the retry policy (backoff, attempts, codes).
    """

    # -- Class Attributes -- #

    DEFAULT_STATUS_CODES: ClassVar[frozenset[int]] = DEFAULT_RETRY_STATUS_CODES
    DEFAULT_CAP: ClassVar[float] = 30.0

    # -- Instance Attributes-- #

    policy: RetryPolicy
    retry_network_errors: bool = False
    cap: float = DEFAULT_CAP
    sleeper: Sleeper = time.sleep
    strategy: RetryStrategy = field(init=False, repr=False)

    # -- Magic Methods (Object Lifecycle) -- #

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            'strategy',
            RetryStrategy.from_policy(
                self.policy,
                default_codes=self.DEFAULT_STATUS_CODES,
            ),
        )

    # -- Properties -- #

    @property
    def backoff(self) -> float:
        """
        Backoff factor.

        Returns
        -------
        float
            Backoff factor.
        """
        return self.strategy.backoff

    @property
    def max_attempts(self) -> int:
        """
        Maximum number of retry attempts.

        Returns
        -------
        int
            Maximum number of retry attempts.
        """
        return self.strategy.max_attempts

    @property
    def retry_on_codes(self) -> set[int]:
        """
        Set of HTTP status codes that should trigger a retry.

        Returns
        -------
        set[int]
            Retry HTTP status codes.
        """
        return set(self.strategy.retry_on_codes)

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
        Execute *func* with exponential-backoff retries.

        Parameters
        ----------
        func : Callable[..., JSONData]
            Function to run with retry logic.
        url : str
            URL for the API request.
        **kwargs : Any
            Additional keyword arguments to pass to *func*

        Returns
        -------
        JSONData
            Response data from the API request.

        Raises
        ------
        ApiRequestError
            Request failed even after exhausting API request retries.

        Notes
        -----
        Authentication failures propagate as :class:`ApiAuthError` from the
        internal ``_raise_terminal_error`` helper when the status code is 401
        or 403.
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
            HTTP status code extracted from the failed response, if any.
        error : Exception
            The exception that was raised.

        Returns
        -------
        bool
            ``True`` when the request should be retried, ``False`` otherwise.
        """
        # HTTP status-based retry
        if status is not None and status in self.retry_on_codes:
            return True

        # Network error retry
        if self.retry_network_errors:
            if isinstance(error, (requests.Timeout, requests.ConnectionError)):
                return True

        return False

    # -- Internal Instance Methods -- #

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

    # -- Internal Static Methods -- #

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
            The HTTP status code if available, else ``None``.
        """
        response = getattr(error, 'response', None)
        return getattr(response, 'status_code', None)
