"""
etlplus.api.types module.

Centralized type aliases and ``TypedDict``-based configurations used in the
:mod:`etlplus.api` package.

Contents
--------
- JSON aliases: ``JSONDict``, ``JSONList``, ``JSONData``
- Pagination configs: ``PagePaginationConfig``, ``CursorPaginationConfig``,
  and the union ``PaginationConfig``
- Rate limiting: ``RateLimitConfig``
- Retry policy: ``RetryPolicy``
- HTTP transport: ``HTTPAdapterRetryConfig``, ``HTTPAdapterMountConfig``

Examples
--------
>>> from etlplus.api import PaginationConfig
>>> pg: PaginationConfig = {"type": "page", "page_size": 100}
>>> from etlplus.api import RetryPolicy
>>> rp: RetryPolicy = {"max_attempts": 3, "backoff": 0.5}
"""
from __future__ import annotations

from typing import Any
from typing import NotRequired
from typing import TypedDict


# SECTION: PUBLIC API  ===================================================== #


__all__ = [
    # Aliases
    'JSONDict', 'JSONList', 'JSONData',

    # HTTP adapter config
    'HTTPAdapterMountConfig', 'HTTPAdapterRetryConfig',

    # Retry
    'RetryPolicy',
]


# SECTION: TYPED DICTS (HTTP Adapter / Retry) =============================== #


class HTTPAdapterRetryConfig(TypedDict, total=False):
    """
    Retry configuration for urllib3 ``Retry``.

    Used by requests' ``HTTPAdapter``.

    Summary
    -------
    Keys mirror the ``Retry`` constructor where relevant. All keys are
    optional; omit any you don't need. When converted downstream, collection-
    valued fields are normalized to tuples/frozensets.

    Attributes
    ----------
    total : int
        Retry counters matching urllib3 semantics.
    connect : int
        Number of connection-related retries.
    read : int
        Number of read-related retries.
    redirect : int
        Number of redirect-related retries.
    status : int
        Number of status-related retries.
    backoff_factor : float
        Base factor for exponential backoff between attempts.
    status_forcelist : list[int] | tuple[int, ...]
        HTTP status codes that should always be retried.
    allowed_methods : list[str] | set[str] | tuple[str, ...]
        Idempotent HTTP methods eligible for retry.
    raise_on_status : bool
        Whether to raise after exhausting status-based retries.
    respect_retry_after_header : bool
        Honor ``Retry-After`` response headers when present.

    Examples
    --------
    >>> retry_cfg: HTTPAdapterRetryConfig = {
    ...     'total': 5,
    ...     'backoff_factor': 0.5,
    ...     'status_forcelist': [429, 503],
    ...     'allowed_methods': ['GET'],
    ... }
    """

    # -- Attributes -- #

    total: int
    connect: int
    read: int
    redirect: int
    status: int
    backoff_factor: float
    status_forcelist: list[int] | tuple[int, ...]
    allowed_methods: list[str] | set[str] | tuple[str, ...]
    raise_on_status: bool
    respect_retry_after_header: bool


class HTTPAdapterMountConfig(TypedDict, total=False):
    """
    Configuration mapping for mounting an ``HTTPAdapter`` on a ``Session``.

    Summary
    -------
    Provides connection pooling and optional retry behavior. Values are
    forwarded into ``HTTPAdapter`` and, when a retry dict is supplied,
    converted to a ``Retry`` instance where supported.

    Attributes
    ----------
    prefix : str
        Prefix to mount the adapter on (e.g., ``'https://'`` or specific base).
    pool_connections : int
        Number of urllib3 connection pools to cache.
    pool_maxsize : int
        Maximum connections per pool.
    pool_block : bool
        Whether the pool should block for connections instead of creating new
        ones.
    max_retries : int | HTTPAdapterRetryConfig
        Retry configuration passed to ``HTTPAdapter`` (int) or converted to
        ``Retry``.

    Examples
    --------
    >>> adapter_cfg: HTTPAdapterMountConfig = {
    ...     'prefix': 'https://',
    ...     'pool_connections': 10,
    ...     'pool_maxsize': 10,
    ...     'pool_block': False,
    ...     'max_retries': {
    ...         'total': 3,
    ...         'backoff_factor': 0.5,
    ...     },
    ... }
    """

    # -- Attributes -- #

    prefix: str
    pool_connections: int
    pool_maxsize: int
    pool_block: bool
    max_retries: int | HTTPAdapterRetryConfig


# SECTION: TYPED DICTS (Retries) ============================================ #


class RetryPolicy(TypedDict):
    """
    Optional retry policy for HTTP requests.

    Summary
    -------
    Controls exponential backoff with jitter (applied externally) and retry
    eligibility by HTTP status code.

    Attributes
    ----------
    max_attempts : NotRequired[int]
        Maximum number of attempts (including the first). If omitted, a default
        may be applied by callers.
    backoff : NotRequired[float]
        Base backoff seconds; attempt ``n`` sleeps ``backoff * 2**(n-1)``
        before retrying.
    retry_on : NotRequired[list[int]]
        HTTP status codes that should trigger a retry.

    Examples
    --------
    >>> rp: RetryPolicy = {
    ...     'max_attempts': 5,
    ...     'backoff': 0.5,
    ...     'retry_on': [429, 502, 503, 504],
    ... }
    """

    # -- Attributes -- #

    max_attempts: NotRequired[int]
    backoff: NotRequired[float]
    retry_on: NotRequired[list[int]]


# SECTION: TYPE ALIASES ===================================================== #


type JSONDict = dict[str, Any]
type JSONList = list[JSONDict]
type JSONData = JSONDict | JSONList

type JSONRecord = dict[str, bool | float | int | str | None]
type JSONRecords = list[JSONRecord]
