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

from typing import NotRequired
from typing import TypedDict


# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Type Aliases
    'JSONScalar',
    'JSONValue',
    'JSONDict',
    'JSONList',
    'JSONData',
    'JSONRecord',
    'JSONRecords',

    # Typed Dicts
    'RetryPolicy',
]


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


type JSONScalar = bool | float | int | str | None
type JSONValue = JSONScalar | 'JSONDict' | 'JSONList'
type JSONDict = dict[str, JSONValue]
type JSONList = list[JSONValue]
type JSONRecord = JSONDict
type JSONRecords = list[JSONRecord]
type JSONData = JSONDict | JSONList | JSONRecords
