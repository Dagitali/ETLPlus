"""
etlplus.api.types
=================

A module centralizing type aliases and ``TypedDict``-based configurations used
in the ``:mod:etlplus.api`` package.

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
from typing import Literal
from typing import NotRequired
from typing import TypedDict


# SECTION: PUBLIC API  ===================================================== #


__all__ = [
    # Aliases
    'JSONDict', 'JSONList', 'JSONData', 'PaginationConfig',

    # HTTP adapter config
    'HTTPAdapterMountConfig', 'HTTPAdapterRetryConfig',

    # Pagination configs
    'CursorPaginationConfig', 'PagePaginationConfig',

    # Rate limit / retry
    'RateLimitConfig', 'RetryPolicy',
]


# SECTION: TYPED DICTS (HTTP Adapter / Retry) =============================== #


class HTTPAdapterRetryConfig(TypedDict, total=False):
    """
    Retry configuration for urllib3 Retry used by requests' HTTPAdapter.

    Keys mirror urllib3.util.retry.Retry constructor where relevant.
    All keys are optional; omit unset values.

    Example
    -------
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
    Configuration for mounting an HTTPAdapter on a Session.

    Attributes
    ----------
    prefix : str
        Prefix to mount the adapter on (e.g., 'https://', 'http://', or a
        specific base like 'https://api.example.com/'). Defaults to
        'https://' if omitted.
    pool_connections : int
        The number of urllib3 connection pools to cache.
    pool_maxsize : int
        The maximum number of connections to save in the pool.
    pool_block : bool
        Whether the connection pool should block for connections.
    max_retries : int | HTTPAdapterRetryConfig
        Retry configuration. When an int, passed directly to HTTPAdapter.
        When a dict, converted to urllib3 Retry with matching keys.

    Example
    -------
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


# SECTION: TYPED DICTS (Pagination) ========================================= #


class CursorPaginationConfig(TypedDict):
    """
    Pagination config for cursor-based pagination.

    Attributes
    ----------
    type : Literal['cursor']
        Pagination type.
    records_path : str
        Dotted path to records in the payload.
    max_pages : int
        Maximum number of pages to fetch.
    max_records : int
        Maximum number of records to fetch.
    cursor_param : str
        Query parameter name for the cursor.
    cursor_path : str
        Dotted path to extract the next cursor from the payload.
    start_cursor : str | int
        Initial cursor value to start pagination.
    page_size : int
        Number of records per page.

    Example
    -------
    >>> cfg: CursorPaginationConfig = {
    ...     'type': 'cursor',
    ...     'records_path': 'data.items',
    ...     'cursor_param': 'cursor',
    ...     'cursor_path': 'data.nextCursor',
    ...     'page_size': 100,
    ... }
    """

    # -- Attributes -- #

    type: Literal['cursor']
    records_path: NotRequired[str]
    max_pages: NotRequired[int]
    max_records: NotRequired[int]
    cursor_param: NotRequired[str]
    cursor_path: NotRequired[str]
    start_cursor: NotRequired[str | int]
    page_size: NotRequired[int]


class PagePaginationConfig(TypedDict):
    """
    Pagination config for 'page' and 'offset' types.

    Attributes
    ----------
    type : Literal['page', 'offset']
        Pagination type.
    records_path : str
        Dotted path to records in the payload.
    max_pages : int
        Maximum number of pages to fetch.
    max_records : int
        Maximum number of records to fetch.
    page_param : str
        Query parameter name for the page number.
    size_param : str
        Query parameter name for the page size.
    start_page : int
        Starting page number (1-based).
    page_size : int
        Number of records per page.

    Example
    -------
    >>> cfg: PagePaginationConfig = {
    ...     'type': 'page',
    ...     'records_path': 'data.items',
    ...     'page_param': 'page',
    ...     'size_param': 'per_page',
    ...     'start_page': 1,
    ...     'page_size': 100,
    ... }
    """

    # -- Attributes -- #

    type: Literal['page', 'offset']
    records_path: NotRequired[str]
    max_pages: NotRequired[int]
    max_records: NotRequired[int]
    page_param: NotRequired[str]
    size_param: NotRequired[str]
    start_page: NotRequired[int]
    page_size: NotRequired[int]


# SECTION: TYPED DICTS (Rate Limits / Retries) ============================== #


class RateLimitConfig(TypedDict):
    """
    Optional rate limit configuration.

    Attributes
    ----------
    sleep_seconds : float
        Fixed delay between requests.
    max_per_sec : float
        Maximum requests per second; converted to ``1 / max_per_sec`` seconds
        between requests when positive.

    Example
    -------
    >>> rl: RateLimitConfig = {'max_per_sec': 4}
    ... # sleep ~= 0.25s between calls
    """

    # -- Attributes -- #

    sleep_seconds: NotRequired[float | int]
    max_per_sec: NotRequired[float | int]


class RetryPolicy(TypedDict):
    """
    Optional retry policy for HTTP requests.

    Attributes
    ----------
    max_attempts : int
        Maximum number of attempts (including the first). If omitted,
        a default is applied when a policy is provided.
    backoff : float
        Base backoff seconds for exponential backoff. Attempt ``n`` sleeps
        ``backoff * 2**(n-1)`` before retrying.
    retry_on : list[int]
        HTTP status codes that should trigger a retry.

    Example
    -------
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

type PaginationConfig = PagePaginationConfig | CursorPaginationConfig
