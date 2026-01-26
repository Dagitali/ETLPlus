"""
:mod:`etlplus.api.transport` module.

Configure ``requests`` ``HTTPAdapter`` instances with connection pooling and
optional ``urllib3`` retry behavior.

Summary
-------
``build_http_adapter`` accepts a lightweight mapping and translates it into
an ``HTTPAdapter``. When a retry dict is provided, it is mapped to
``urllib3.util.retry.Retry`` where available; otherwise, falls back to an
integer retry count or no retries.

Examples
--------
>>> from etlplus.api import build_http_adapter
>>> cfg = {
...   "pool_connections": 10,
...   "pool_maxsize": 10,
...   "pool_block": False,
...   "max_retries": {"total": 3, "backoff_factor": 0.5},
... }
>>> adapter = build_http_adapter(cfg)
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import TypedDict

import requests  # type: ignore[import]
from requests.adapters import HTTPAdapter  # type: ignore

from ..utils import to_maximum_int
from ..utils import to_positive_int

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'HTTPAdapterMountConfig',
    'HTTPAdapterRetryConfig',
    # Functions
    'build_http_adapter',
    'build_session_with_adapters',
]


# SECTION: TYPED DICTS ====================================================== #


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


# SECTION: INTERNAL FUNCTIONS ============================================== #


def _build_retry_value(
    config: Mapping[str, Any],
) -> int | Any:
    """
    Create an ``urllib3.Retry`` (when available) or integer fallback.

    Parameters
    ----------
    config : Mapping[str, Any]
        Mapping with urllib3 ``Retry`` kwargs.

    Returns
    -------
    int | Any
        ``Retry`` instance, ``0`` when config is empty, or integer fallback
        when urllib3 is absent.
    """
    try:
        from urllib3.util.retry import Retry  # type: ignore
    except ImportError:  # pragma: no cover - optional dependency
        return to_maximum_int(config.get('total'), 0)

    kwargs = _normalize_retry_kwargs(config)
    return Retry(**kwargs) if kwargs else 0


def _normalize_retry_kwargs(
    retries_cfg: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Filter and normalize urllib3 ``Retry`` kwargs from a mapping.

    Parameters
    ----------
    retries_cfg : Mapping[str, Any]
        Raw retry configuration mapping.

    Returns
    -------
    dict[str, Any]
        Filtered and normalized keyword arguments for ``Retry``.
    """
    allowed_keys = {
        'total',
        'connect',
        'read',
        'redirect',
        'status',
        'backoff_factor',
        'status_forcelist',
        'allowed_methods',
        'raise_on_status',
        'respect_retry_after_header',
    }
    normalized: dict[str, Any] = {}
    for key, value in retries_cfg.items():
        if key not in allowed_keys:
            continue
        match key:
            case 'status_forcelist' if isinstance(value, (list, tuple, set)):
                normalized[key] = tuple(value)
            case 'allowed_methods' if isinstance(
                value,
                (list, tuple, set, frozenset),
            ):
                normalized[key] = frozenset(value)
            case _:
                normalized[key] = value
    return normalized


def _resolve_max_retries(
    retries_cfg: object,
) -> int | Any:
    """
    Normalize ``max_retries`` values accepted by ``HTTPAdapter``.

    Parameters
    ----------
    retries_cfg : object
        Raw ``max_retries`` configuration value.

    Returns
    -------
    int | Any
        Integer retry count or ``Retry`` instance.
    """
    match retries_cfg:
        case int():
            return to_maximum_int(retries_cfg, 0)
        case Mapping():
            try:
                return _build_retry_value(retries_cfg)
            except (TypeError, ValueError, AttributeError):
                return to_maximum_int(retries_cfg.get('total'), 0)
        case _:
            return 0


# SECTION: FUNCTIONS ======================================================== #


def build_http_adapter(
    cfg: Mapping[str, Any],
) -> HTTPAdapter:
    """
    Build a requests ``HTTPAdapter`` from a configuration mapping.

    Supported keys in cfg:
    - pool_connections (int)
    - pool_maxsize (int)
    - pool_block (bool)
    - max_retries (int or dict matching urllib3 ``Retry`` args)

    When ``max_retries`` is a dict, this attempts to construct an
    ``urllib3.util.retry.Retry`` instance with the provided keys. Unknown
    keys are ignored. If urllib3 is unavailable, falls back to no retries
    (0) or an integer value when provided.

    Parameters
    ----------
    cfg : Mapping[str, Any]
        Adapter configuration mapping.

    Returns
    -------
    HTTPAdapter
        Configured HTTPAdapter instance.
    """
    pool_connections = to_positive_int(cfg.get('pool_connections'), 10)
    pool_maxsize = to_positive_int(cfg.get('pool_maxsize'), 10)
    pool_block = bool(cfg.get('pool_block', False))

    max_retries = _resolve_max_retries(cfg.get('max_retries'))

    return HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=max_retries,
        pool_block=pool_block,
    )


def build_session_with_adapters(
    adapters_cfg: Sequence[HTTPAdapterMountConfig],
) -> requests.Session:
    """
    Mount adapters described by *adapters_cfg* onto a new session.

    Ignores invalid adapter configurations so that a usable session is always
    returned.

    Parameters
    ----------
    adapters_cfg : Sequence[HTTPAdapterMountConfig]
        Configuration mappings describing the adapter prefix, pooling
        values, and retry policy for each mounted adapter.

    Returns
    -------
    requests.Session
        Configured session instance.
    """
    session = requests.Session()
    for cfg in adapters_cfg:
        prefix = cfg.get('prefix', 'https://')
        try:
            adapter = build_http_adapter(cfg)
            session.mount(prefix, adapter)
        except (ValueError, TypeError, AttributeError):
            # Skip invalid adapter configs but still return a usable session.
            continue
    return session
