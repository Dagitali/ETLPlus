"""
etlplus.api package.

High-level helpers for building REST API clients with pagination, retry,
rate limiting, and transport configuration.

Summary
-------
Use :class:`etlplus.api.EndpointClient` to register relative endpoint paths
under a base URL and paginate responses. The client can apply rate limits
between requests and perform exponential-backoff retries with full jitter.

Examples
--------
Page-based pagination
^^^^^^^^^^^^^^^^^^^^^
>>> from etlplus.api import EndpointClient
>>> client = EndpointClient(
...     base_url="https://api.example.com/v1",
...     endpoints={"list_users": "/users"},
... )
>>> page_cfg = {
...     "type": "page",               # or "offset"
...     "records_path": "data.items", # dotted path into payload
...     "page_param": "page",
...     "size_param": "per_page",
...     "start_page": 1,
...     "page_size": 100,
... }
>>> rows = client.paginate(
...     "list_users",
...     query_parameters={"active": "true"},
...     pagination=page_cfg,
... )

Retries and network errors
^^^^^^^^^^^^^^^^^^^^^^^^^^
>>> client = EndpointClient(
...     base_url="https://api.example.com/v1",
...     endpoints={"list": "/items"},
...     retry={"max_attempts": 5, "backoff": 0.5, "retry_on": [429, 503]},
...     retry_network_errors=True,
... )
>>> items = client.paginate(
...     "list", pagination={"type": "page", "page_size": 50}
... )

Absolute URLs
^^^^^^^^^^^^^
Use :meth:`EndpointClient.paginate_url` for an already composed absolute URL.
It accepts the same pagination config and returns either the raw JSON object
(no pagination) or a list of record dicts aggregated across pages.

Notes
-----
- ``EndpointClient.endpoints`` is read-only at runtime.
- Pagination defaults are centralized on the client (``page``, ``per_page``,
  ``cursor``, ``limit``; start page ``1``; page size ``100``).
- Retries are opt-in via the ``retry`` parameter; backoff uses jitter.
- Use ``retry_network_errors=True`` to also retry timeouts/connection errors.

See Also
--------
- :mod:`etlplus.api.types` for shared types (``PaginationConfig``,
  ``RetryPolicy``, HTTP adapter configs)
- :mod:`etlplus.api.transport` for HTTPAdapter helpers
- :func:`etlplus.api.compute_sleep_seconds` for deriving inter-request delay
  from rate limit settings
"""
from __future__ import annotations

from .auth import EndpointCredentialsBearer
from .client import EndpointClient
from .request import compute_sleep_seconds
from .request import RateLimitConfigMap
from .request import RateLimiter
from .response import CursorPaginationMap
from .response import PagePaginationMap
from .response import PaginationMap
from .response import PaginationType
from .transport import build_http_adapter
from .transport import HTTPAdapterMountConfig
from .transport import HTTPAdapterRetryConfig
from .types import JSONData
from .types import JSONDict
from .types import JSONList
from .types import RetryPolicy


# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'CursorPaginationMap',
    'EndpointClient',
    'EndpointCredentialsBearer',
    'HTTPAdapterMountConfig',
    'HTTPAdapterRetryConfig',
    'PagePaginationMap',
    'PaginationMap',
    'PaginationType',
    'RateLimitConfigMap',
    'RateLimiter',

    # Functions
    'build_http_adapter',
    'compute_sleep_seconds',

    # Common types
    'JSONDict', 'JSONList', 'JSONData',
    'RetryPolicy',
]
