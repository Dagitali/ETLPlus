"""
:mod:`etlplus.api` package.

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
- Prefer :data:`JSONRecords` (list of :data:`JSONDict`) for paginated
    responses; scalar/record aliases are exported for convenience.
- The underlying :class:`Paginator` is exported for advanced scenarios that
    need to stream pages manually.

See Also
--------
- :mod:`etlplus.api.rate_limiting` for rate-limit helpers and config shapes
- :mod:`etlplus.api.pagination` for pagination helpers and config shapes
- :mod:`etlplus.api.retry_manager` for retry policies
- :mod:`etlplus.api.transport` for HTTPAdapter helpers
"""

from __future__ import annotations

from .auth import EndpointCredentialsBearer
from .config import ApiConfig
from .config import ApiProfileConfig
from .config import EndpointConfig
from .endpoint_client import EndpointClient
from .enums import HttpMethod
from .pagination import CursorPaginationConfigMap
from .pagination import PagePaginationConfigMap
from .pagination import PaginationClient
from .pagination import PaginationConfig
from .pagination import PaginationConfigMap
from .pagination import PaginationType
from .pagination import Paginator
from .rate_limiting import RateLimitConfig
from .rate_limiting import RateLimitConfigMap
from .rate_limiting import RateLimiter
from .retry_manager import RetryManager
from .retry_manager import RetryPolicy
from .retry_manager import RetryStrategy
from .transport import HTTPAdapterMountConfig
from .transport import HTTPAdapterRetryConfig
from .transport import build_http_adapter
from .types import Headers
from .types import Params
from .types import RequestOptions
from .types import Url
from .utils import compose_api_request_env
from .utils import compose_api_target_env
from .utils import paginate_with_client
from .utils import resolve_request

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'EndpointClient',
    'EndpointCredentialsBearer',
    'Paginator',
    'RateLimiter',
    'RetryManager',
    # Data Classes
    'ApiConfig',
    'ApiProfileConfig',
    'EndpointConfig',
    'PaginationClient',
    'PaginationConfig',
    'RateLimitConfig',
    'RequestOptions',
    'RetryStrategy',
    # Enums
    'HttpMethod',
    'PaginationType',
    # Functions
    'build_http_adapter',
    'compose_api_request_env',
    'compose_api_target_env',
    'paginate_with_client',
    'resolve_request',
    # Type Aliases
    'CursorPaginationConfigMap',
    'Headers',
    'HTTPAdapterMountConfig',
    'HTTPAdapterRetryConfig',
    'PagePaginationConfigMap',
    'PaginationConfigMap',
    'Params',
    'RateLimitConfigMap',
    'RetryPolicy',
    'Url',
]
